import { spawn } from "child_process";
import { once } from "events";
import { createWriteStream } from "fs";
import { open } from "fs/promises";
import { Writable } from "stream";

import {
  DATA_COLUMN_SEPARATOR,
  DATA_ROW_TERMINATOR,
  FINAL_DATA_ROW,
  RETAIN,
} from "./constants";
import { PgCustomReader } from "./pgCustom";
import {
  ColumnMapper,
  ColumnMappings,
  DbCreds,
  DbDumpUploader,
  ParsedCopyStatement,
  PgHead,
  PgTocEntry,
  TableColumnMappings,
} from "./types";

function buildDbUrl({ host, password, username, dbname }: DbCreds) {
  const result = ["postgres://"];
  if (username) {
    result.push(username);
    if (password) {
      result.push(":", password);
    }
    result.push("@");
  }
  result.push(host, "/", dbname);

  return result.join("");
}

export async function consumeHead(inputStream: NodeJS.ReadableStream) {
  const prelude = await PgCustomReader.readPrelude(inputStream);
  PgCustomReader.validatePrelude(prelude);

  const reader = new PgCustomReader(inputStream, prelude);
  const head = await reader.readHead();

  return { prelude, reader, head };
}

export function countDataBlocks(head: PgHead) {
  return head.tocEntries.filter(it => Boolean(it.dataDumper)).length;
}

export function findColumnMappings(
  tableMappings: TableColumnMappings,
  toc: PgTocEntry
): ColumnMappings | null {
  const table = toc.tag;
  if (!table) {
    throw new Error(`Missing tag in tocEntry ${toc.dumpId}`);
  }

  const tableMapper = tableMappings[table];
  if (!tableMapper) {
    console.log(
      `Warn: Unable to find table column mappings for table "${table}"`
    );
    return null;
  }

  const { columns } = parseCopyStatement(toc);
  const mappers: ColumnMapper[] = columns.map(it => {
    const foundMapping = tableMapper[it];
    if (!foundMapping) {
      console.log(
        `Warn: Unable to find column mapping for column "${it}" in table "${table}"`
      );
      return RETAIN;
    }
    return foundMapping;
  });

  if (mappers.every(f => f === RETAIN)) {
    return null;
  }
  return {
    names: columns,
    mappers: mappers,
  };
}

export function findTocEntry(head: PgHead, dumpId: number) {
  const result = head.tocEntries.find(it => it.dumpId === dumpId);
  if (!result) {
    throw new Error(
      `Data block referenced dumpId ${dumpId} but that tocEntry doesn't exist`
    );
  }

  if (!result.dataDumper) {
    throw new Error(
      `Data block referenced dumpId ${dumpId} but that tocEntry wasn't expected to have data`
    );
  }

  return result;
}

export function localFileDumpUploader(outFile: string): DbDumpUploader {
  const outputStream = createWriteStream(outFile);
  const contentFinished = once(outputStream, "close");

  return {
    outputStream,
    async finalize(finalHeader) {
      await contentFinished;

      // Overwrite the original header with the new content
      const f = await open(outFile, "r+");
      try {
        await f.write(finalHeader, 0, finalHeader.length);
      } finally {
        await f.close();
      }
    },
  };
}

function parseCopyStatement(toc: PgTocEntry): ParsedCopyStatement {
  const { copyStmt } = toc;
  if (!copyStmt) {
    throw new Error(
      `Missing copyStatement for ${toc.tag ?? `[dumpid:${toc.dumpId}]`}`
    );
  }
  //assumptions:
  // * table names do not contain " "
  // * column names do not contain ", "
  const match = /COPY .* \((.*)\) FROM stdin;/.exec(copyStmt);
  if (!match) {
    throw new Error(`"Unable to parse copyStmt: ${copyStmt}`);
  }

  const [, rawColumnList] = match;
  const columns = rawColumnList.split(", ").map(it => {
    // eslint-disable-next-line quotes
    if (it.startsWith(`"`) && it.endsWith(`"`)) {
      return it.slice(1, -1);
    }
    return it;
  });
  return { rawColumnList, columns };
}

function parseDataRow(buf: Buffer): Buffer[] {
  const parts = [];
  let ndx = 0;
  while (ndx !== -1) {
    const nextNdx = buf.indexOf(DATA_COLUMN_SEPARATOR, ndx);
    if (nextNdx === -1) {
      // last character on row is DATA_ROW_TERMINATOR;
      parts.push(buf.slice(ndx, -1));
      break;
    } else {
      parts.push(buf.slice(ndx, nextNdx));
      ndx = nextNdx + 1;
    }
  }
  return parts;
}

export function rowCounter() {
  let rows = 0;
  return {
    getCount: () => rows,
    iterator: async function* <T>(iterator: AsyncGenerator<T>) {
      for await (const row of iterator) {
        yield row;
        rows++;
      }
      // a data terminator is always emitted as the last row
      rows--;
    },
  };
}

function serializeDataRow(bufs: Buffer[]): Buffer {
  const totalLength = bufs.length + bufs.reduce((a, v) => a + v.byteLength, 0);
  const result = Buffer.alloc(totalLength);
  let ndx = 0;
  for (const b of bufs) {
    if (b.byteLength) {
      result.fill(b, ndx, ndx + b.byteLength);
      ndx += b.byteLength;
    }
    result[ndx] = DATA_COLUMN_SEPARATOR;
    ndx += 1;
  }
  result[ndx - 1] = DATA_ROW_TERMINATOR;
  return result;
}

export async function* transformDataRows(
  columnMappings: ColumnMappings,
  dataIterator: AsyncGenerator<Buffer>
) {
  let ended = false;
  for await (const row of dataIterator) {
    if (row.equals(FINAL_DATA_ROW)) {
      yield row;
      ended = true;
      continue;
    }
    if (ended) {
      throw new Error("Received data after the content terminator");
    }
    const data = parseDataRow(row).map((col, ndx, src) =>
      columnMappings.mappers[ndx](col, columnMappings.names, src)
    );

    yield serializeDataRow(data);
  }
}

export function spawnPgDump(dbCreds: DbCreds, stdErr?: Writable) {
  const args = [buildDbUrl(dbCreds), "--format=custom", "--compress=0"];
  const result = spawn("pg_dump", args, {
    stdio: ["ignore", "pipe", "pipe"],
  });
  result.stderr.pipe(stdErr ?? process.stderr);
  return result;
}

export function updateHeadForObfuscation(head: PgHead) {
  // Signal that we're compressing the data going out and at what level
  head.compression = 9;

  // Ensure the offsets are reset for the first write. They will be updated
  // as each section is updated.
  for (const tocEntry of head.tocEntries) {
    tocEntry.offset = { flag: "Not Set", value: null };
  }
}
