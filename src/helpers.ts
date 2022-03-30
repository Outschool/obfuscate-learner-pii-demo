import { spawn } from "child_process";
import { Writable } from "stream";

import {
  ColumnMapper,
  ColumnMappings,
  DATA_COLUMN_SEPARATOR,
  DATA_ROW_TERMINATOR,
  DbCreds,
  OUTSCHOOL_DOMAIN,
  ParsedCopyStatement,
  PG_NULL,
  PgHead,
  PgTocEntry,
  RETAIN,
  TableColumnMappings,
} from "./constantsAndTypes";
import { PgCustomReader } from "./pgCustom";

function bufferEndsWith(buf: Buffer, ending: Buffer) {
  return -1 !== buf.indexOf(ending, buf.byteLength - ending.byteLength);
}

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
  return head.tocEntries.filter((it) => Boolean(it.dataDumper)).length;
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
  const mappers: ColumnMapper[] = columns.map((it) => {
    const foundMapping = tableMapper[it];
    if (!foundMapping) {
      console.log(
        `Warn: Unable to find column mapping for column "${it}" in table "${table}"`
      );
      return RETAIN;
    }
    return foundMapping;
  });

  if (mappers.every((f) => f === RETAIN)) {
    return null;
  }
  return {
    names: columns,
    mappers: mappers,
  };
}

export function findTocEntry(head: PgHead, dumpId: number) {
  const result = head.tocEntries.find((it) => it.dumpId === dumpId);
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

function parseCopyStatement(toc: PgTocEntry): ParsedCopyStatement {
  // const { copyStmt } = toc;
  // TODO: fix injecting of columns
  const copyStmt = "COPY public.pii_demo (uid, email_address, personal_info, secret_token) FROM stdin;\n";
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
  const columns = rawColumnList.split(", ").map((it) => {
    // eslint-disable-next-line quotes
    if (it.startsWith(`"`) && it.endsWith(`"`)) {
      return it.slice(1, -1);
    }
    return it;
  });
  return { rawColumnList, columns };
}

export function parseDataRow(buf: Buffer): Buffer[] {
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

export function serializeDataRow(bufs: Buffer[]): Buffer {
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

export function updateHeadForObfuscation(head: PgHead) {
  // Signal that we're compressing the data going out and at what level
  head.compression = 9;

  // Ensure the offsets are reset for the first write. They will be updated
  // as each section is updated.
  for (const tocEntry of head.tocEntries) {
    tocEntry.offset = { flag: "Not Set", value: null };
  }
}

export function replaceEmailBasedOnColumn(uidColumn: string): ColumnMapper {
  return (content, columns, row) => {
    if (content.equals(PG_NULL)) {
      return PG_NULL;
    }
    if (bufferEndsWith(content, OUTSCHOOL_DOMAIN)) {
      return content;
    }
    const uid = parsePgString(row[columns.indexOf(uidColumn)]);
    return serializePgString(uid + "@blackhole.outschool.com");
  };
}

export const replaceWithNull: ColumnMapper = () => PG_NULL;

export const replaceWithScrambledText: ColumnMapper = (content) => {
  const str = parsePgString(content);
  if (str === null) {
    return PG_NULL;
  }

  const result = str
    .replace(/[A-z]/g, randomLetter)
    .replace(/[0-9]/g, randomDigit);
  return serializePgString(result);
};

function parsePgString(content: Buffer): string | null {
  if (content.equals(PG_NULL)) {
    return null;
  }
  //Note: The docs claim that `\OCT` and `\xHEX` patterns are supported but
  // that they will not be emitted by COPY TO. We trust this claim.
  return content.toString("utf8").replace(/\\./g, (str) => {
    if (str === "\\\\") {
      return "\\";
    }
    if (str === "\\b") {
      return "\b";
    }
    if (str === "\\t") {
      return "\t";
    }
    if (str === "\\n") {
      return "\n";
    }
    if (str === "\\v") {
      return "\v";
    }
    if (str === "\\f") {
      return "\f";
    }
    if (str === "\\r") {
      return "\r";
    }
    return str[1];
  });
}

function randomLetter() {
  return Math.floor(Math.random() * 26 + 10).toString(36);
}

function randomDigit() {
  return Math.floor(Math.random() * 10).toString(10);
}

function serializePgString(str: string | null) {
  if (str === null) {
    return PG_NULL;
  }
  return Buffer.from(
    str
      .replace(/\\/g, "\\\\")
      /* eslint-disable no-control-regex */
      .replace(/\x08/g, "\\b")
      .replace(/\x09/g, "\\t")
      .replace(/\x0a/g, "\\n")
      .replace(/\x0b/g, "\\v")
      .replace(/\x0c/g, "\\f")
      .replace(/\x0d/g, "\\r")
  );
}

export function spawnPgDump(dbCreds: DbCreds, stdErr?: Writable) {
  const args = [buildDbUrl(dbCreds), "--format=custom", "--compress=0"];
  const result = spawn("pg_dump", args, {
    stdio: ["ignore", "pipe", "pipe"],
  });
  result.stderr.pipe(stdErr ?? process.stderr);
  return result;
}
