import EventEmitter from "events";
import Fs from "fs";
import Path from "path";
import { Client } from "pg";
import { to as copyTo } from "pg-copy-streams";
import { PassThrough, Readable } from "stream";
import { createDeflate } from "zlib";

import { DATA_ROW_TERMINATOR, FINAL_DATA_ROW, ONE_MB } from "./constants";
import {
  consumeHead,
  countDataBlocks,
  findColumnMappings,
  findTocEntry,
  parseDataRow,
  replaceEmailBasedOnColumn,
  rowCounter,
  serializeDataRow,
  spawnPgDump,
  updateHeadForObfuscation,
} from "./helpers";
import { PgCustomFormatter } from "./pgCustom";
import {
  ColumnMappings,
  MainDumpProps,
  PgLogger,
  PgRowIterable,
  PgTocEntry,
  TableColumnMappings,
} from "./types";

const client = new Client();

const DEFAULT_DATABASE = "obufscate_outschool_pii";

const PG_DUMP_EXPORT_PATH = Path.join(
  __dirname,
  "output/obfuscated-export.sql"
);

async function run() {
  await client.connect();
  await createTestDb();
  await copyTable();
  await dbDumpObfuscated();
  await client.end();
}
run();

async function createTestDb() {
  await client.query(`DROP TABLE IF EXISTS ${DEFAULT_DATABASE}`);
  await client.query(
    `CREATE TEMP TABLE  ${DEFAULT_DATABASE}(email_address text);`
  );
  await client.query(
    `INSERT INTO  ${DEFAULT_DATABASE}(email_address) values('myunobfuscatedemail@address.com');`
  );
}

function copyTable(): AsyncGenerator<Buffer> {
  return (async function* () {
    const query = "SELECT email_address from _obfuscated_pii";
    const stream = client.query(copyTo(`COPY (${query}) TO STDOUT`));
    yield* copyStreamToRows(stream);
    yield FINAL_DATA_ROW;
  })();
}

async function dbDumpObfuscated() {
  const currentUser = (await client.query("SELECT current_user"))?.rows[0]
    ?.current_user;
  const dbCreds = {
    dbname: DEFAULT_DATABASE,
    host: "localhost",
    user: currentUser,
    password: "",
  };
  const logger = console.log;
  const tableMappings = {
    _obfuscated_pii: {
      email: replaceEmailBasedOnColumn("uid"),
    },
  };
  const pgDump = spawnPgDump(dbCreds, tableMappings);
  const outputStream = Fs.createWriteStream(PG_DUMP_EXPORT_PATH);

  logger("Starting");
  await obfuscatePgCustomDump({
    logger,
    tableMappings,
    outputStream,
    // Perf optimization:
    // add a large intermediate buffer to allow data to flow while processing,
    // since the reader doesn't consume in streaming mode
    inputStream: pgDump.stdout.pipe(
      new PassThrough({ highWaterMark: 20 * ONE_MB })
    ),
  });
  logger("Finished");
}

async function obfuscatePgCustomDump(props: MainDumpProps): Promise<Buffer> {
  const { logger, outputStream, tableMappings } = props;
  const { prelude, reader, head } = await consumeHead(props.inputStream);
  logger("header consumed");
  updateHeadForObfuscation(head);

  const formatter = new PgCustomFormatter(prelude);

  const initialHeader = formatter.formatFileHeader(prelude, head);
  outputStream.write(initialHeader);

  const dataBlockCount = countDataBlocks(head);
  let dataStartPos = initialHeader.byteLength;
  for (let i = 0; i < dataBlockCount; i++) {
    const dataHead = await reader.readDataBlockHead();
    const bytesWritten = await obfuscateSingleTable({
      logger,
      dataStartPos,
      toc: findTocEntry(head, dataHead.dumpId),
      dataRows: reader.createDataRowIterable(),
      formatter,
      outputStream,
      tableMappings,
    });
    dataStartPos += bytesWritten;
  }

  outputStream.end();
  // Return a header with updated offsets to enable parallel restore
  return formatter.formatFileHeader(prelude, head);
}

async function obfuscateSingleTable(props: {
  logger: PgLogger;
  dataStartPos: number;
  toc: PgTocEntry;
  dataRows: PgRowIterable;
  formatter: PgCustomFormatter;
  tableMappings: TableColumnMappings;
  outputStream: NodeJS.WritableStream;
}) {
  const { toc } = props;
  toc.offset = { flag: "Set", value: BigInt(props.dataStartPos) };
  const dataFmtr = props.formatter.createDataBlockFormatter(toc);
  const rowCounts = rowCounter();

  let iterator = rowCounts.iterator(props.dataRows);
  const columnMappings = findColumnMappings(props.tableMappings, toc);
  iterator = transformDataRows(<ColumnMappings>columnMappings, iterator);

  Readable.from(iterator, { objectMode: true })
    .pipe(createDeflate({ level: 9, memLevel: 9 }))
    .pipe(dataFmtr.transformStream)
    .pipe(props.outputStream, { end: false });

  await EventEmitter.once(props.outputStream, "unpipe");

  return dataFmtr.getBytesWritten();
}

async function* transformDataRows(
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

async function* copyStreamToRows(stream: NodeJS.ReadableStream) {
  const prevChunks: Buffer[] = [];
  for await (let ch of stream) {
    if (typeof ch === "string") {
      ch = Buffer.from("ch", "utf8");
    }
    let rowIndex = ch.indexOf(DATA_ROW_TERMINATOR);
    if (rowIndex === -1) {
      prevChunks.push(ch);
      continue;
    }
    if (prevChunks.length) {
      prevChunks.push(ch);
      ch = Buffer.concat(prevChunks);
      prevChunks.splice(0, prevChunks.length);
      rowIndex = ch.indexOf(DATA_ROW_TERMINATOR);
    }

    while (rowIndex !== -1) {
      yield ch.slice(0, rowIndex + 1);
      ch = ch.slice(rowIndex + 1);
      rowIndex = ch.indexOf(DATA_ROW_TERMINATOR);
    }

    if (ch.length) {
      prevChunks.push(ch);
    }
  }
}
