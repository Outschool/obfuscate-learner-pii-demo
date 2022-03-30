import EventEmitter from "events";
import Fs from "fs";
import Path from "path";
import { Client } from "pg";
import { PassThrough, Readable } from "stream";
import { createDeflate } from "zlib";

import { FINAL_DATA_ROW, ONE_MB } from "./constants";
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

const DEFAULT_DATABASE = "outschool_obfuscate_demo";

const DEFAULT_TABLE = "pii_demo";

const PG_DUMP_EXPORT_PATH = Path.resolve("./output/");

const dbCreds = {
  dbname: DEFAULT_DATABASE,
  host: "localhost",
  user: "",
  password: "",
};

const client = new Client(dbCreds);

async function run() {
  await client.connect();
  await createTestDb();
  await dbDumpObfuscated();
  await client.end();
}
run();

async function createTestDb() {
  await client.query(`DROP TABLE IF EXISTS ${DEFAULT_TABLE}`);
  await client.query(`CREATE TABLE ${DEFAULT_TABLE}(email_address text);`);
  await client.query(
    `INSERT INTO  ${DEFAULT_TABLE}(email_address) values('myunobfuscatedemail@address.com');`
  );
}

async function dbDumpObfuscated() {
  const logger = console.log;
  const tableMappings = {} as any;
  tableMappings[DEFAULT_TABLE] = {
    email: replaceEmailBasedOnColumn("uid"),
  };
  const pgDump = spawnPgDump(dbCreds, tableMappings);
  const dumpId = new Date().toISOString().replace(/[:.]/g, "-");
  const mainFile = `${PG_DUMP_EXPORT_PATH}/${dumpId}-main.dat`;
  const headerFile = `${PG_DUMP_EXPORT_PATH}/${dumpId}-header-update.dat`;
  const outputStream = Fs.createWriteStream(mainFile);

  logger("Starting");
  const finalHeader = await obfuscatePgCustomDump({
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

  await writeHeader(headerFile, finalHeader);

  logger("Finished");
}

function writeHeader(
  headerFile: string,
  finalHeaderBuffer: Buffer
): Promise<boolean> {
  return new Promise((resolve, reject) => {
    const writableStream = Fs.createWriteStream(headerFile);
    writableStream.on("error", reject);
    writableStream.write(finalHeaderBuffer);
    writableStream.on("finish", () => {
      console.log("DONE");
      writableStream.end();
      resolve(true);
    });
  });
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
