import EventEmitter from "events";
import Fs from "fs";
import { Client } from "pg";
import { PassThrough, Readable } from "stream";
import uuid from "uuid-random";
import { createDeflate } from "zlib";

import {
  ColumnMappings,
  dbCreds,
  DEFAULT_TABLE,
  FINAL_DATA_ROW,
  MainDumpProps,
  ONE_MB,
  PG_DUMP_EXPORT_PATH,
  PgLogger,
  PgRowIterable,
  PgTocEntry,
  RETAIN,
  secretInfo,
  TableColumnMappings,
} from "./constantsAndTypes";
import {
  consumeHead,
  countDataBlocks,
  findColumnMappings,
  findTocEntry,
  parseDataRow,
  replaceEmailBasedOnColumn,
  replaceWithNull,
  replaceWithScrambledText,
  rowCounter,
  serializeDataRow,
  spawnPgDump,
  updateHeadForObfuscation,
} from "./helpers";
import { PgCustomFormatter } from "./pgCustom";

const client = new Client(dbCreds);

async function run() {
  await client.connect();
  await createTestDb();
  await dbDumpObfuscated();
  await client.end();
}
setTimeout(run, 1000);

async function createTestDb() {
  await client.query(`DROP TABLE IF EXISTS ${DEFAULT_TABLE}`);
  await client.query(
    `CREATE TABLE ${DEFAULT_TABLE}(uid uuid, email_address text, personal_info text, secret_token uuid);`
  );
  await client.query(
    `INSERT INTO  ${DEFAULT_TABLE}(uid, email_address, personal_info, secret_token) values($1, $2, $3, $4);`,
    [uuid(), "myunobfuscatedemail@address.com", secretInfo, uuid()]
  );
  await client.query(
    `INSERT INTO  ${DEFAULT_TABLE}(uid, email_address, personal_info, secret_token) values($1, $2, $3, $4);`,
    [uuid(), "oslearneremail@address.com", secretInfo, uuid()]
  );
  await client.query(
    `INSERT INTO  ${DEFAULT_TABLE}(uid, email_address, personal_info, secret_token) values($1, $2, $3, $4);`,
    [uuid(), "osparentemail@address.com", secretInfo, uuid()]
  );
}

async function dbDumpObfuscated() {
  const logger = console.log;
  const tableMappings = {} as any;
  tableMappings[DEFAULT_TABLE] = {
    uid: RETAIN,
    email_address: replaceEmailBasedOnColumn("uid"),
    personal_info: replaceWithScrambledText,
    secret_token: replaceWithNull,
  };
  const pgDump = spawnPgDump(dbCreds);
  const dumpId = new Date().toISOString().replace(/[:.]/g, "-");
  const mainFile = `${PG_DUMP_EXPORT_PATH}/${dumpId}-main.dat`;
  const headerFile = `${PG_DUMP_EXPORT_PATH}/${dumpId}-header-update.dat`;
  const outputStream = Fs.createWriteStream(mainFile);

  try {
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
  } catch (e) {
    console.error(e);
    // remove files generated during thrown error
    Fs.unlinkSync(mainFile);
    Fs.unlinkSync(headerFile);
  }
}

function writeHeader(
  headerFile: string,
  finalHeaderBuffer: Buffer
): Promise<void> {
  return new Promise((resolve, reject) => {
    const writableStream = Fs.createWriteStream(headerFile);
    writableStream.on("error", reject);
    writableStream.write(finalHeaderBuffer);
    writableStream.end();
    writableStream.on("finish", resolve);
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
    const data = parseDataRow(row).map((col, ndx, src) => {
      return columnMappings.mappers[ndx](col, columnMappings.names, src);
    });

    yield serializeDataRow(data);
  }
}
