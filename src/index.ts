import EventEmitter from "events";
import Fs from "fs";
import { Client } from "pg";
import { PassThrough, Readable } from "stream";
import uuid from "uuid-random";
import { createDeflate } from "zlib";

import {
  ColumnMappings,
  DbCreds,
  dbCreds,
  DbDumpUploader,
  FINAL_DATA_ROW,
  MainDumpProps,
  ONE_MB,
  PG_DUMP_EXPORT_PATH,
  PgLogger,
  PgRowIterable,
  PgTocEntry,
  RETAIN,
  TableColumnMappings,
} from "./constantsAndTypes";
import {
  consumeHead,
  countDataBlocks,
  findColumnMappings,
  findTocEntry,
  localFileDumpUploader,
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
  await dumpDb();
  // TODO: add step to tranform dat file > sql
  await client.end();
}
run();

async function dumpDb() {
  const targetFile = `${PG_DUMP_EXPORT_PATH}/db-obfuscation-main.dat`;
  console.log(`Running locally, writing to ${targetFile}`);

  try {
    await dbDumpObfuscated(
      (msg: string) => console.log(new Date().toISOString(), msg),
      dbCreds,
      localFileDumpUploader(targetFile)
    );
  } catch (e) {
    console.error(e);
    // remove files generated during thrown error
    Fs.unlinkSync(targetFile);
  }
}

async function dbDumpObfuscated(
  logger: (msg: string) => void,
  dbCreds: DbCreds,
  uploader: DbDumpUploader
) {
  const tableMappings = await buildObfuscationTableMappings();
  const pgDump = spawnPgDump(dbCreds);

  logger("Starting");
  const finalHeaderBuffer = await obfuscatePgCustomDump({
    logger,
    tableMappings,
    outputStream: uploader.outputStream,

    // Perf optimization:
    // add a large intermediate buffer to allow data to flow while processing,
    // since the reader doesn't consume in streaming mode
    inputStream: pgDump.stdout.pipe(
      new PassThrough({ highWaterMark: 20 * ONE_MB })
    ),
  });

  logger("Finalizing");
  await uploader.finalize(finalHeaderBuffer);

  logger("Finished");
}

function buildObfuscationTableMappings(): TableColumnMappings {
  return {
    learner_pii: {
      uid: RETAIN,
      email_address: replaceEmailBasedOnColumn("uid"),
      personal_info: replaceWithScrambledText,
      secret_token: replaceWithNull,
    },
  };
}

/**
 * Takes a 'pg_dump --format=custom' stream and obfuscates the data.
 *
 * The data is streamed to avoid excessive memory or file system usage for
 * large tables.
 *
 * A final buffer is returned with an updated file header. This buffer
 * may be used to overwrite the beginning of the output stream. Doing so
 * will enable pg_restore to load tables in parallel.
 */
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
