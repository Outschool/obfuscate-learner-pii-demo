import "dotenv/config";

import { exec } from "child_process";
import EventEmitter from "events";
import Fs from "fs";
import { Client } from "pg";
import { PassThrough, Readable } from "stream";
import { createDeflate } from "zlib";

import {
  ColumnMappings,
  DbCreds,
  dbCreds,
  DbDumpUploader,
  MainDumpProps,
  obfuscatedSqlFile,
  ONE_MB,
  PgLogger,
  PgRowIterable,
  PgTocEntry,
  TableColumnMappings,
  targetDumpFile,
} from "./constantsAndTypes";
import {
  consumeHead,
  countDataBlocks,
  findColumnMappings,
  findTocEntry,
  localFileDumpUploader,
  rowCounter,
  spawnPgDump,
  transformDataRows,
  updateHeadForObfuscation,
} from "./helpers";
import { PgCustomFormatter } from "./pgCustom";
import { tableMappings } from "./tableMappings";

const client = new Client(dbCreds);

async function run() {
  await client.connect();
  await dumpDb();
  await convertDumpToSql();
  await client.end();
}
run();

async function dumpDb() {
  console.log(`Running locally, writing to ${targetDumpFile}`);

  try {
    await dbDumpObfuscated(
      (msg: string) => console.log(new Date().toISOString(), msg),
      dbCreds,
      localFileDumpUploader(targetDumpFile)
    );
  } catch (e) {
    console.error(e);
    // remove files generated during thrown error
    Fs.unlinkSync(targetDumpFile);
  }
}

async function convertDumpToSql(): Promise<void> {
  return new Promise((resolve, reject) => {
    exec(`pg_restore ${targetDumpFile} > ${obfuscatedSqlFile}`, function (err) {
      if (err) {
        return reject(err);
      }
      resolve();
    });
  });
}

async function dbDumpObfuscated(
  logger: (msg: string) => void,
  dbCreds: DbCreds,
  uploader: DbDumpUploader
) {
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
