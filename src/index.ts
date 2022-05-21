import "dotenv/config";

import { exec } from "child_process";
import EventEmitter from "events";
import Fs from "fs";
import { Client } from "pg";
import { PassThrough, Readable } from "stream";
import { createDeflate } from "zlib";

import {
  dbCreds,
  obfuscatedSqlFile,
  ONE_MB,
  targetDumpFile,
} from "./constants";
import { PgCustomFormatter } from "./pgCustom";
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
} from "./shared";
import { tableMappings } from "./tableMappings";
import {
  ColumnMappings,
  DbCreds,
  DbDumpUploader,
  MainDumpProps,
  PgLogger,
  TableDumpProps,
} from "./types";

// setup client
const client = new Client(dbCreds);

async function run() {
  const logger = (msg: string) => console.log(new Date().toISOString(), msg);
  await client.connect();
  await exportDb(logger);
  await convertExportToSql(logger);
  await client.end();
}
run();

async function exportDb(logger: PgLogger) {
  logger(`Output file: ${targetDumpFile}`);

  try {
    await obfuscateDbExport(
      logger,
      dbCreds,
      localFileDumpUploader(targetDumpFile)
    );
  } catch (e) {
    console.error(e);
    // remove files generated during thrown error
    Fs.unlinkSync(targetDumpFile);
  }
}

async function obfuscateDbExport(
  logger: (msg: string) => void,
  dbCreds: DbCreds,
  uploader: DbDumpUploader
) {
  const pgDump = spawnPgDump(dbCreds);

  logger("Starting export");
  const finalHeaderBuffer = await obfuscatePgCustomExport({
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

  logger("Finalizing export");
  await uploader.finalize(finalHeaderBuffer);

  logger("Finished export");
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
async function obfuscatePgCustomExport({
  logger,
  inputStream,
  outputStream,
  tableMappings,
}: MainDumpProps): Promise<Buffer> {
  const { prelude, reader, head } = await consumeHead(inputStream);
  logger("Header consumed");
  updateHeadForObfuscation(head);

  const formatter = new PgCustomFormatter(prelude);
  const initialHeader = formatter.formatFileHeader(prelude, head);
  outputStream.write(initialHeader);

  const tableCount = countDataBlocks(head);
  logger(`Table export count: ${tableCount}`);

  let dataStartPos = initialHeader.byteLength;
  for (let i = 0; i < tableCount; i++) {
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

async function obfuscateSingleTable({
  dataStartPos,
  dataRows,
  formatter,
  outputStream,
  tableMappings,
  toc,
}: TableDumpProps & {
  formatter: PgCustomFormatter;
}) {
  toc.offset = { flag: "Set", value: BigInt(dataStartPos) };
  const dataFmtr = formatter.createDataBlockFormatter(toc);
  const rowCounts = rowCounter();
  let iterator = rowCounts.iterator(dataRows);
  const columnMappings = findColumnMappings(tableMappings, toc);
  iterator = transformDataRows(<ColumnMappings>columnMappings, iterator);

  Readable.from(iterator, { objectMode: true })
    .pipe(createDeflate({ level: 9, memLevel: 9 }))
    .pipe(dataFmtr.transformStream)
    .pipe(outputStream, { end: false });

  await EventEmitter.once(outputStream, "unpipe");

  return dataFmtr.getBytesWritten();
}

async function convertExportToSql(logger: PgLogger): Promise<void> {
  logger("Converting export to sql");
  return new Promise((resolve, reject) => {
    exec(`pg_restore ${targetDumpFile} > ${obfuscatedSqlFile}`, function (err) {
      if (err) {
        return reject(err);
      }
      resolve();
    });
  });
}
