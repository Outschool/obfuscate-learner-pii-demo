import Path from "path";

import { ColumnMapper, DbCreds } from "./types";

export const DATA_COLUMN_SEPARATOR = "\t".charCodeAt(0);

export const DATA_ROW_TERMINATOR = "\n".charCodeAt(0);

const DEFAULT_DATABASE = "outschool_obfuscate_demo";

export const EMPTY = Buffer.from("");

export const FINAL_DATA_ROW = Buffer.from("\\.\n\n\n");

export const ONE_MB = 1024 ** 2;

export const OUTSCHOOL_DOMAIN = Buffer.from("@outschool.com");

export const PG_NULL = Buffer.from("\\N");

const PG_DUMP_EXPORT_PATH = Path.resolve("./output/");

export const RETAIN: ColumnMapper = content => content;

export const dbCreds: DbCreds = {
  dbname: process.env.DB_NAME ?? DEFAULT_DATABASE,
  host: process.env.DB_HOST ?? "localhost",
  username: process.env.DB_USER ?? "",
  password: process.env.DB_PASSWORD ?? "",
};

export const obfuscatedSqlFile = `${PG_DUMP_EXPORT_PATH}/obfuscated.sql`;

export const targetDumpFile = `${PG_DUMP_EXPORT_PATH}/db-obfuscation-main.dat`;
