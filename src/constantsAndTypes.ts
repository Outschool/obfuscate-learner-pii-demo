export const DATA_COLUMN_SEPARATOR = "\t".charCodeAt(0);

export const DATA_ROW_TERMINATOR = "\n".charCodeAt(0);

export const EMPTY = Buffer.from("");

export const FINAL_DATA_ROW = Buffer.from("\\.\n\n\n");

export const ONE_MB = 1024 ** 2;

export const OUTSCHOOL_DOMAIN = Buffer.from("@outschool.com");

export const PG_NULL = Buffer.from("\\N");

export const RETAIN: ColumnMapper = (content) => content;

export const formats = [
  "Unknown",
  "Custom",
  "Files",
  "Tar",
  "Null",
  "Directory",
] as const;

export const offsetFlag = ["Unknown", "Not Set", "Set", "No Data"] as const;

export const sections = [
  "Unknown",
  "None",
  "PreData",
  "Data",
  "PostData",
] as const;

/**
 * Defines the function signature for transforming table data.
 *
 * Row content is in the "COPY" "text" format described here:
 * https://www.postgresql.org/docs/11/sql-copy.html
 *
 * @param content The raw column content
 * @param columns An array of column names for the current table
 * @param row All columns for the current table row. Positions align with the columns array.
 */
export type ColumnMapper = (
  content: Buffer,
  columns: string[],
  row: Buffer[]
) => Buffer;

export interface ColumnMappings {
  names: string[];
  mappers: ColumnMapper[];
}

export interface DbCreds {
  host: string;
  dbname: string;
  password?: string;
  username?: string;
}

export interface MainDumpProps {
  logger: PgLogger;
  tableMappings: TableColumnMappings;
  inputStream: NodeJS.ReadableStream;
  outputStream: NodeJS.WritableStream;
}

export interface ParsedCopyStatement {
  rawColumnList: string;
  columns: string[];
}

export enum PgVersion {
  unknown = -1,
  v1_13_0,
  v1_14_0,
}

export type PgLogger = (msg: string) => void;

export interface PgPrelude {
  magic: string;
  version: [number, number, number];
  intSize: number;
  offSize: number;
  format: typeof formats[number];
}

export interface PgHead {
  compression: number;
  sec: number;
  min: number;
  hour: number;
  mday: number;
  mon: number;
  yr: number;
  isdst: number;
  dbname: string | null;
  remoteVersion: string | null;
  pgdumpVersion: string | null;
  tocCount: number;
  tocEntries: PgTocEntry[];
}

export interface PgDataHead {
  type: number;
  dumpId: number;
}

export interface PgTocEntry {
  dumpId: number;
  dataDumper: number;
  tableoid: string | null;
  oid: string | null;
  tag: string | null;
  desc: string | null;
  section: typeof sections[number];
  defn: string | null;
  dropStmt: string | null;
  copyStmt: string | null;
  namespace: string | null;
  tablespace: string | null;
  tableam: string | null; //added in v1.14.0
  owner: string | null;
  withOids: string | null;
  deps: string[];
  offset: PgOffset;
}

export interface PgOffset {
  flag: typeof offsetFlag[number];
  value: bigint | null;
}

export type PgRowIterable = AsyncGenerator<Buffer, void>;

export type TableColumnMappings = Record<string, TableMapper>;

type TableMapper = Record<string, ColumnMapper>;
