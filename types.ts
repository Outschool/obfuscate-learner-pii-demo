import { formats, offsetFlag, sections } from "./constants";

export interface DbCreds {
  host: string;
  dbname: string;
  password?: string;
  username?: string;
}

export interface DbDumpUploader {
  outputStream: NodeJS.WritableStream;
  finalize: (finalHeader: Buffer) => Promise<void>;
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
