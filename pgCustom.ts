import { Transform } from "stream";

import { EMPTY, formats, offsetFlag, sections } from "./constants";
import {
  PgDataHead,
  PgHead,
  PgOffset,
  PgPrelude,
  PgTocEntry,
  PgVersion,
} from "./types";

export class PgCustomReader {
  private readonly stream: NodeJS.ReadableStream;
  private readonly version: PgVersion;
  private readonly intSize: number;
  private readonly offsetSize: number;

  constructor(stream: NodeJS.ReadableStream, prelude: PgPrelude) {
    this.stream = stream;
    this.version = getVersion(prelude);
    this.intSize = prelude.intSize;
    this.offsetSize = prelude.offSize;
  }

  static async readPrelude(stream: NodeJS.ReadableStream): Promise<PgPrelude> {
    const buffer = await readStreamBytes(stream, 11);

    return {
      magic: buffer.slice(0, 5).toString("ascii"),
      version: [buffer[5], buffer[6], buffer[7]],
      intSize: buffer[8],
      offSize: buffer[9],
      format: formats[buffer[10]],
    };
  }

  static validatePrelude(prelude: PgPrelude) {
    if (prelude.magic !== "PGDMP") {
      throw new Error("content did not begin with correct identifier sequence");
    }
    const version = getVersion(prelude);
    if (version === PgVersion.unknown) {
      const versionDesc = prelude.version.join(".");
      throw new Error(
        `Parser has not been validated against custom output format version ${versionDesc}`
      );
    }
    if (prelude.intSize !== 4) {
      throw new Error(
        `Parser has not been coded for ${prelude.intSize * 8}-bit integers`
      );
    }
    if (prelude.offSize !== 8) {
      throw new Error(
        `Parser has not been coded for ${prelude.offSize * 8}-bit offsets`
      );
    }
    if (prelude.format !== "Custom") {
      throw new Error(
        `Parser only supports the 'Custom' output format, not ${prelude.format}`
      );
    }
  }

  static validateTocEntry(toc: PgTocEntry) {
    const badDep = toc.deps.find((it) => !it.match(/^\d+$/));
    if (badDep) {
      throw new Error(`toc.deps has a non-integer value: ${badDep}`);
    }
  }

  async readHead(): Promise<PgHead> {
    const head: PgHead = {
      compression: await this.readInt(),
      sec: await this.readInt(),
      min: await this.readInt(),
      hour: await this.readInt(),
      mday: await this.readInt(),
      mon: 1 + (await this.readInt()),
      yr: 1900 + (await this.readInt()),
      isdst: await this.readInt(),
      dbname: await this.readString(),
      remoteVersion: await this.readString(),
      pgdumpVersion: await this.readString(),
      tocCount: await this.readInt(),
      tocEntries: [],
    };
    head.tocEntries = await this.readTocEntries(head.tocCount);
    return head;
  }

  async readDataBlockHead(): Promise<PgDataHead> {
    const type = (await readStreamBytes(this.stream, 1))[0];
    if (type !== 1) {
      throw new Error("Expected data block to begin with `0x01` byte");
    }

    return {
      type,
      dumpId: await this.readInt(),
    };
  }

  createDataRowIterable() {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self: PgCustomReader = this;
    return generator();

    async function* generator() {
      while (true) {
        const len = await self.readInt();
        if (len <= 0) {
          break;
        }
        yield await readStreamBytes(self.stream, len);
      }
    }
  }

  private async readTocEntries(tocCount: number) {
    const result = [];
    for (let i = 0; i < tocCount; i++) {
      result.push(await this.readTocEntry());
    }
    return result;
  }

  private async readTocEntry(): Promise<PgTocEntry> {
    return {
      dumpId: await this.readInt(),
      dataDumper: await this.readInt(),
      tableoid: await this.readString(),
      oid: await this.readString(),
      tag: await this.readString(),
      desc: await this.readString(),
      section: sections[Number(await this.readInt())],
      defn: await this.readString(),
      dropStmt: await this.readString(),
      copyStmt: await this.readString(),
      namespace: await this.readString(),
      tablespace: await this.readString(),
      tableam:
        this.version >= PgVersion.v1_14_0 ? await this.readString() : null,
      owner: await this.readString(),
      withOids: await this.readString(),
      deps: await this.readStringArray(),
      offset: await this.readOffset(),
    };
  }

  private async readInt(): Promise<number> {
    const buf = await readStreamBytes(this.stream, this.intSize + 1);
    const sign = buf[0];

    let result;
    if (this.intSize === 4) {
      result = buf.readInt32LE(1);
    } else {
      throw new Error(`Unsupported intSize=${this.intSize}`);
    }
    return sign ? -result : result;
  }

  private async readOffset(): Promise<PgOffset> {
    const buf = await readStreamBytes(this.stream, this.offsetSize + 1);
    const flag = offsetFlag[buf[0]];
    if (flag !== "Set") {
      return { flag, value: null };
    }

    let value;
    if (this.offsetSize == 8) {
      value = buf.readBigInt64LE(1);
    } else {
      throw new Error(`Unsupported size=${this.offsetSize}`);
    }

    return { flag, value };
  }

  private async readString(): Promise<string | null> {
    const len = await this.readInt();
    if (len === -1) {
      return null;
    }
    if (len <= 0) {
      return "";
    }
    const result = await readStreamBytes(this.stream, len);
    return result.toString("utf8");
  }

  private async readStringArray(): Promise<string[]> {
    const result = [];
    let item;
    while ((item = await this.readString())) {
      result.push(item);
    }
    return result;
  }
}

async function readStreamBytes(
  stream: NodeJS.ReadableStream,
  size: number
): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    doRead();

    function doRead() {
      const result = stream.read(size);
      if (result !== null) {
        if (result.length === size) {
          resolve(result as Buffer);
        } else {
          reject(new Error("Stream has ended with insufficient length"));
        }
      } else {
        const badEnd = () => {
          reject(new Error("did not expect end"));
        };
        stream.once("readable", () => {
          stream.off("end", badEnd);
          doRead();
        });
        stream.on("end", badEnd);
      }
    }
  });
}

export class PgCustomFormatter {
  private readonly version: PgVersion;
  private readonly intSize: number;
  private readonly offsetSize: number;
  constructor(prelude: PgPrelude) {
    this.version = getVersion(prelude);
    this.intSize = prelude.intSize;
    this.offsetSize = prelude.offSize;
  }

  formatFileHeader(prelude: PgPrelude, head: PgHead) {
    return Buffer.concat([this.formatPrelude(prelude), this.formatHead(head)]);
  }

  formatPrelude(prelude: PgPrelude): Buffer {
    const parts = [
      Buffer.from(prelude.magic, "ascii"),
      Buffer.from(prelude.version),
      Buffer.from([
        prelude.intSize,
        prelude.offSize,
        formats.indexOf(prelude.format),
      ]),
    ];
    return Buffer.concat(parts);
  }

  formatHead(head: PgHead): Buffer {
    const parts = [
      this.formatInt(head.compression),
      this.formatInt(head.sec),
      this.formatInt(head.min),
      this.formatInt(head.hour),
      this.formatInt(head.mday),
      this.formatInt(head.mon - 1),
      this.formatInt(head.yr - 1900),
      this.formatInt(head.isdst),
      this.formatString(head.dbname),
      this.formatString(head.remoteVersion),
      this.formatString(head.pgdumpVersion),
      this.formatInt(head.tocCount),
      ...head.tocEntries.flatMap((it) => this.formatTocEntry(it)),
    ];
    return Buffer.concat(parts);
  }

  createDataBlockFormatter(entry: PgTocEntry) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const fmtr: PgCustomFormatter = this;
    let bytesWritten = 0;
    let first = true;
    return {
      getBytesWritten: () => bytesWritten,
      transformStream: new Transform({
        transform(chunk: Buffer, encoding, callback) {
          if (first) {
            const head = fmtr.formatDataBlockHead(entry);
            bytesWritten += head.byteLength;
            this.push(head);
            first = false;
          }
          const result = fmtr.formatDataChunk(chunk);
          bytesWritten += result.byteLength;
          callback(null, result);
        },
        flush(callback) {
          const tail = fmtr.formatInt(0);
          bytesWritten += tail.byteLength;
          callback(null, tail);
        },
      }),
    };
  }

  private formatDataBlockHead(entry: PgTocEntry): Buffer {
    const result = Buffer.alloc(2 + this.intSize);
    result[0] = 1;

    if (this.intSize === 4) {
      result.writeInt32LE(entry.dumpId, 2);
    } else {
      throw new Error(`Unsupported intSize=${this.intSize}`);
    }
    return result;
  }

  private formatDataChunk(chunk: Buffer): Buffer {
    return Buffer.concat([this.formatInt(chunk.byteLength), chunk]);
  }

  private formatTocEntry(entry: PgTocEntry): Buffer[] {
    return [
      this.formatInt(entry.dumpId),
      this.formatInt(entry.dataDumper),
      this.formatString(entry.tableoid),
      this.formatString(entry.oid),
      this.formatString(entry.tag),
      this.formatString(entry.desc),
      this.formatInt(sections.indexOf(entry.section)),
      this.formatString(entry.defn),
      this.formatString(entry.dropStmt),
      this.formatString(entry.copyStmt),
      this.formatString(entry.namespace),
      this.formatString(entry.tablespace),
      this.version >= PgVersion.v1_14_0
        ? this.formatString(entry.tableam)
        : EMPTY,
      this.formatString(entry.owner),
      this.formatString(entry.withOids),
      ...this.formatStringArray(entry.deps),
      this.formatOffset(entry.offset.value, Boolean(entry.dataDumper)),
    ];
  }

  private formatInt(value: number) {
    const result = Buffer.alloc(1 + this.intSize);
    result[0] = value < 0 ? 1 : 0;
    if (this.intSize === 4) {
      result.writeInt32LE(Math.abs(value), 1);
    } else {
      throw new Error(`Unsupported intSize=${this.intSize}`);
    }
    return result;
  }

  private formatString(value: string | null) {
    if (value === null) {
      return this.formatInt(-1);
    }
    if (value.length === 0) {
      return this.formatInt(0);
    }

    const result = Buffer.alloc(1 + this.intSize + value.length);
    if (this.intSize === 4) {
      result.writeInt32LE(value.length, 1);
    } else {
      throw new Error(`Unsupported intSize=${this.intSize}`);
    }
    result.fill(value, this.intSize + 1, result.length, "utf8");
    return result;
  }

  private formatStringArray(values: string[]): Buffer[] {
    return [...values.map((it) => this.formatString(it)), this.formatInt(-1)];
  }

  private formatOffset(value: bigint | null, hasData: boolean) {
    const result = Buffer.alloc(1 + this.offsetSize);
    if (value === null) {
      result[0] = offsetFlag.indexOf(hasData ? "Not Set" : "No Data");
      return result;
    }
    result[0] = offsetFlag.indexOf("Set");
    if (this.offsetSize === 4) {
      result.writeInt32LE(Number(value), 1);
    } else if (this.offsetSize === 8) {
      result.writeBigInt64LE(value, 1);
    } else {
      throw new Error(`Unsupported offsetSize=${this.offsetSize}`);
    }

    return result;
  }
}

function getVersion(prelude: PgPrelude) {
  const [major, minor, patch] = prelude.version;
  if (major === 1 && minor === 13 && patch === 0) {
    return PgVersion.v1_13_0;
  }
  if (major === 1 && minor === 14 && patch === 0) {
    return PgVersion.v1_14_0;
  }
  return PgVersion.unknown;
}
