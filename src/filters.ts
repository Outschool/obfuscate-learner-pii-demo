import { OUTSCHOOL_DOMAIN, PG_NULL } from "./constants";
import { ColumnMapper } from "./types";

export function replaceEmailWithColumn(uidColumn: string): ColumnMapper {
  return (content, columns, row) => {
    if (content.equals(PG_NULL)) {
      return PG_NULL;
    }
    if (bufferEndsWith(content, OUTSCHOOL_DOMAIN)) {
      return content;
    }
    const uid = parsePgString(row[columns.indexOf(uidColumn)]);
    return serializePgString(uid + "@obfuscated.outschool.com");
  };
}

export const replaceWithNull: ColumnMapper = () => PG_NULL;

export const replaceWithScrambledText: ColumnMapper = content => {
  const str = parsePgString(content);
  if (str === null) {
    return PG_NULL;
  }

  const result = str
    .replace(/[A-z]/g, randomLetter)
    .replace(/[0-9]/g, randomDigit);
  return serializePgString(result);
};

function bufferEndsWith(buf: Buffer, ending: Buffer) {
  return -1 !== buf.indexOf(ending, buf.byteLength - ending.byteLength);
}

function parsePgString(content: Buffer): string | null {
  if (content.equals(PG_NULL)) {
    return null;
  }
  //Note: The docs claim that `\OCT` and `\xHEX` patterns are supported but
  // that they will not be emitted by COPY TO. We trust this claim.
  return content.toString("utf8").replace(/\\./g, str => {
    if (str === "\\\\") {
      return "\\";
    }
    if (str === "\\b") {
      return "\b";
    }
    if (str === "\\t") {
      return "\t";
    }
    if (str === "\\n") {
      return "\n";
    }
    if (str === "\\v") {
      return "\v";
    }
    if (str === "\\f") {
      return "\f";
    }
    if (str === "\\r") {
      return "\r";
    }
    return str[1];
  });
}

function randomLetter() {
  return Math.floor(Math.random() * 26 + 10).toString(36);
}

function randomDigit() {
  return Math.floor(Math.random() * 10).toString(10);
}

function serializePgString(str: string | null) {
  if (str === null) {
    return PG_NULL;
  }
  return Buffer.from(
    str
      .replace(/\\/g, "\\\\")
      /* eslint-disable no-control-regex */
      .replace(/\x08/g, "\\b")
      .replace(/\x09/g, "\\t")
      .replace(/\x0a/g, "\\n")
      .replace(/\x0b/g, "\\v")
      .replace(/\x0c/g, "\\f")
      .replace(/\x0d/g, "\\r")
  );
}
