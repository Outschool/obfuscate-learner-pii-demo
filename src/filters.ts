import { OUTSCHOOL_DOMAIN, PG_NULL } from "./constants";
import {
  bufferEndsWith,
  parsePgString,
  randomDigit,
  randomLetter,
  serializePgString,
} from "./shared";
import { ColumnMapper } from "./types";

export function replaceEmailBasedOnColumn(uidColumn: string): ColumnMapper {
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
