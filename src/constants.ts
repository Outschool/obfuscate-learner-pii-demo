export const DATA_COLUMN_SEPARATOR = "\t".charCodeAt(0);

export const DATA_ROW_TERMINATOR = "\n".charCodeAt(0);

export const EMPTY = Buffer.from("");

export const FINAL_DATA_ROW = Buffer.from("\\.\n\n\n");

export const ONE_MB = 1024 ** 2;

export const OUTSCHOOL_DOMAIN = Buffer.from("@outschool.com");

export const PG_NULL = Buffer.from("\\N");

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
