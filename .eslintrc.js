module.exports = {
  env: {
    es2021: true,
    node: true,
  },
  extends: ["eslint:recommended", "plugin:@typescript-eslint/recommended"],
  parser: "@typescript-eslint/parser",
  parserOptions: {
    ecmaVersion: 13,
    sourceType: "module",
  },
  plugins: ["@typescript-eslint", "import", "simple-import-sort"],
  root: true,
  rules: {
    "@typescript-eslint/indent": ["error", 2],
    "import/first": "error",
    "import/newline-after-import": "error",
    "import/no-duplicates": "error",
    indent: "off",
    "linebreak-style": ["error", "unix"],
    quotes: ["error", "double"],
    semi: ["error", "always"],
    "simple-import-sort/exports": "error",
    "simple-import-sort/imports": "error",
  },
};
