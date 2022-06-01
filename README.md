# [Obfuscate Learner Pii Demo](https://gitlab.com/outschool-eng/growth-pod/obfuscate-learner-pii-demo)
[![MIT license](https://img.shields.io/badge/License-MIT-blue.svg)](https://mit-license.org/)

## Contributors
- [Danny Kirchmeier](https://github.com/danthegoodman) - Code
- [Justin Hernandez](https://github.com/justinhernandez) - Demo

## Setup 🛠

### ✍️ Step 1: Edit .env
Create an `.env` file from the provided [`sample.env`](https://gitlab.com/outschool-eng/growth-pod/obfuscate-learner-pii-demo/-/blob/main/sample.env) file. Update your database settings to match your local installation.

```
DB_NAME=outschool_obfuscate_demo
DB_HOST=localhost
DB_USER=outschool
DB_PASSWORD=mypassword
```

_*hint:_ you can create a new `.env` file by running the following command from the root project folder:

```
cp ./sample.env ./.env
```

### 🌱 Step 2: Import seed data
Import the provided [`seed_data.sql`](https://gitlab.com/outschool-eng/growth-pod/obfuscate-learner-pii-demo/-/blob/main/seed_data.sql) file into your database.

```
psql -U outschool --password -h 127.0.0.1 -d outschool_obfuscate_demo -a -f ./seed_data.sql
```

### 📦 Step 3: Install packages
```
yarn install
```

_reference:_ [`package.json`](https://gitlab.com/outschool-eng/growth-pod/obfuscate-learner-pii-demo/-/blob/main/package.json#L16)

### 🏃‍♀️ Step 4: Run demo

```
yarn obfuscate
```

After running the above 👆 command you should find an obfuscated sql file at `./output/obfuscated.sql`. 

**Example export:**

The exported obfuscated sql file should look similar to the one provided in the demo [`example_export.sql`](https://gitlab.com/outschool-eng/growth-pod/obfuscate-learner-pii-demo/-/blob/main/example_export.sql)

## Key Pieces 🛠

Here are the key pieces to get you up and running:

* [pgCustom.ts](https://gitlab.com/outschool-eng/growth-pod/obfuscate-learner-pii-demo/-/blob/main/src/pgCustom.ts) - this the 💙 of the demo and helps us deconstruct and reassemble PostgreSQL streams on the fly. Worth a read through
* [tableMappings.ts](https://gitlab.com/outschool-eng/growth-pod/obfuscate-learner-pii-demo/-/blob/main/src/tableMappings.ts) - this is where we map the table columns to the obfuscation methods we would like to filter them through
* [filters.ts](https://gitlab.com/outschool-eng/growth-pod/obfuscate-learner-pii-demo/-/blob/main/src/filters.ts) - three obfuscation filters are included within the demo, they are: [*replaceEmailWithColumn*](https://gitlab.com/outschool-eng/growth-pod/obfuscate-learner-pii-demo/-/blob/main/src/filters.ts#L4), [*replaceWithNull*](https://gitlab.com/outschool-eng/growth-pod/obfuscate-learner-pii-demo/-/blob/main/src/filters.ts#L17), and [*replaceWithScrambledText*](https://gitlab.com/outschool-eng/growth-pod/obfuscate-learner-pii-demo/-/blob/main/src/filters.ts#L19). Feel free to extend this file and add your own!

## Misc

* Shout out to BrianC's [`node-pg-copy-streams`](https://www.npmjs.com/package/pg-copy-streams) project. We used the [`@types`](https://www.npmjs.com/package/@types/pg-copy-streams) for this demo.