# Obfuscate Learner Pii Demo
[![MIT license](https://img.shields.io/badge/License-MIT-blue.svg)](https://lbesson.mit-license.org/)

## Setup 🛠

### ✍️ Step 1: Edit .env
Create an `.env` file from the provided `sample.env` file. Update your database settings to match your local installation.

```
DB_NAME=outschool_obfuscate_demo
DB_HOST=localhost
DB_USER=outschool
DB_PASSWORD=mypassword
```

### 🌱 Step 2: Import seed data
Import the provided seed sql file into your database.

```
psql -U outschool --password -h 127.0.0.1 -d outschool_obfuscate_demo -a -f seedData.sql
```

### 📦 Step 3: Install packages
```
yarn install
```

### 🏃‍♀️ Step 4: Run demo

```
yarn obfuscate
```
