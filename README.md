# Obfuscate Learner Pii Demo
[![MIT license](https://img.shields.io/badge/License-MIT-blue.svg)](https://lbesson.mit-license.org/)

## Setup ⚒

### ✍️ Step 1: Edit .env
Copy a `.env` file from the provided `sample.env` file. Update your database settings to match your local installation.

### 🌱 Step 2: Import seed data
Import the provided seed sql file into your database.

```
psql -U db_user -h 127.0.0.1 -d outschool_obfuscate_demo -a -f seedData.sql
```

### 📦 Step 3: Install packages
```
yarn install
```

### 🏃‍♀️ Step 4: Run demo

```
yarn obfuscate
```



