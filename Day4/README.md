# Day 4 — Data Types, Storage & MySQL
**UBA Solutions · Data Engineering Internship · Week 3**


## What this session covered

| Topic | Summary |
| Data types | Structured · Unstructured · Semi-structured |
| Why not CSV | No querying, no integrity, slow at scale, no concurrency |
| MySQL | CREATE TABLE, INSERT, SELECT, WHERE, UPDATE, DELETE |
| Python + MySQL | mysql.connector — connect, cursor, execute, commit |
| Full pipeline | API → parse JSON → store in DB → query → export |

## Project structure
Day4/
├── .env                  ← MySQL credentials (never commit this)
├── .gitignore
├── db_connection.py      ← Shared MySQL connection used by all tasks
├── README.md
│
├── Task1/
│   └── library.py        ← Books database — 4 queries + JOIN bonus
├── Task2/
│   └── pipeline.py       ← API → MySQL (users + posts)
├── Task3/
│   └── weather.py        ← Open-Meteo → MySQL → summary.txt
├── Task4/
│   └── grades.py         ← UPDATE, DELETE, ALTER TABLE
└── Task5/
└── full_pipeline.py  ← Capstone: API → MySQL → CSV + TXT export
All 5 tasks write to one shared database: `uba_internship`

---

## Setup

```bash
# 1. Create virtual environment
python -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install mysql-connector-python python-dotenv requests

# 3. Start MySQL
brew services start mysql

# 4. Run any task
cd Task1 && python library.py
cd Task2 && python pipeline.py
cd Task3 && python weather.py
cd Task4 && python grades.py
cd Task5 && python full_pipeline.py
```

## Database

All tasks connect to one MySQL database: `uba_internship`

Tables created:
- `books` — Task 1
- `reviews` — Task 1 bonus
- `users` — Task 2
- `posts` — Task 2
- `forecasts` — Task 3
- `students` — Task 4
- `countries` — Task 5


## Concepts used from all sessions

| Session | Concept | Where used |
|---|---|---|
| Day 1 | File I/O, csv module | Task 3 (summary.txt), Task 5 (report.csv + report.txt) |
| Day 2 | requests, .json(), nested JSON, status codes | Task 2, 3, 5 |
| Day 3 | try/except, .env, os.environ, venv | All tasks |
| Day 4 | MySQL, CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, ALTER TABLE, GROUP BY, JOIN | All tasks |


## Key SQL commands

```sql
-- Create table
CREATE TABLE IF NOT EXISTS books (
    id     INT AUTO_INCREMENT PRIMARY KEY,
    title  VARCHAR(200) NOT NULL,
    rating DECIMAL(3,1)
);

-- Insert
INSERT IGNORE INTO books (title, rating) VALUES ('Dune', 4.9);

-- Query with filter
SELECT title, rating FROM books
WHERE rating > 4.0 ORDER BY rating DESC;

-- Group by
SELECT genre, COUNT(*), AVG(rating)
FROM books GROUP BY genre;

-- Update
UPDATE students SET grade = 'A' WHERE score >= 80;

-- Delete
DELETE FROM students WHERE score < 50;

-- Alter table
ALTER TABLE students ADD COLUMN passed TINYINT(1) DEFAULT 0;

-- Join
SELECT u.name, COUNT(p.id)
FROM users u
LEFT JOIN posts p ON p.user_id = u.id
GROUP BY u.id;
```


## How to explain queries during presentation

**WHERE**
> "WHERE filters rows before returning them. Only rows matching the condition are included."

**ORDER BY DESC**
> "ORDER BY sorts results. DESC means highest first, ASC means lowest first."

**GROUP BY + COUNT()**
> "GROUP BY collapses rows sharing the same value into one group. COUNT tells me how many rows are in each group."

**HAVING**
> "HAVING is like WHERE but runs after GROUP BY. I use it to filter groups, not individual rows."

**JOIN**
> "JOIN connects two tables on a matching column. Here book_id in reviews matches id in books."

**INSERT IGNORE**
> "If a row with the same unique key exists, MySQL skips it silently. Makes the script safe to run multiple times."

**ALTER TABLE**
> "ALTER TABLE changes the structure of an existing table. I used it to add a new column without recreating the whole table."


