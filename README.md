# Day 3 — Automated News Data Pipeline

A fully automated data pipeline that fetches top headlines from five countries
using the GNews API, stores them in a structured CSV, and performs analytical
queries to answer eight business questions.

---

## Project Structure

```
.
├── .env.example          # Template for environment variables
├── .gitignore
├── requirements.txt
├── pipeline.py           # Fetches data from GNews API and saves to CSV
├── analysis.py           # Reads CSV and answers all 8 analytical questions
└── data/
    ├── headlines_raw.csv       # Full dataset (auto-generated)
    └── headlines_filtered.csv  # Titles > 6 words only (auto-generated)
```

---

## Setup

### 1. Clone and create the Day3 branch

```bash
git checkout main
git pull origin main
git checkout -b Day3
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv

# Linux / macOS
source venv/bin/activate

# Windows
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure your API key

```bash
cp .env.example .env
```

Open `.env` and replace `your_gnews_api_key_here` with your actual GNews API key
from [gnews.io](https://gnews.io).

---

## Usage

### Step 1 — Run the pipeline to fetch and store data

```bash
python pipeline.py
```

This fetches up to 10 headlines per country (Nepal, India, USA, UK, Australia)
and appends only new records to `data/headlines_raw.csv`.

### Step 2 — Run the analysis

```bash
python analysis.py
```

This reads from the CSV and prints answers to all eight questions, and writes
a filtered CSV to `data/headlines_filtered.csv`.

---

## Duplicate Prevention

Each article in the GNews API has a unique canonical URL.  Before writing any
row, `pipeline.py` loads the set of URLs already present in the CSV and skips
any article whose URL has been seen before.  Running the script multiple times
is therefore safe — the dataset grows only when new articles appear.

---

## Questions Answered

| # | Question |
|---|----------|
| 1 | Which country published the most headlines today? |
| 2 | Average word count per headline title, per country |
| 3 | Headlines appearing in more than one country |
| 4 | News source with the most headlines across all countries |
| 5 | % of headlines published in last 6 hours vs older |
| 6 | How duplicate rows are prevented across multiple runs |
| 7 | Headlines with titles longer than 6 words (saved to filtered CSV) |
| 8 | Country with the longest/shortest average headline |

---

## Notes

- All analysis is performed by reading from the CSV — never directly from the API response.
- Missing API fields are written as `N/A` — no cell is ever left empty.
- Column names are lowercase with no spaces.
