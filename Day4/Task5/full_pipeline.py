

import sys
import os
import requests
import csv
from datetime import datetime
from mysql.connector import Error

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db_connection import get_connection, ensure_database

DB_NAME  = os.environ.get("DB_NAME",  "uba_internship")
CSV_FILE = os.environ.get("CSV_FILE", "report.csv")
TXT_FILE = os.environ.get("TXT_FILE", "report.txt")
API_URL  = os.environ.get("COUNTRIES_API_URL",
           "https://restcountries.com/v3.1/region/asia")
TIMEOUT  = int(os.environ.get("TIMEOUT", "10"))


def print_section(title: str) -> None:
    print(f"\n{'─' * 65}")
    print(f"  {title}")
    print(f"{'─' * 65}")


def print_table(cursor) -> None:
    rows    = cursor.fetchall()
    headers = [d[0] for d in cursor.description]
    if not rows:
        print("  (no results)")
        return
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, v in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(v)))
    print("  " + "  ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)))
    print("  " + "  ".join("─" * w for w in col_widths))
    for row in rows:
        print("  " + "  ".join(str(v).ljust(col_widths[i]) for i, v in enumerate(row)))
    print(f"\n  → {len(rows)} row(s) returned")


def fetch_data(url: str) -> list:
    print(f"  ↳ GET  {url}")
    try:
        response = requests.get(url, timeout=TIMEOUT)
    except requests.exceptions.ConnectionError:
        raise ConnectionError("No internet connection or host unreachable.")
    except requests.exceptions.Timeout:
        raise TimeoutError(f"Request timed out after {TIMEOUT}s.")
    if response.status_code == 404:
        raise ValueError("404 — endpoint not found.")
    if response.status_code == 429:
        raise RuntimeError("429 — rate limited.")
    if response.status_code != 200:
        raise ValueError(f"Unexpected HTTP {response.status_code}.")
    try:
        data = response.json()
    except requests.exceptions.JSONDecodeError:
        raise ValueError("Response is not valid JSON.")
    print(f"  ✔  HTTP {response.status_code}  —  {len(data)} records received")
    return data


def create_table(conn) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS countries (
            id         INT          AUTO_INCREMENT PRIMARY KEY,
            name       VARCHAR(150) NOT NULL UNIQUE,
            region     VARCHAR(100),
            subregion  VARCHAR(100),
            population BIGINT,
            area       DECIMAL(15,2),
            capital    VARCHAR(100),
            currency   VARCHAR(200),
            languages  VARCHAR(300)
        )
    """)
    conn.commit()
    cursor.close()
    print("  ✔  Table 'countries' ready.")


def store_data(conn, countries: list) -> int:
    cursor   = conn.cursor()
    inserted = 0
    errors   = 0
    for c in countries:
        try:
            name       = c.get("name", {}).get("common", "Unknown")
            region     = c.get("region",    "Unknown")
            subregion  = c.get("subregion", "Unknown")
            population = c.get("population", 0)
            area       = c.get("area", 0.0)
            capital    = (c.get("capital") or ["—"])[0]
            currencies = ", ".join(
                v.get("name", k) for k, v in c.get("currencies", {}).items()
            ) or "—"
            languages = ", ".join(c.get("languages", {}).values()) or "—"
            cursor.execute("""
                INSERT IGNORE INTO countries
                    (name, region, subregion, population,
                     area, capital, currency, languages)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (name, region, subregion, population,
                  area, capital, currencies, languages))
            if cursor.rowcount:
                inserted += 1
        except (KeyError, TypeError) as e:
            errors += 1
            print(f"  ⚠  Skipping: {e}")
    conn.commit()
    cursor.close()
    print(f"  ✔  Inserted: {inserted}  |  Errors: {errors}")
    return inserted


def run_report(conn) -> dict:
    results = {}

    print_section("Query 1 · Top 10 most populous Asian countries")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, subregion, population
        FROM   countries
        ORDER BY population DESC
        LIMIT 10
    """)
    results["top_populous"] = cursor.fetchall()
    cursor.close()

    cursor = conn.cursor()
    cursor.execute("SELECT name, subregion, population FROM countries ORDER BY population DESC LIMIT 10")
    print_table(cursor)
    cursor.close()

    print_section("Query 2 · Countries & avg population by subregion")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT   subregion,
                 COUNT(*)                  AS countries,
                 ROUND(AVG(population), 0) AS avg_population,
                 ROUND(AVG(area), 0)       AS avg_area_km2
        FROM     countries
        WHERE    subregion != 'Unknown'
        GROUP BY subregion
        ORDER BY avg_population DESC
    """)
    results["by_subregion"] = cursor.fetchall()
    cursor.close()

    cursor = conn.cursor()
    cursor.execute("""
        SELECT subregion, COUNT(*) AS countries,
               ROUND(AVG(population),0) AS avg_pop,
               ROUND(AVG(area),0) AS avg_area
        FROM countries WHERE subregion != 'Unknown'
        GROUP BY subregion ORDER BY avg_pop DESC
    """)
    print_table(cursor)
    cursor.close()

    print_section("Query 3 · Countries with population over 100 million")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, subregion, population, capital
        FROM   countries
        WHERE  population > 100000000
        ORDER BY population DESC
    """)
    results["large_countries"] = cursor.fetchall()
    cursor.close()

    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, subregion, population, capital
        FROM countries WHERE population > 100000000
        ORDER BY population DESC
    """)
    print_table(cursor)
    cursor.close()

    return results


def export_csv(conn, filepath: str) -> int:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, region, subregion, population,
               area, capital, currency, languages
        FROM   countries ORDER BY population DESC
    """)
    rows    = cursor.fetchall()
    headers = ["Name","Region","Subregion","Population",
               "Area (km²)","Capital","Currency","Languages"]
    cursor.close()
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"  ✔  {len(rows)} rows  →  '{filepath}'")
    return len(rows)


def export_txt(results: dict, filepath: str, total: int) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "═" * 65,
        "  ASIAN COUNTRIES — FULL PIPELINE REPORT",
        f"  Generated  : {now}",
        f"  Source     : REST Countries API",
        f"  Total rows : {total} countries",
        "═" * 65, "",
        "── Top 10 Most Populous ────────────────────────────────────", "",
    ]
    for name, sub, pop in results.get("top_populous", []):
        lines.append(f"  {name:<30}  {sub:<22}  {pop:>12,}")
    lines += ["", "── Subregion Summary ───────────────────────────────────────", ""]
    for sub, count, avg_pop, avg_area in results.get("by_subregion", []):
        lines.append(
            f"  {sub:<25}  {count:>3} countries  "
            f"Avg pop: {int(avg_pop):>10,}  Avg area: {int(avg_area):>8,} km²"
        )
    lines += ["", "── Population > 100M ───────────────────────────────────────", ""]
    for name, sub, pop, capital in results.get("large_countries", []):
        lines.append(f"  {name:<25}  Capital: {capital:<20}  Pop: {pop:>12,}")
    lines += ["", "═" * 65, "  End of report", "═" * 65]
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"  ✔  Report written  →  '{filepath}'")


def main() -> None:
    print("\n" + "═" * 65)
    print("  🌏  Task 05 · Full Pipeline Capstone")
    print("  Week 3 · Day 4 | UBA Solutions Internship")
    print("═" * 65)

    conn = None
    try:
        print_section("Step 1 · Fetch from API")
        raw = fetch_data(API_URL)

        print_section("Step 2 · Connect to MySQL + create table")
        ensure_database(DB_NAME)
        conn = get_connection(DB_NAME)
        print(f"  ✔  Connected  →  '{DB_NAME}'")
        create_table(conn)

        print_section("Step 3 · Parse + store records")
        store_data(conn, raw)

        results = run_report(conn)

        print_section("Step 4 · Export CSV + TXT")
        total = export_csv(conn, CSV_FILE)
        export_txt(results, TXT_FILE, total)

    except (ConnectionError, TimeoutError) as net:
        print(f"\n  ✖  Network: {net}")
    except ValueError as ve:
        print(f"\n  ✖  Data: {ve}")
    except RuntimeError as rt:
        print(f"\n  ✖  Runtime: {rt}")
    except Error as e:
        print(f"\n  ✖  MySQL: {e}")
    except OSError as e:
        print(f"\n  ✖  File: {e}")
    except Exception as e:
        print(f"\n  ✖  Unexpected {type(e).__name__}: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("\n  ✔  Connection closed.")

    print("\n" + "═" * 65)
    print(f"  ✅  Task 05 complete")
    print(f"      DB  → {DB_NAME}")
    print(f"      CSV → {CSV_FILE}")
    print(f"      TXT → {TXT_FILE}")
    print("═" * 65 + "\n")


if __name__ == "__main__":
    main()