

import sys
import os
import requests
from datetime import datetime
from mysql.connector import Error

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db_connection import get_connection, ensure_database

DB_NAME      = "uba_internship"
SUMMARY_FILE = "summary.txt"
API_BASE     = "https://api.open-meteo.com/v1/forecast"
TIMEOUT      = 10

CITIES = [
    {"name": "Kathmandu", "lat": 27.7172, "lon": 85.3240},
    {"name": "London",    "lat": 51.5074, "lon": -0.1278},
    {"name": "Tokyo",     "lat": 35.6762, "lon": 139.6503},
]


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


def fetch_weather(city: dict) -> list:
    params = {
        "latitude":      city["lat"],
        "longitude":     city["lon"],
        "daily":         "temperature_2m_max,temperature_2m_min",
        "timezone":      "auto",
        "forecast_days": 7,
    }
    try:
        r = requests.get(API_BASE, params=params, timeout=TIMEOUT)
    except requests.exceptions.ConnectionError:
        raise ConnectionError("Cannot reach Open-Meteo API.")
    except requests.exceptions.Timeout:
        raise TimeoutError("Request timed out.")

    if r.status_code != 200:
        raise ValueError(f"HTTP {r.status_code} for {city['name']}")

    daily = r.json().get("daily", {})
    records = [
        {"city": city["name"], "date": d,
         "max_temp": round(mx, 1), "min_temp": round(mn, 1)}
        for d, mx, mn in zip(
            daily.get("time", []),
            daily.get("temperature_2m_max", []),
            daily.get("temperature_2m_min", [])
        )
        if mx is not None and mn is not None
    ]
    print(f"  ✔  {city['name']:<12}  {len(records)} days fetched")
    return records


def create_table(conn) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS forecasts (
            id       INT          AUTO_INCREMENT PRIMARY KEY,
            city     VARCHAR(100) NOT NULL,
            date     DATE         NOT NULL,
            max_temp DECIMAL(5,1),
            min_temp DECIMAL(5,1),
            UNIQUE KEY uq_city_date (city, date)
        )
    """)
    conn.commit()
    cursor.close()
    print("  ✔  Table 'forecasts' ready.")


def insert_weather(conn, records: list) -> int:
    cursor   = conn.cursor()
    inserted = 0
    for r in records:
        cursor.execute("""
            INSERT IGNORE INTO forecasts (city, date, max_temp, min_temp)
            VALUES (%s, %s, %s, %s)
        """, (r["city"], r["date"], r["max_temp"], r["min_temp"]))
        if cursor.rowcount:
            inserted += 1
    conn.commit()
    cursor.close()
    return inserted


def query_hottest_city(conn) -> list:
    print_section("Query 1 · Average max temperature per city")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT   city,
                 ROUND(AVG(max_temp), 2) AS avg_max_temp,
                 ROUND(AVG(min_temp), 2) AS avg_min_temp
        FROM     forecasts
        GROUP BY city
        ORDER BY avg_max_temp DESC
    """)
    print_table(cursor)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT city, ROUND(AVG(max_temp),2), ROUND(AVG(min_temp),2)
        FROM forecasts GROUP BY city ORDER BY 2 DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def query_hottest_day(conn) -> list:
    print_section("Query 2 · Single hottest day across all cities")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT city, date, max_temp
        FROM   forecasts
        ORDER  BY max_temp DESC
        LIMIT  1
    """)
    print_table(cursor)
    cursor = conn.cursor()
    cursor.execute("SELECT city, date, max_temp FROM forecasts ORDER BY max_temp DESC LIMIT 1")
    rows = cursor.fetchall()
    cursor.close()
    return rows


def query_high_swing(conn) -> list:
    print_section("Query 3 · Days where temp swing (max - min) > 10°C")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT city, date, max_temp, min_temp,
               ROUND(max_temp - min_temp, 1) AS swing_c
        FROM   forecasts
        WHERE  (max_temp - min_temp) > 10
        ORDER  BY swing_c DESC
    """)
    rows = cursor.fetchall()
    headers = [d[0] for d in cursor.description]
    cursor.close()
    if not rows:
        print("  → No days with a swing > 10°C found.")
        return []
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, v in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(v)))
    print("  " + "  ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)))
    print("  " + "  ".join("─" * w for w in col_widths))
    for row in rows:
        print("  " + "  ".join(str(v).ljust(col_widths[i]) for i, v in enumerate(row)))
    return rows


def export_summary(city_rows, day_rows, swing_rows) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "═" * 65,
        "  WEATHER ANALYSIS SUMMARY REPORT",
        f"  Generated  : {now}",
        f"  Cities     : Kathmandu, London, Tokyo",
        "═" * 65, "",
        "── Average Temperatures per City ──────────────────────────", "",
    ]
    for city, avg_max, avg_min in city_rows:
        lines.append(f"  {city:<14}  Avg Max: {avg_max}°C   Avg Min: {avg_min}°C")
    lines += ["", "── Hottest Single Day ──────────────────────────────────────", ""]
    if day_rows:
        city, date, temp = day_rows[0]
        lines.append(f"  {city} on {date}  →  {temp}°C")
    lines += ["", "── Days with Swing > 10°C ──────────────────────────────────", ""]
    if not swing_rows:
        lines.append("  No days found with swing greater than 10°C.")
    else:
        for city, date, mx, mn, swing in swing_rows:
            lines.append(f"  {city:<14}  {date}   {mx}°C / {mn}°C   swing: {swing}°C")
    lines += ["", "═" * 65, "  End of report", "═" * 65]
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"\n  ✔  Summary written → '{SUMMARY_FILE}'")


def main() -> None:
    print("\n" + "═" * 65)
    print("  🌦   Task 03 · Weather Data + Analysis")
    print("  Week 3 · Day 4 | UBA Solutions Internship")
    print("═" * 65)

    conn = None
    try:
        print_section("Step 1 · Fetching forecast from Open-Meteo")
        all_records = []
        for city in CITIES:
            all_records.extend(fetch_weather(city))
        print(f"\n  ✔  Total records: {len(all_records)}")

        print_section("Step 2 · Connecting to MySQL")
        ensure_database(DB_NAME)
        conn = get_connection(DB_NAME)
        print(f"  ✔  Connected  →  '{DB_NAME}'")
        create_table(conn)

        print_section("Step 3 · Inserting records")
        n = insert_weather(conn, all_records)
        print(f"  ✔  Rows inserted: {n}")

        city_rows  = query_hottest_city(conn)
        day_rows   = query_hottest_day(conn)
        swing_rows = query_high_swing(conn)

        export_summary(city_rows, day_rows, swing_rows)

    except (ConnectionError, TimeoutError, ValueError) as e:
        print(f"\n  ✖  {e}")
    except Error as e:
        print(f"\n  ✖  MySQL error: {e}")
    except OSError as e:
        print(f"\n  ✖  File error: {e}")
    except Exception as e:
        print(f"\n  ✖  Unexpected: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("\n  ✔  Connection closed.")

    print("\n" + "═" * 65)
    print("  ✅  Task 03 complete")
    print("═" * 65 + "\n")


if __name__ == "__main__":
    main()