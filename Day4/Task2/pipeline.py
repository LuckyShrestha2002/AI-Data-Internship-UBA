

import sys
import os
import requests
from mysql.connector import Error

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db_connection import get_connection, ensure_database

DB_NAME   = "uba_internship"
USERS_URL = "https://jsonplaceholder.typicode.com/users"
POSTS_URL = "https://jsonplaceholder.typicode.com/posts"
TIMEOUT   = 10


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


def fetch_json(url: str, label: str) -> list:
    print(f"  ↳ GET {url}")
    try:
        r = requests.get(url, timeout=TIMEOUT)
    except requests.exceptions.ConnectionError:
        raise ConnectionError(f"Cannot reach {url} — check internet.")
    except requests.exceptions.Timeout:
        raise TimeoutError(f"Timed out after {TIMEOUT}s.")
    if r.status_code != 200:
        raise ValueError(f"HTTP {r.status_code} from {url}")
    data = r.json()
    print(f"  ✔  {len(data)} {label} fetched  (HTTP {r.status_code})")
    return data


def create_tables(conn) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id           INT          PRIMARY KEY,
            name         VARCHAR(100) NOT NULL,
            email        VARCHAR(150) NOT NULL UNIQUE,
            phone        VARCHAR(50),
            city         VARCHAR(100),
            company_name VARCHAR(150)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id      INT          PRIMARY KEY,
            user_id INT          NOT NULL,
            title   VARCHAR(255) NOT NULL,
            body    TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()
    cursor.close()
    print("  ✔  Tables 'users' and 'posts' ready.")


def insert_users(conn, users: list) -> None:
    cursor   = conn.cursor()
    inserted = 0
    for u in users:
        try:
            city    = u.get("address", {}).get("city", "Unknown")
            company = u.get("company",  {}).get("name", "Unknown")
            cursor.execute("""
                INSERT IGNORE INTO users
                    (id, name, email, phone, city, company_name)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (u["id"], u["name"], u["email"],
                  u.get("phone",""), city, company))
            if cursor.rowcount:
                inserted += 1
        except KeyError as e:
            print(f"  ⚠  Skipping user — missing key: {e}")
    conn.commit()
    cursor.close()
    print(f"  ✔  Users inserted: {inserted}")


def insert_posts(conn, posts: list) -> None:
    cursor   = conn.cursor()
    inserted = 0
    for p in posts:
        if p.get("userId") not in {1, 2, 3}:
            continue
        cursor.execute("""
            INSERT IGNORE INTO posts (id, user_id, title, body)
            VALUES (%s, %s, %s, %s)
        """, (p["id"], p["userId"], p["title"], p.get("body", "")))
        if cursor.rowcount:
            inserted += 1
    conn.commit()
    cursor.close()
    print(f"  ✔  Posts inserted (user_id 1–3): {inserted}")


def query_users_alphabetical(conn) -> None:
    print_section("Query 1 · All users sorted A → Z by name")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, email, city, company_name
        FROM   users
        ORDER  BY name ASC
    """)
    print_table(cursor)
    cursor.close()


def query_users_same_city(conn) -> None:
    print_section("Query 2 · Cities shared by more than one user")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT   city,
                 COUNT(*) AS user_count,
                 GROUP_CONCAT(name SEPARATOR ', ') AS users
        FROM     users
        GROUP BY city
        HAVING   COUNT(*) > 1
        ORDER BY user_count DESC
    """)
    rows = cursor.fetchall()
    headers = [d[0] for d in cursor.description]
    if not rows:
        print("  → Each user is in a unique city in this dataset.")
    else:
        col_widths = [len(h) for h in headers]
        for row in rows:
            for i, v in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(v)))
        print("  " + "  ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)))
        print("  " + "  ".join("─" * w for w in col_widths))
        for row in rows:
            print("  " + "  ".join(str(v).ljust(col_widths[i]) for i, v in enumerate(row)))
    cursor.close()


def query_join_posts(conn) -> None:
    print_section("Bonus · JOIN — each user name + post count")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT   u.name,
                 u.city,
                 COUNT(p.id) AS posts_stored
        FROM     users u
        LEFT JOIN posts p ON p.user_id = u.id
        GROUP BY u.id
        ORDER BY posts_stored DESC, u.name ASC
    """)
    print_table(cursor)
    cursor.close()


def main() -> None:
    print("\n" + "═" * 65)
    print("  🔗  Task 02 · API → MySQL Pipeline")
    print("  Week 3 · Day 4 | UBA Solutions Internship")
    print("═" * 65)

    conn = None
    try:
        print_section("Step 1 · Fetching from API")
        users = fetch_json(USERS_URL, "users")
        posts = fetch_json(POSTS_URL, "posts")

        print_section("Step 2 · Connecting to MySQL")
        ensure_database(DB_NAME)
        conn = get_connection(DB_NAME)
        print(f"  ✔  Connected  →  '{DB_NAME}'")
        create_tables(conn)

        print_section("Step 3 · Inserting records")
        insert_users(conn, users)
        insert_posts(conn, posts)

        query_users_alphabetical(conn)
        query_users_same_city(conn)
        query_join_posts(conn)

    except (ConnectionError, TimeoutError, ValueError) as api_err:
        print(f"\n  ✖  {api_err}")
    except Error as e:
        print(f"\n  ✖  MySQL error: {e}")
    except Exception as e:
        print(f"\n  ✖  Unexpected: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("\n  ✔  Connection closed.")

    print("\n" + "═" * 65)
    print("  ✅  Task 02 complete")
    print("═" * 65 + "\n")


if __name__ == "__main__":
    main()