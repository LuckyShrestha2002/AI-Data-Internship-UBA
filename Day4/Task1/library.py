
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db_connection import get_connection, ensure_database
from mysql.connector import Error

DB_NAME = "uba_internship"


def print_section(title: str) -> None:
    print(f"\n{'─' * 62}")
    print(f"  {title}")
    print(f"{'─' * 62}")


def print_table(cursor) -> None:
    rows    = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    if not rows:
        print("  (no results)")
        return
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))
    print("  " + "  ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)))
    print("  " + "  ".join("─" * w for w in col_widths))
    for row in rows:
        print("  " + "  ".join(str(v).ljust(col_widths[i]) for i, v in enumerate(row)))
    print(f"\n  → {len(rows)} row(s) returned")


def create_table(conn) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id     INT          AUTO_INCREMENT PRIMARY KEY,
            title  VARCHAR(200) NOT NULL,
            author VARCHAR(150) NOT NULL,
            year   INT          NOT NULL,
            genre  VARCHAR(50)  NOT NULL,
            rating DECIMAL(3,1) NOT NULL
        )
    """)
    conn.commit()
    cursor.close()
    print("  ✔  Table 'books' created / verified.")


BOOKS = [
    ("The Pragmatic Programmer",   "David Thomas",        1999, "Tech",        4.7),
    ("Clean Code",                 "Robert C. Martin",    2008, "Tech",        4.5),
    ("Sapiens",                    "Yuval Noah Harari",   2011, "Non-Fiction",  4.6),
    ("Atomic Habits",              "James Clear",         2018, "Self-Help",    4.8),
    ("Dune",                       "Frank Herbert",       1965, "Fiction",      4.9),
    ("The Alchemist",              "Paulo Coelho",        1988, "Fiction",      4.2),
    ("Educated",                   "Tara Westover",       2018, "Memoir",       4.7),
    ("Project Hail Mary",          "Andy Weir",           2021, "Fiction",      4.8),
    ("Deep Work",                  "Cal Newport",         2016, "Self-Help",    4.4),
    ("The Great Gatsby",           "F. Scott Fitzgerald", 1925, "Fiction",      3.9),
]


def insert_books(conn) -> None:
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM books")
    if cursor.fetchone()[0] > 0:
        print("  ✔  Books already in table — skipping insert.")
        cursor.close()
        return
    cursor.executemany("""
        INSERT INTO books (title, author, year, genre, rating)
        VALUES (%s, %s, %s, %s, %s)
    """, BOOKS)
    conn.commit()
    print(f"  ✔  Inserted {cursor.rowcount} books.")
    cursor.close()


def query_books_after_2000(conn) -> None:
    print_section("Query 1 · Books published after 2000  (ORDER BY rating DESC)")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT title, author, year, rating
        FROM   books
        WHERE  year > 2000
        ORDER  BY rating DESC
    """)
    print_table(cursor)
    cursor.close()


def query_fiction_high_rating(conn) -> None:
    print_section("Query 2 · Fiction books with rating > 4.0")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT title, author, rating
        FROM   books
        WHERE  genre = 'Fiction'
          AND  rating > 4.0
        ORDER  BY rating DESC
    """)
    print_table(cursor)
    cursor.close()


def query_average_rating(conn) -> None:
    print_section("Query 3 · Average rating across ALL books")
    cursor = conn.cursor()
    cursor.execute("SELECT ROUND(AVG(rating), 2) AS average_rating FROM books")
    row = cursor.fetchone()
    print(f"  Average rating : {row[0]} / 5.0")
    cursor.close()


def query_books_per_genre(conn) -> None:
    print_section("Query 4 · Book count per genre  (GROUP BY)")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT   genre,
                 COUNT(*)              AS total_books,
                 ROUND(AVG(rating), 2) AS avg_rating
        FROM     books
        GROUP BY genre
        ORDER BY total_books DESC
    """)
    print_table(cursor)
    cursor.close()


def create_reviews_and_join(conn) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            id       INT          AUTO_INCREMENT PRIMARY KEY,
            book_id  INT          NOT NULL,
            reviewer VARCHAR(100) NOT NULL,
            stars    INT          CHECK (stars BETWEEN 1 AND 5),
            comment  TEXT,
            FOREIGN KEY (book_id) REFERENCES books(id)
        )
    """)
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM reviews")
    if cursor.fetchone()[0] == 0:
        sample = [
            (1, "Alice",   5, "Changed how I write code."),
            (4, "Bob",     5, "Every page has a gem."),
            (5, "Charlie", 5, "A masterpiece of world-building."),
            (8, "Diana",   5, "Could not put it down."),
        ]
        cursor.executemany("""
            INSERT INTO reviews (book_id, reviewer, stars, comment)
            VALUES (%s, %s, %s, %s)
        """, sample)
        conn.commit()
    print_section("Bonus · JOIN books + reviews")
    cursor.execute("""
        SELECT b.title, r.reviewer, r.stars, r.comment
        FROM   reviews r
        JOIN   books   b ON b.id = r.book_id
        ORDER  BY r.stars DESC
    """)
    print_table(cursor)
    cursor.close()


def main() -> None:
    print("\n" + "═" * 62)
    print("  📚  Task 01 · Library Database")
    print("  Week 3 · Day 4 | UBA Solutions Internship")
    print("═" * 62)

    conn = None
    try:
        ensure_database(DB_NAME)
        conn = get_connection(DB_NAME)
        print(f"\n  ✔  Connected to MySQL  →  database: '{DB_NAME}'")

        print_section("Setup — creating table and inserting data")
        create_table(conn)
        insert_books(conn)

        query_books_after_2000(conn)
        query_fiction_high_rating(conn)
        query_average_rating(conn)
        query_books_per_genre(conn)
        create_reviews_and_join(conn)

    except ConnectionError as ce:
        print(ce)
    except Error as e:
        print(f"\n  ✖  MySQL error: {e}")
    except Exception as e:
        print(f"\n  ✖  Unexpected error: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("\n  ✔  MySQL connection closed.")

    print("\n" + "═" * 62)
    print("  ✅  Task 01 complete")
    print("═" * 62 + "\n")


if __name__ == "__main__":
    main()