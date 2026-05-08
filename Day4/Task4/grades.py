

import sys
import os
from mysql.connector import Error

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db_connection import get_connection, ensure_database

DB_NAME = "uba_internship"


def print_section(title: str) -> None:
    print(f"\n{'─' * 62}")
    print(f"  {title}")
    print(f"{'─' * 62}")


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
    print(f"\n  → {len(rows)} row(s)")


def assign_grade(score: float) -> str:
    """Return letter grade — A/B/C/D/F based on score."""
    if   score >= 80: return "A"
    elif score >= 65: return "B"
    elif score >= 50: return "C"
    elif score >= 40: return "D"
    else:             return "F"


def create_table(conn) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS students (
            id      INT          AUTO_INCREMENT PRIMARY KEY,
            name    VARCHAR(100) NOT NULL,
            subject VARCHAR(50)  NOT NULL,
            score   DECIMAL(5,1) NOT NULL,
            grade   CHAR(1)      DEFAULT '',
            UNIQUE KEY uq_name_subject (name, subject)
        )
    """)
    conn.commit()
    cursor.close()
    print("  ✔  Table 'students' ready.")


STUDENTS = [
    ("Aarav Sharma",    "Maths",   88),
    ("Priya Thapa",     "Science", 73),
    ("Rohan Karki",     "Maths",   45),
    ("Sneha Rai",       "English", 91),
    ("Bibek Gurung",    "Science", 55),
    ("Anita Shrestha",  "Maths",   62),
    ("Dev Tamang",      "English", 40),
    ("Kavya Magar",     "Science", 78),
    ("Niraj Basnet",    "Maths",   34),
    ("Suman Poudel",    "English", 85),
    ("Riya Adhikari",   "Science", 47),
    ("Anil Bhattarai",  "Maths",   69),
    ("Pooja Limbu",     "English", 52),
    ("Kiran Subedi",    "Science", 95),
    ("Manisha Dhakal",  "Maths",   28),
]


def student_exists(cursor, name: str, subject: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM students WHERE name = %s AND subject = %s",
        (name, subject)
    )
    return cursor.fetchone() is not None


def insert_students(conn) -> None:
    cursor   = conn.cursor()
    inserted = 0
    skipped  = 0
    for name, subject, score in STUDENTS:
        if student_exists(cursor, name, subject):
            skipped += 1
            continue
        cursor.execute("""
            INSERT INTO students (name, subject, score, grade)
            VALUES (%s, %s, %s, %s)
        """, (name, subject, score, assign_grade(score)))
        inserted += 1
    conn.commit()
    cursor.close()
    print(f"  ✔  Inserted: {inserted}  |  Skipped: {skipped}")


def update_grades(conn) -> None:
    cursor = conn.cursor()
    cursor.execute("SELECT id, score FROM students")
    rows = cursor.fetchall()
    for sid, score in rows:
        cursor.execute(
            "UPDATE students SET grade = %s WHERE id = %s",
            (assign_grade(float(score)), sid)
        )
    conn.commit()
    cursor.close()
    print(f"  ✔  Grades updated for {len(rows)} students.")


def delete_failing(conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name, subject, score FROM students WHERE score < 50"
    )
    to_remove = cursor.fetchall()
    if to_remove:
        print("\n  Students being removed (score < 50):")
        for name, subject, score in to_remove:
            print(f"    ✖  {name:<22}  {subject:<10}  {score}")
    cursor.execute("DELETE FROM students WHERE score < 50")
    conn.commit()
    print(f"\n  ✔  Deleted {cursor.rowcount} student(s).")
    cursor.close()


def add_passed_column(conn) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE  table_schema = %s
          AND  table_name   = 'students'
          AND  column_name  = 'passed'
    """, (DB_NAME,))
    exists = cursor.fetchone()[0]
    if not exists:
        cursor.execute(
            "ALTER TABLE students ADD COLUMN passed TINYINT(1) DEFAULT 0"
        )
        conn.commit()
        print("  ✔  Column 'passed' added via ALTER TABLE.")
    else:
        print("  ✔  Column 'passed' already exists — skipping.")
    cursor.execute(
        "UPDATE students SET passed = CASE WHEN score >= 50 THEN 1 ELSE 0 END"
    )
    conn.commit()
    cursor.close()
    print("  ✔  'passed' values set.")


def report_grade_distribution(conn) -> None:
    print_section("Final Report · Student count per grade")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT   grade,
                 COUNT(*)             AS students,
                 ROUND(AVG(score), 1) AS avg_score,
                 MIN(score)           AS lowest,
                 MAX(score)           AS highest
        FROM     students
        GROUP BY grade
        ORDER BY grade ASC
    """)
    print_table(cursor)
    cursor.close()


def report_full_roster(conn) -> None:
    print_section("Full Roster · All remaining students")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, subject, score, grade,
               CASE passed WHEN 1 THEN 'Yes' ELSE 'No' END AS passed
        FROM   students
        ORDER  BY grade ASC, score DESC
    """)
    print_table(cursor)
    cursor.close()


def main() -> None:
    print("\n" + "═" * 62)
    print("  🎓  Task 04 · Grade Management System")
    print("  Week 3 · Day 4 | UBA Solutions Internship")
    print("═" * 62)

    conn = None
    try:
        ensure_database(DB_NAME)
        conn = get_connection(DB_NAME)
        print(f"\n  ✔  Connected  →  '{DB_NAME}'")

        print_section("Step 1 · Create table")
        create_table(conn)
        print_section("Step 2 · Insert 15 students")
        insert_students(conn)
        print_section("Step 3 · UPDATE grades")
        update_grades(conn)
        print_section("Step 4 · DELETE students below 50")
        delete_failing(conn)
        print_section("Step 5 · ALTER TABLE — add passed column")
        add_passed_column(conn)

        report_grade_distribution(conn)
        report_full_roster(conn)

    except ConnectionError as ce:
        print(ce)
    except Error as e:
        print(f"\n  ✖  MySQL error: {e}")
    except Exception as e:
        print(f"\n  ✖  Unexpected: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("\n  ✔  Connection closed.")

    print("\n" + "═" * 62)
    print("  ✅  Task 04 complete")
    print("═" * 62 + "\n")


if __name__ == "__main__":
    main()