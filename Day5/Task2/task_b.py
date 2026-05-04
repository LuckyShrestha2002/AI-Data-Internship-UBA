import mysql.connector
import urllib.request
import json
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

def get_db_connection(create_db=False):
    """Handles DB connection with error handling as requested."""
    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        cursor = conn.cursor()
        if create_db:
            cursor.execute("CREATE DATABASE IF NOT EXISTS monitor_db")
        
        conn.database = "monitor_db"
        return conn, cursor
    except mysql.connector.Error as err:
        print(f"Critical Database Error: {err}")
        return None, None

def setup_tables(cursor):
    """Creates the relational structure for monitoring."""
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INT PRIMARY KEY,
                userId INT,
                title VARCHAR(255),
                body TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS change_log (
                log_id INT AUTO_INCREMENT PRIMARY KEY,
                post_id INT,
                change_type ENUM('NEW', 'MODIFIED'),
                changed_at DATETIME,
                details TEXT
            )
        """)
    except mysql.connector.Error as err:
        print(f"Table Creation Error: {err}")

def fetch_api_data():
    """Fetches data using the built-in urllib library (no pip install needed)."""
    url = "https://jsonplaceholder.typicode.com/posts"
    try:
        # Using urllib.request.urlopen instead of requests.get
        with urllib.request.urlopen(url, timeout=10) as response:
            if response.status == 200:
                data = response.read().decode('utf-8')
                return json.loads(data)
            else:
                print(f"API Error: Received status {response.status}")
                return []
    except Exception as e:
        print(f"Inbuilt Library Fetch Error: {e}")
        return []

def monitor_sync():
    conn, cursor = get_db_connection(create_db=True)
    if not conn: return
    
    setup_tables(cursor)
    api_posts = fetch_api_data()
    current_time = datetime.now()
    
    for post in api_posts:
        cursor.execute("SELECT title, body FROM posts WHERE id = %s", (post['id'],))
        existing_record = cursor.fetchone()
        
        if existing_record is None:
            # First Run: Log as NEW
            cursor.execute(
                "INSERT INTO posts (id, userId, title, body) VALUES (%s, %s, %s, %s)",
                (post['id'], post['userId'], post['title'], post['body'])
            )
            cursor.execute(
                "INSERT INTO change_log (post_id, change_type, changed_at, details) VALUES (%s, %s, %s, %s)",
                (post['id'], 'NEW', current_time, 'Initial fetch from API')
            )
        else:
            # Change Detection: Compare API vs Local DB
            db_title, db_body = existing_record
            if post['title'] != db_title or post['body'] != db_body:
                cursor.execute(
                    "UPDATE posts SET title = %s, body = %s WHERE id = %s",
                    (post['title'], post['body'], post['id'])
                )
                cursor.execute(
                    "INSERT INTO change_log (post_id, change_type, changed_at, details) VALUES (%s, %s, %s, %s)",
                    (post['id'], 'MODIFIED', current_time, f"Update detected. Old title prefix: {db_title[:20]}...")
                )
                print(f"Change detected and logged for Post ID: {post['id']}")

    conn.commit()
    run_reports(cursor)
    cursor.close()
    conn.close()

def run_reports(cursor):
    print("\n--- REPORT 1: Post Count Per User ---")
    cursor.execute("SELECT userId, COUNT(*) FROM posts GROUP BY userId")
    for row in cursor.fetchall():
        print(f"User {row[0]}: {row[1]} posts")

    print("\n--- REPORT 2: Latest Change Log Entries ---")
    cursor.execute("SELECT * FROM change_log ORDER BY changed_at DESC LIMIT 5")
    for row in cursor.fetchall():
        print(f"Post ID: {row[1]} | Status: {row[2]} | Time: {row[3]}")

    print("\n--- REPORT 3: User with Most Change Events ---")
    cursor.execute("""
        SELECT p.userId, COUNT(l.log_id) as event_count
        FROM posts p
        JOIN change_log l ON p.id = l.post_id
        GROUP BY p.userId
        ORDER BY event_count DESC LIMIT 1
    """)
    result = cursor.fetchone()
    if result:
        print(f"User {result[0]} is the most active with {result[1]} events.")

if __name__ == "__main__":
    monitor_sync()