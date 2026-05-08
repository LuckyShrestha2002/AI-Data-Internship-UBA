
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import pathlib

_ENV_PATH = pathlib.Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH)


def get_connection(database: str = None):
    """
    Create and return a MySQL connection using credentials from .env.

    Args:
        database: Optional — connect to a specific database.
                  If None, connects to the server only (used for DB creation).

    Returns:
        mysql.connector.connection object

    Raises:
        ConnectionError if the connection fails.
    """
    try:
        config = {
            "host":     os.getenv("DB_HOST",     "localhost"),
            "port":     int(os.getenv("DB_PORT", "3306")),
            "user":     os.getenv("DB_USER",     "root"),
            "password": os.getenv("DB_PASSWORD", ""),
        }

        if database:
            config["database"] = database

        conn = mysql.connector.connect(**config)

        if conn.is_connected():
            return conn

    except Error as e:
        raise ConnectionError(
            f"\n  Could not connect to MySQL.\n"
            f"  Error : {e}\n\n"
            f"  Make sure:\n"
            f"    1. MySQL server is running\n"
            f"    2. Your password in .env is correct\n"
            f"    3. The database '{database}' exists\n"
        )


def ensure_database(db_name: str) -> None:
    """
    Create the database if it does not already exist.
    Connects without specifying a DB first, then runs CREATE DATABASE.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
        f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
    conn.commit()
    cursor.close()
    conn.close()