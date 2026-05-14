import os
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load env from the parent directory (Day7/.env)
load_dotenv("../.env")

def get_mysql_engine():
    user = os.getenv("DB_USER")
    pw = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    db = os.getenv("DB_NAME")
    
    # Connection to create DB if it doesn't exist
    base_engine = create_engine(f"mysql+pymysql://{user}:{pw}@{host}")
    with base_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db}"))
        conn.commit()
    
    return create_engine(f"mysql+pymysql://{user}:{pw}@{host}/{db}")

def run_task1():
    print("🚀 Starting Task 1 ETL Pipeline...")
    engine = get_mysql_engine()

    # --- 1. EXTRACT (Fault-Tolerant) ---
    try:
        print("📥 Fetching from API (Users & Posts)...")
        # Source 1: Users (Nested JSON)
        u_res = requests.get("https://jsonplaceholder.typicode.com/users", timeout=10)
        u_res.raise_for_status()
        df_users = pd.json_normalize(u_res.json()) # Requirement: pd.json_normalize()

        # Source 2: Posts
        p_res = requests.get("https://jsonplaceholder.typicode.com/posts", timeout=10)
        p_res.raise_for_status()
        df_posts = pd.DataFrame(p_res.json())

        # Source 3: Locally generated messy CSV (Simulated)
        # Note: 'Sincere@april.biz' has a conflict here vs API
        csv_data = {
            'email': ['Sincere@april.biz', 'lucky.shrestha@example.com'],
            'name': ['Leanne Messy Name', 'Lucky Shrestha'],
            'source_type': ['Manual', 'Internship']
        }
        df_csv = pd.DataFrame(csv_data)

    except Exception as e:
        print(f"❌ Extraction Error: {e}")
        return

    # --- 2. MERGE & CONFLICT RESOLUTION ---
    print("🔗 Merging and resolving conflicts...")
    # Merge API Users and Posts
    df_api = pd.merge(df_users, df_posts, left_on='id', right_on='userId', suffixes=('_u', '_p'))
    
    # Merge with CSV on email
    df_final = pd.merge(df_api, df_csv, on='email', how='outer', suffixes=('_api', '_csv'))

    # CONFLICT RESOLUTION: API name wins over CSV name
    df_final['name'] = df_final['name_api'].fillna(df_final['name_csv'])
    # Decision: API data is the verified "Source of Truth."

    # --- 3. CLEANING (6 Techniques) ---
    print("🧹 Cleaning data...")
    # 1. Nulls
    df_final['title'] = df_final['title'].fillna('N/A')
    # 2. Duplicates
    df_final = df_final.drop_duplicates(subset=['id_p', 'email'])
    # 3. Casing
    df_final['name'] = df_final['name'].str.title()
    # 4. Types
    df_final['userId'] = pd.to_numeric(df_final['userId'], errors='coerce').fillna(0).astype(int)
    # 5. Whitespace
    df_final['email'] = df_final['email'].str.strip()
    # 6. Outliers
    df_final = df_final[df_final['title'].str.len() > 2]

    # --- 4. LOAD (Idempotency) ---
    print("💾 Loading to CSV and MySQL...")
    df_final[['userId', 'name', 'email', 'title']].to_csv("final_task1.csv", index=False)
    
    # Idempotency: 'replace' ensures second run doesn't duplicate rows
    df_final[['userId', 'name', 'email', 'title']].to_sql('task1_results', con=engine, if_exists='replace', index=False)
    print("✅ Task 1 Completed successfully!")

if __name__ == "__main__":
    run_task1()