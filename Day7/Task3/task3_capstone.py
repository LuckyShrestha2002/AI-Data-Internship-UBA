import os
import pandas as pd
import requests
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 2. Load Environment
load_dotenv("../.env")

def get_mysql_engine():
    user, pw, host, db = os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_HOST"), os.getenv("DB_NAME")
    return create_engine(f"mysql+pymysql://{user}:{pw}@{host}/{db}")

def extract():
    logging.info("Starting Extraction phase...")
    try:
        # Fetching Todos as a sample dataset for enrichment
        url = "https://jsonplaceholder.typicode.com/todos"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        logging.info(f"Extracted {len(df)} rows from API.")
        return df
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        return None

def clean(df):
    if df is None: return None
    logging.info(f"Starting Cleaning phase on {len(df)} rows...")
    
    # Cleaning Logic
    df = df.drop_duplicates()
    df['title'] = df['title'].str.strip().str.capitalize()
    
    # Requirement: Handling nulls and types
    df = df.dropna(subset=['title'])
    df['userId'] = df['userId'].astype(int)
    
    logging.info(f"Cleaning complete. {len(df)} rows remaining.")
    return df

def transform(df):
    if df is None: return None
    logging.info("Starting Transformation phase...")

    # Requirement: Engineer 3 new columns
    # 1. Title Length
    df['title_word_count'] = df['title'].apply(lambda x: len(str(x).split()))
    
    # 2. Status Category (Based on 'completed' boolean)
    df['status_label'] = df['completed'].apply(lambda x: 'Finished' if x else 'Pending')
    
    # 3. Task Priority (Mock logic based on ID)
    df['priority_score'] = df['id'].apply(lambda x: 'High' if x % 3 == 0 else 'Normal')

    # Requirement: GroupBy Summary Table
    summary = df.groupby('status_label').agg({
        'title_word_count': ['mean', 'min', 'max'],
        'id': 'count'
    })
    print("\n--- GROUPBY SUMMARY TABLE ---")
    print(summary)
    print("-----------------------------\n")

    return df

def load(df):
    if df is None: return
    logging.info("Starting Load phase...")
    engine = get_mysql_engine()

    # Load to CSV
    csv_file = "capstone_output.csv"
    df.to_csv(csv_file, index=False)
    logging.info(f"Saved data to {csv_file}")

    # Load to MySQL (Idempotent)
    try:
        # Using 'replace' to ensure running twice doesn't duplicate data
        df.to_sql('capstone_tasks', con=engine, if_exists='replace', index=False)
        logging.info("Successfully loaded data to MySQL table: capstone_tasks")
    except Exception as e:
        logging.error(f"MySQL Load failed: {e}")

def run_pipeline():
    logging.info("!!! Pipeline Execution Started !!!")
    
    raw_data = extract()
    cleaned_data = clean(raw_data)
    transformed_data = transform(cleaned_data)
    load(transformed_data)
    
    logging.info("!!! Pipeline Execution Finished Successfully !!!")

if __name__ == "__main__":
    run_pipeline()