import pandas as pd
import requests
import sqlite3
import logging

# Set up logging to track the pipeline progress
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def extract():
    """Step 1: Extract data from a real public API."""
    logging.info("Starting Extraction...")
    url = "https://jsonplaceholder.typicode.com/users"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        logging.info(f"Successfully extracted {len(df)} records.")
        return df
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        return None

def transform(df):
    """Step 2: Clean and Enrich the data."""
    if df is None: return None
    logging.info("Starting Transformation...")

    # A. Cleaning: Flatten address and keep specific columns
    df_flat = pd.json_normalize(df.to_dict('records'))
    columns_to_keep = {
        'id': 'user_id',
        'name': 'full_name',
        'email': 'email',
        'address.city': 'city',
        'company.name': 'company'
    }
    df_clean = df_flat[list(columns_to_keep.keys())].rename(columns=columns_to_keep)

    # B. Handling Nulls/Duplicates
    df_clean = df_clean.drop_duplicates().dropna()

    # C. Enrichment: Add 2 calculated columns
    # 1. Email provider (extracting domain)
    df_clean['email_provider'] = df_clean['email'].str.split('@').str[1]
    # 2. Name Length (complexity metric)
    df_clean['name_length'] = df_clean['full_name'].apply(len)

    # D. Standardization
    df_clean['full_name'] = df_clean['full_name'].str.strip().str.title()
    
    logging.info(f"Transformation complete. Prepared {len(df_clean)} rows.")
    return df_clean

def load(df):
    """Step 3: Load to SQLite and CSV."""
    if df is None: return
    logging.info("Starting Load...")

    # Load to CSV
    df.to_csv("final_report.csv", index=False)

    # Load to SQLite
    conn = sqlite3.connect("intern_capstone.db")
    df.to_sql("cleaned_users", conn, if_exists="replace", index=False)
    conn.close()
    
    logging.info("Load complete. Files 'final_report.csv' and 'intern_capstone.db' are ready.")

def run_pipeline():
    """Main function to run the full ETL cycle."""
    logging.info("--- PIPELINE START ---")
    
    raw_data = extract()
    clean_data = transform(raw_data)
    load(clean_data)
    
    logging.info("--- PIPELINE FINISHED SUCCESSFULLY ---")

if __name__ == "__main__":
    run_pipeline()