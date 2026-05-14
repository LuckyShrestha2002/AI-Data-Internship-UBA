import os
import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load env from parent directory
load_dotenv("../.env")

def get_mysql_engine():
    user, pw, host, db = os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_HOST"), os.getenv("DB_NAME")
    return create_engine(f"mysql+pymysql://{user}:{pw}@{host}/{db}")

def run_task2_audit():
    print("📊 Starting Task 2: Data Quality Audit System...")
    engine = get_mysql_engine()

    # --- 1. FETCH DATA ---
    try:
        res = requests.get("https://jsonplaceholder.typicode.com/posts", timeout=10)
        df = pd.DataFrame(res.json())
        
        # Manually inject some "issues" for the audit to detect (since API data is usually clean)
        df.loc[0, 'title'] = None  # Add a Null
        df.loc[1, 'body'] = None   # Add another Null
        df = pd.concat([df, df.iloc[[5, 10]]], ignore_index=True) # Add 2 Duplicates
    except Exception as e:
        print(f"❌ Error: {e}")
        return

    # --- 2. PRE-CLEANING AUDIT ---
    print("🔎 Auditing data quality...")
    audit_report = {
        "Metric": ["Total Rows", "Null Values", "Duplicate Rows", "Average Word Count"],
        "Before": [
            len(df),
            df.isnull().sum().sum(),
            df.duplicated().sum(),
            df['body'].str.split().str.len().mean() if 'body' in df else 0
        ]
    }

    # --- 3. TRANSFORM & CLEAN ---
    print("🛠 Transforming and Enriching data...")
    # Clean duplicates and nulls
    df_clean = df.drop_duplicates().copy()
    df_clean['title'] = df_clean['title'].fillna("Untitled")
    df_clean['body'] = df_clean['body'].fillna("No content")

    # Enrichments
    df_clean['word_count'] = df_clean['body'].apply(lambda x: len(str(x).split()))
    df_clean['title_upper'] = df_clean['title'].str.upper()
    df_clean['rank'] = df_clean['word_count'].rank(ascending=False, method='min')

    # --- 4. POST-CLEANING AUDIT ---
    audit_report["After"] = [
        len(df_clean),
        df_clean.isnull().sum().sum(),
        df_clean.duplicated().sum(),
        df_clean['word_count'].mean()
    ]

    # --- 5. GENERATE REPORT & LOAD ---
    audit_df = pd.DataFrame(audit_report)
    print("\n--- DATA QUALITY AUDIT REPORT ---")
    print(audit_df.to_string(index=False))
    
    # Save Report to CSV
    audit_df.to_csv("data_quality_audit.csv", index=False)

    # Load Clean Data to MySQL
    try:
        df_clean.to_sql('posts_audited', con=engine, if_exists='replace', index=False)
        print("\n✅ Cleaned data loaded to MySQL table: posts_audited")
    except Exception as e:
        print(f"❌ Database Load Error: {e}")

if __name__ == "__main__":
    run_task2_audit()