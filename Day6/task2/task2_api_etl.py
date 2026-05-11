import pandas as pd
import requests
import sqlite3

def run_api_etl():
    print("--- Starting Task 02 ETL ---")
    
    # 1. EXTRACT: Fetch posts from API
    url = "https://jsonplaceholder.typicode.com/posts"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        print(f"Extracted {len(df)} posts from API.")
    except Exception as e:
        print(f"Extraction failed: {e}")
        return

    # 2. TRANSFORM
    # Keep only specific columns 
    df = df[['userId', 'id', 'title', 'body']]
    
    # Add word_count column (count words in title) 
    df['word_count'] = df['title'].str.split().str.len()
    
    # Filter: keep only posts where word_count >= 4 
    df = df[df['word_count'] >= 4]
    
    # Standardise: Title Case and strip whitespace 
    df['title'] = df['title'].str.title().str.strip()
    df['body'] = df['body'].str.strip()
    
    print(f"Transformed data. {len(df)} posts remaining after filtering.")

    # 3. LOAD
    # Save to CSV (no index) 
    df.to_csv("clean_posts.csv", index=False)
    
    # Save to SQLite 
    conn = sqlite3.connect("posts.db")
    df.to_sql("posts", conn, if_exists="replace", index=False)
    conn.close()
    
    # 4. Final Stats 
    print("\n--- Task 02 Report ---")
    print(f"Total posts fetched: 100")
    print(f"Posts after filter (word_count >= 4): {len(df)}")
    print("\nTop 3 Users by Post Count:")
    print(df['userId'].value_counts().head(3))
    print("\nDone! Files clean_posts.csv and posts.db created.")

if __name__ == "__main__":
    run_api_etl()