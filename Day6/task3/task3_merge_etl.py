import pandas as pd
import requests
import sqlite3

def run_task_03():
    print("--- Starting Task 03: Multi-Source Merge ETL ---")
    
    # 1. EXTRACT: Fetch from two sources
    try:
        users_res = requests.get("https://jsonplaceholder.typicode.com/users", timeout=10)
        posts_res = requests.get("https://jsonplaceholder.typicode.com/posts", timeout=10)
        users_res.raise_for_status()
        posts_res.raise_for_status()
        
        # Load into DataFrames
        df_users_raw = pd.DataFrame(users_res.json())
        df_posts = pd.DataFrame(posts_res.json())
        print("Successfully extracted data from both API sources.")
    except Exception as e:
        print(f"Extraction failed: {e}")
        return

    # 2. TRANSFORM: Part A - Flatten and Clean Users
    # Use json_normalize to get 'city' from the nested 'address' column
    df_users_flat = pd.json_normalize(users_res.json())
    
    # Keep specific columns and rename userId-related field to 'id' for merging
    df_users = df_users_flat[['id', 'name', 'email', 'address.city']].copy()
    df_users.rename(columns={'address.city': 'city'}, inplace=True)
    
    # TRANSFORM: Part B - Prepare Posts
    df_posts = df_posts[['userId', 'title']].copy()
    df_posts.rename(columns={'userId': 'id'}, inplace=True)

    # TRANSFORM: Part C - Merge and Enrich
    # Merge the two DataFrames on 'id'
    df_merged = pd.merge(df_users, df_posts, on='id')
    
    # Add post_count per user
    post_counts = df_posts.groupby('id').size().reset_index(name='post_count')
    df_final = pd.merge(df_merged, post_counts, on='id')

    # TRANSFORM: Part D - Final Cleaning
    df_final['email'] = df_final['email'].str.lower()
    df_final['name'] = df_final['name'].str.strip()
    df_final['city'] = df_final['city'].str.strip()
    df_final.dropna(inplace=True)

    # 3. LOAD: Save to CSV and SQLite
    df_final.to_csv("merged_data.csv", index=False)
    
    conn = sqlite3.connect("merged.db")
    df_final.to_sql("merged_users_posts", conn, if_exists="replace", index=False)
    conn.close()

    # 4. REPORT
    print("\n--- Task 03 Final Report ---")
    print(f"Total merged rows: {len(df_final)}")
    print("\nTop 3 Most Active Users:")
    # Get unique users and their post counts for the top 3
    top_users = df_final[['name', 'post_count']].drop_duplicates().sort_values(by='post_count', ascending=False)
    print(top_users.head(3))
    print("\nFiles 'merged_data.csv' and 'merged.db' created.")

if __name__ == "__main__":
    run_task_03()