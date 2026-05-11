import pandas as pd
import numpy as np

def clean_messy_data():
    # STEP 1: EXTRACT
    # Load the messy CSV [cite: 75]
    try:
        df = pd.read_csv("messy_students.csv")
        print(f"Initial row count: {len(df)}")
    except FileNotFoundError:
        print("Error: messy_students.csv not found.")
        return

    # STEP 2: TRANSFORM (The 6 Problems)
    
    # 1. Handle Missing Values [cite: 86, 111]
    # Drop rows where name is missing
    df.dropna(subset=['name'], inplace=True)
    # Convert score to numeric (handling the 'str' error) and fill NaN with mean [cite: 118]
    df['score'] = pd.to_numeric(df['score'], errors='coerce')
    df['score'] = df['score'].fillna(df['score'].mean())

    # 2. Remove Duplicate Rows [cite: 90, 128]
    df = df.drop_duplicates()

    # 3. Standardize Casing (Title Case) [cite: 138]
    df['name'] = df['name'].str.title()

    # 4. Remove Extra Whitespace [cite: 96, 140]
    df['name'] = df['name'].str.strip()

    # 5. Fix Data Types [cite: 91, 133]
    df['age'] = pd.to_numeric(df['age'], errors='coerce')
    df['score'] = df['score'].astype(float)

    # 6. Filter Invalid/Outlier Values [cite: 98, 145]
    # Keep scores between 0-100 and reasonable ages
    df = df[(df['score'] >= 0) & (df['score'] <= 100)]
    df = df[df['age'] < 120]

    # STEP 3: ENRICH (Add Grade Column) [cite: 211]
    def assign_grade(score):
        if score >= 90: return 'A'
        elif score >= 75: return 'B'
        elif score >= 50: return 'C'
        else: return 'F'

    df['grade'] = df['score'].apply(assign_grade)

    # STEP 4: LOAD
    # Save cleaned data to clean_students.csv [cite: 152, 211]
    df.to_csv("clean_students.csv", index=False)
    
    print(f"Final row count after cleaning: {len(df)}")
    print("\n--- Cleaned Data Preview ---")
    print(df.head())
    print("\n--- Summary Report ---")
    print(df.info())

if __name__ == "__main__":
    clean_messy_data()