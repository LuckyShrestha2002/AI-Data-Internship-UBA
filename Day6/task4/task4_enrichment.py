import pandas as pd

def run_task_04():
    print("--- Starting Task 04: Transform & Enrich ---")
    
    # 1. Load clean_students.csv from Task 01 
    try:
        df = pd.read_csv("clean_students.csv")
    except FileNotFoundError:
        print("Error: clean_students.csv not found in this folder.")
        return

    # 2. Add grade column with updated thresholds 
    # A (>=90), B (>=75), C (>=60), D (>=50), F (<50)
    def calculate_grade(score):
        if score >= 90: return 'A'
        elif score >= 75: return 'B'
        elif score >= 60: return 'C'
        elif score >= 50: return 'D'
        else: return 'F'
    
    df['grade'] = df['score'].apply(calculate_grade)

    # 3. Add 'passed' column: True if score >= 50 
    df['passed'] = df['score'] >= 50

    # 4. Add 'score_category' column 
    # High (>=80), Medium (50–79), Low (<50)
    def get_category(score):
        if score >= 80: return 'High'
        elif score >= 50: return 'Medium'
        else: return 'Low'
    
    df['score_category'] = df['score'].apply(get_category)

    # 5. Add 'rank' column (highest score is rank 1) 
    df['rank'] = df['score'].rank(ascending=False, method='min').astype(int)

    # 6. Group by grade and calculate summary stats 
    summary = df.groupby('grade')['score'].agg(['count', 'mean', 'min', 'max'])
    print("\n--- Groupby Summary (Grade Stats) ---")
    print(summary)

    # 7. Sort by rank and reset index 
    df_final = df.sort_values(by='rank').reset_index(drop=True)

    # 8. Save enriched data 
    df_final.to_csv("enriched_students.csv", index=False)
    
    print("\n--- Top 5 Ranked Students ---")
    print(df_final.head(5))
    print("\nFile 'enriched_students.csv' created successfully.")

if __name__ == "__main__":
    run_task_04()