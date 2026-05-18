"""
Task 03: Student Matrix Correlation Analysis over MySQL Data Engines
"""
import os
import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv

load_dotenv()

# Get the directory where THIS script is saved (Day8/Task3/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

def run_student_correlation_suite():
    conn = None
    try:
        np.random.seed(42)
        total_records = 50
        
        study_hours = np.random.uniform(2.0, 12.0, total_records)
        sleep_hours = np.random.uniform(5.0, 9.0, total_records)
        attendance_pct = np.random.uniform(60.0, 100.0, total_records)
        
        variance_noise = np.random.normal(0, 4.0, total_records)
        score = (study_hours * 5.0) + (attendance_pct * 0.35) + (sleep_hours * 0.8) + variance_noise
        score = np.clip(score, 0, 100)
        passed = np.where(score >= 40, "Yes", "No")
        
        df_synthetic = pd.DataFrame({
            "name": [f"Student_ID_{i+1}" for i in range(total_records)],
            "study_hours": np.round(study_hours, 1),
            "sleep_hours": np.round(sleep_hours, 1),
            "attendance_pct": np.round(attendance_pct, 1),
            "score": np.round(score, 1),
            "passed": passed
        })
        
        # Save explicitly inside the Task3 directory
        csv_path = os.path.join(SCRIPT_DIR, "students.csv")
        df_synthetic.to_csv(csv_path, index=False)
        print(f"[SUCCESS] Synthetic data written to: {csv_path}")

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS student_performance")
        cursor.execute("""
            CREATE TABLE student_performance (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(30),
                study_hours DOUBLE,
                sleep_hours DOUBLE,
                attendance_pct DOUBLE,
                score DOUBLE,
                passed VARCHAR(5)
            )
        """)
        
        insert_stmt = """
            INSERT INTO student_performance (name, study_hours, sleep_hours, attendance_pct, score, passed) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        for _, row in df_synthetic.iterrows():
            cursor.execute(insert_stmt, (str(row['name']), float(row['study_hours']), float(row['sleep_hours']), float(row['attendance_pct']), float(row['score']), str(row['passed'])))
        conn.commit()

        cursor.execute("SELECT study_hours, sleep_hours, attendance_pct, score FROM student_performance")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        
        corr_matrix = df.corr()
        print("\n--- Pearson Correlation Matrix Output ---")
        print(corr_matrix)

        plt.figure(figsize=(6, 5))
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt=".2f", linewidths=0.5)
        plt.title("Task 3: Parameter Interactions Matrix Heatmap", fontsize=11)
        plt.tight_layout()
        plt.savefig(os.path.join(SCRIPT_DIR, "heatmap.png"))
        plt.close()

        unstacked = corr_matrix.unstack().sort_values(ascending=False)
        distinct_links = unstacked[unstacked < 1.0]
        print("\n--- Top Strongest Correlation Pairs ---")
        print(distinct_links.head(3))
        print("\n--- Weakest Correlation Pairs ---")
        print(distinct_links.tail(3))

        plt.figure(figsize=(6, 4))
        sns.regplot(data=df, x='study_hours', y='score', scatter_kws={'color':'royalblue'}, line_kws={'color':'crimson'})
        plt.title("Regression Plot 1: Study Hours vs Score Trajectory", fontsize=11)
        plt.savefig(os.path.join(SCRIPT_DIR, "scatter_pair_1.png"))
        plt.close()

        plt.figure(figsize=(6, 4))
        sns.regplot(data=df, x='attendance_pct', y='score', scatter_kws={'color':'forestgreen'}, line_kws={'color':'darkorange'})
        plt.title("Regression Plot 2: Attendance Ratio vs Score", fontsize=11)
        plt.savefig(os.path.join(SCRIPT_DIR, "scatter_pair_2.png"))
        plt.close()

        print("\n[SUCCESS] Task 3 complete.")

    except Exception as e:
        print(f"[FATAL PIPELINE CRASH]: {str(e)}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    run_student_correlation_suite()