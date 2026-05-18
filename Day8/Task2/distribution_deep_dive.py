"""
Task 02: Weather Distribution Deep Dive with MySQL Storage Backend
"""
import os
import requests
import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv

load_dotenv()

# Get the directory where THIS script is saved (Day8/Task2/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

API_URL = os.getenv("OPEN_METEO_BASE_URL")
CITIES = {
    "Kathmandu": {"lat": 27.7172, "lon": 85.3240},
    "Pokhara": {"lat": 28.2096, "lon": 83.9856},
    "Lalitpur": {"lat": 27.6744, "lon": 85.3244},
    "Biratnagar": {"lat": 26.4525, "lon": 87.2718},
    "Chitwan": {"lat": 27.5341, "lon": 84.4413}
}

def fetch_and_deep_dive_weather():
    conn = None
    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS weather_metrics")
        cursor.execute("""
            CREATE TABLE weather_metrics (
                city VARCHAR(50), date VARCHAR(20), max_temp DOUBLE, min_temp DOUBLE
            )
        """)
        
        for city, coords in CITIES.items():
            params = {
                "latitude": coords["lat"], "longitude": coords["lon"],
                "daily": ["temperature_2m_max", "temperature_2m_min"], "timezone": "auto"
            }
            response = requests.get(API_URL, params=params, timeout=12)
            response.raise_for_status()
            
            daily = response.json().get("daily", {})
            dates = daily.get("time", [])
            max_t = daily.get("temperature_2m_max", [])
            min_t = daily.get("temperature_2m_min", [])
            
            for i in range(min(7, len(dates))):
                cursor.execute("""
                    INSERT INTO weather_metrics (city, date, max_temp, min_temp)
                    VALUES (%s, %s, %s, %s)
                """, (city, dates[i], float(max_t[i]), float(min_t[i])))
        conn.commit()

        cursor.execute("SELECT city, date, max_temp, min_temp FROM weather_metrics")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        
        # Save plots cleanly into the current folder
        plt.figure(figsize=(6, 4))
        sns.histplot(df['max_temp'], color="purple", bins=8, kde=False, edgecolor='black')
        plt.title("Chart 1: Combined Maximum Temperature Distributions", fontsize=11)
        plt.savefig(os.path.join(SCRIPT_DIR, "01_combined_histogram.png"))
        plt.close()

        plt.figure(figsize=(7, 4))
        sns.boxplot(x='city', y='max_temp', data=df, hue='city', palette="Set3", legend=False)
        plt.title("Chart 2: Max Temperature Distribution Variance Across Cities", fontsize=11)
        plt.savefig(os.path.join(SCRIPT_DIR, "02_city_boxplot.png"))
        plt.close()

        plt.figure(figsize=(7, 4))
        for city in CITIES.keys():
            sns.kdeplot(df[df['city'] == city]['max_temp'], label=city, fill=True, alpha=0.1)
        plt.title("Chart 3: Temperature Density Spread Profile Matrix (KDE)", fontsize=11)
        plt.legend()
        plt.savefig(os.path.join(SCRIPT_DIR, "03_city_kde.png"))
        plt.close()

        plt.figure(figsize=(8, 4))
        sns.lineplot(x='date', y='max_temp', hue='city', data=df, marker="o", linewidth=1.5)
        plt.title("Chart 4: Chronological Temperature Progression Curve Over 7 Days", fontsize=11)
        plt.xticks(rotation=30)
        plt.tight_layout()
        plt.savefig(os.path.join(SCRIPT_DIR, "04_temperature_linechart.png"))
        plt.close()

        print("\n--- Outlier Detection Analysis (Mathematical IQR) ---")
        q1 = df['max_temp'].quantile(0.25)
        q3 = df['max_temp'].quantile(0.75)
        iqr = q3 - q1
        outliers = df[(df['max_temp'] < (q1 - 1.5*iqr)) | (df['max_temp'] > (q3 + 1.5*iqr))]
        print(outliers if not outliers.empty else "No statistical outliers detected.")

        print("\n--- Grouped Aggregations Grid Summary Table Per City ---")
        print(df.groupby('city')['max_temp'].agg(['mean', 'median', 'std', 'min', 'max']))

    except Exception as e:
        print(f"[FATAL SCRIPT FAILURE]: {str(e)}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    fetch_and_deep_dive_weather()