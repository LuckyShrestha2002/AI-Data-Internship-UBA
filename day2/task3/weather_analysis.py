import requests
import csv

# ─────────────────────────────────────────
# STEP 1: Fetch 7-day weather for Kathmandu
# ─────────────────────────────────────────
print("=" * 50)
print("Fetching 7-day weather forecast for Kathmandu...")
print("=" * 50)

url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 27.7172,
    "longitude": 85.3240,
    "daily": "temperature_2m_max",
    "timezone": "Asia/Kathmandu"
}

response = requests.get(url, params=params)

if response.status_code == 200:
    print(f"✅ Success! Status Code: {response.status_code}")
    data = response.json()
else:
    print(f"❌ Failed! Status Code: {response.status_code}")
    exit()

# ─────────────────────────────────────────
# STEP 2: Extract dates and temperatures
# ─────────────────────────────────────────
dates = data["daily"]["time"]
temps = data["daily"]["temperature_2m_max"]

print(f"\n📅 Forecast for {len(dates)} days:")
for date, temp in zip(dates, temps):
    print(f"   {date} → {temp}°C")

# ─────────────────────────────────────────
# STEP 3: Save to weather.csv
# ─────────────────────────────────────────
with open("weather.csv", mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=["date", "max_temp"])
    writer.writeheader()
    for date, temp in zip(dates, temps):
        writer.writerow({"date": date, "max_temp": temp})

print(f"\n✅ Saved weather data to 'weather.csv'")

# ─────────────────────────────────────────
# STEP 4: Find hottest and coldest day
# ─────────────────────────────────────────
max_temp = max(temps)
min_temp = min(temps)
hottest_day = dates[temps.index(max_temp)]
coldest_day = dates[temps.index(min_temp)]

print("\n" + "=" * 50)
print("🌡️  Weather Analysis:")
print("=" * 50)
print(f"🔥 Hottest Day  : {hottest_day} → {max_temp}°C")
print(f"❄️  Coldest Day  : {coldest_day} → {min_temp}°C")

# ─────────────────────────────────────────
# STEP 5: Save summary to summary.txt
# ─────────────────────────────────────────
with open("summary.txt", mode="w", encoding="utf-8") as file:
    file.write("7-Day Weather Forecast Summary - Kathmandu\n")
    file.write("=" * 45 + "\n")
    for date, temp in zip(dates, temps):
        file.write(f"{date} : {temp}°C\n")
    file.write("=" * 45 + "\n")
    file.write(f"Hottest Day : {hottest_day} → {max_temp}°C\n")
    file.write(f"Coldest Day : {coldest_day} → {min_temp}°C\n")

print(f"\n✅ Summary saved to 'summary.txt'")
print("\n🎉 Task 3 Complete! Files created:")
print("   → weather.csv")
print("   → summary.txt")