# Day 2 - Task 3: Real API + Weather Analysis

## 📌 Objective
Fetch 7-day weather forecast for Kathmandu using Open-Meteo API,
save to CSV, find hottest and coldest day, and save summary to txt file.

## 🛠️ Tools & Libraries Used
- Python 3
- `requests` — to fetch data from API
- `csv` — to read and write CSV files

## 📁 Files
| File | Description |
|------|-------------|
| `weather_analysis.py` | Main script |
| `weather.csv` | 7-day forecast with date and max temp |
| `summary.txt` | Summary with hottest and coldest day |
| `screenshot.png` | Terminal output screenshot |

## 🚀 How to Run

### Install dependency
```bash
pip3 install requests
```

### Run the script
```bash
python3 weather_analysis.py
```

## 📊 Output
- Fetches 7-day max temperature forecast for Kathmandu
- Latitude: 27.7172, Longitude: 85.3240
- Saves date + max_temp to `weather.csv`
- Finds and prints hottest and coldest day
- Saves full summary to `summary.txt`