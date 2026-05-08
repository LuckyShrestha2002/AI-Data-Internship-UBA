import os
import csv
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

# load environment variables from .env file
load_dotenv()

API_KEY = os.getenv("GNEWS_API_KEY")
BASE_URL = "https://gnews.io/api/v4/top-headlines"

# countries we want to fetch headlines for
COUNTRIES = {
    "Nepal": "np",
    "India": "in",
    "USA": "us",
    "UK": "gb",
    "Australia": "au",
}

RAW_CSV = "data/headlines_raw.csv"

# column names for the CSV file
FIELDNAMES = [
    "country",
    "title",
    "description",
    "content",
    "url",
    "source_name",
    "source_url",
    "published_at",
    "fetched_at",
]


def fetch_headlines(country_name, country_code):
    # build request parameters and call the GNews API
    params = {
        "token": API_KEY,
        "lang": "en",
        "country": country_code,
        "max": 10,
    }
    response = requests.get(BASE_URL, params=params, timeout=15)
    response.raise_for_status()

    articles = response.json().get("articles", [])
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows = []
    for article in articles:
        source = article.get("source") or {}
        # use "N/A" for any missing fields so no cell is ever empty
        rows.append({
            "country": country_name,
            "title": article.get("title") or "N/A",
            "description": article.get("description") or "N/A",
            "content": article.get("content") or "N/A",
            "url": article.get("url") or "N/A",
            "source_name": source.get("name") or "N/A",
            "source_url": source.get("url") or "N/A",
            "published_at": article.get("publishedAt") or "N/A",
            "fetched_at": fetched_at,
        })
    return rows


def get_existing_urls(filepath):
    # read all URLs already saved in the CSV to avoid duplicates later
    existing = set()
    if not os.path.exists(filepath):
        return existing
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing.add(row["url"])
    return existing


def save_to_csv(articles, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    existing_urls = get_existing_urls(filepath)
    file_exists = os.path.exists(filepath)

    seen = set()
    new_rows = []

    for article in articles:
        url = article["url"]
        # skip if URL already exists in CSV or was already seen in this run
        if url in existing_urls or url in seen:
            continue
        seen.add(url)
        new_rows.append(article)

    duplicates = len(articles) - len(new_rows)

    if new_rows:
        with open(filepath, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            # write header only if file is being created for the first time
            if not file_exists:
                writer.writeheader()
            writer.writerows(new_rows)

    return len(new_rows), duplicates


def main():
    if not API_KEY:
        raise EnvironmentError("GNEWS_API_KEY not found. Check your .env file.")

    all_articles = []

    # loop through each country and fetch headlines
    for country_name, country_code in COUNTRIES.items():
        print(f"Fetching headlines for {country_name} ({country_code.upper()})...")
        try:
            articles = fetch_headlines(country_name, country_code)
            all_articles.extend(articles)
            print(f"  Retrieved {len(articles)} articles.")
        except requests.HTTPError as e:
            print(f"  HTTP error for {country_name}: {e}")
        except requests.RequestException as e:
            print(f"  Network error for {country_name}: {e}")

    # save all fetched articles to CSV (duplicates are skipped automatically)
    written, skipped = save_to_csv(all_articles, RAW_CSV)

    print("\n--- Pipeline Summary ---")
    print(f"Total articles fetched  : {len(all_articles)}")
    print(f"New rows written to CSV : {written}")
    print(f"Duplicates skipped      : {skipped}")
    print(f"Output file             : {RAW_CSV}")


if __name__ == "__main__":
    main()