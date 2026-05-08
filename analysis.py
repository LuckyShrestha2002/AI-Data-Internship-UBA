import csv
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta

RAW_CSV = "data/headlines_raw.csv"
FILTERED_CSV = "data/headlines_filtered.csv"

# columns for the filtered CSV (headlines with title longer than 6 words)
FILTERED_FIELDNAMES = [
    "country",
    "title",
    "word_count",
    "source_name",
    "published_at",
    "url",
]


def load_csv(filepath):
    # make sure the raw CSV exists before trying to read it
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"{filepath} not found. Run pipeline.py first."
        )
    with open(filepath, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def word_count(title):
    # return 0 for missing or N/A titles
    if not title or title.strip().upper() == "N/A":
        return 0
    return len(title.strip().split())


def parse_datetime(ts):
    # parse ISO-8601 timestamp, return None if it fails
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def divider(title="", width=60):
    if title:
        return f"\n{'=' * width}\n  {title}\n{'=' * width}"
    return "=" * width


def q1_most_headlines_today(rows):
    today = datetime.now(timezone.utc).date()
    today_counts = Counter()
    all_counts = Counter()

    for row in rows:
        all_counts[row["country"]] += 1
        dt = parse_datetime(row["published_at"])
        # only count if the article was published today
        if dt and dt.date() == today:
            today_counts[row["country"]] += 1

    print(divider("Q1 — Country with Most Headlines Today"))

    if today_counts:
        for country, count in today_counts.most_common():
            print(f"  {country:<12}: {count} headline(s)")
        winner, count = today_counts.most_common(1)[0]
        print(f"\n  Result: {winner} with {count} headline(s) today.")
    else:
        # GNews free tier sometimes returns older articles, so fall back to full dataset
        print("  No headlines found for today's date (GNews free tier limitation).")
        print("  Counts across all fetched data:")
        for country, count in all_counts.most_common():
            print(f"  {country:<12}: {count} headline(s)")
        winner, count = all_counts.most_common(1)[0]
        print(f"\n  Result: {winner} with {count} headline(s) in dataset.")


def q2_avg_words_per_country(rows):
    totals = defaultdict(int)
    counts = defaultdict(int)

    for row in rows:
        totals[row["country"]] += word_count(row["title"])
        counts[row["country"]] += 1

    print(divider("Q2 — Average Headline Word Count by Country"))
    for country in sorted(totals):
        avg = totals[country] / counts[country]
        print(f"  {country:<12}: {avg:.2f} words/headline  ({counts[country]} articles)")


def q3_cross_country_duplicates(rows):
    # map each title to the set of countries it appeared in
    title_map = defaultdict(set)
    for row in rows:
        title = row["title"].strip()
        if title and title.upper() != "N/A":
            title_map[title].add(row["country"])

    # keep only titles that appeared in more than one country
    shared = {t: c for t, c in title_map.items() if len(c) > 1}

    print(divider("Q3 — Headlines Shared Across Multiple Countries"))
    if not shared:
        print("  No headlines appeared in more than one country.")
    else:
        for title, countries in shared.items():
            print(f"\n  Title    : {title}")
            print(f"  Countries: {', '.join(sorted(countries))}")
        print(f"\n  Total: {len(shared)} shared headline(s)")


def q4_top_source(rows):
    # count how many headlines each source published across all countries
    counts = Counter(
        row["source_name"] for row in rows
        if row["source_name"].upper() != "N/A"
    )

    print(divider("Q4 — Most Prolific News Source (All Countries)"))
    for source, count in counts.most_common(5):
        print(f"  {source:<40}: {count} headline(s)")

    if counts:
        top, count = counts.most_common(1)[0]
        print(f"\n  Result: '{top}' with {count} headline(s).")


def q5_recency_breakdown(rows):
    # cutoff = 6 hours ago from now
    cutoff = datetime.now(timezone.utc) - timedelta(hours=6)
    recent, older, bad = 0, 0, 0

    for row in rows:
        dt = parse_datetime(row["published_at"])
        if dt is None:
            bad += 1
        elif dt >= cutoff:
            recent += 1
        else:
            older += 1

    total = recent + older
    print(divider("Q5 — Publication Recency Breakdown"))

    if total == 0:
        print("  No valid timestamps found.")
        return

    print(f"  Last 6 hours  : {recent:>4} headline(s)  ({recent/total*100:.1f}%)")
    print(f"  Older than 6h : {older:>4} headline(s)  ({older/total*100:.1f}%)")
    if bad:
        print(f"  Unparseable   : {bad:>4} row(s) skipped")


def q6_duplicate_check(rows):
    urls = [row["url"] for row in rows]
    unique = set(urls)

    print(divider("Q6 — Duplicate Prevention"))
    print(
        "  pipeline.py loads all existing URLs from the CSV before writing.\n"
        "  Any article whose URL already exists in the file gets skipped.\n"
        "  A seen-set also handles duplicates within the same run (e.g. the\n"
        "  same article appearing in multiple country feeds). URL is used as\n"
        "  the unique key since every article has a distinct canonical URL."
    )
    print()
    print(f"  Total rows    : {len(urls)}")
    print(f"  Unique URLs   : {len(unique)}")

    if len(urls) == len(unique):
        print("  Verification  : PASSED — no duplicates in CSV.")
    else:
        print(f"  Verification  : WARNING — {len(urls) - len(unique)} duplicate(s) found.")


def q7_filter_long_titles(rows):
    filtered = []
    for row in rows:
        wc = word_count(row["title"])
        # only keep headlines with more than 6 words in the title
        if wc > 6:
            filtered.append({
                "country": row["country"],
                "title": row["title"],
                "word_count": wc,
                "source_name": row["source_name"],
                "published_at": row["published_at"],
                "url": row["url"],
            })

    os.makedirs("data", exist_ok=True)
    with open(FILTERED_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FILTERED_FIELDNAMES)
        writer.writeheader()
        writer.writerows(filtered)

    print(divider("Q7 — Headlines with Title Longer Than 6 Words"))
    print(f"  Total in dataset   : {len(rows)}")
    print(f"  Passed filter      : {len(filtered)}")
    print(f"  Filtered out       : {len(rows) - len(filtered)}")
    print(f"  Saved to           : {FILTERED_CSV}")


def q8_headline_length_by_country(rows):
    totals = defaultdict(int)
    counts = defaultdict(int)

    for row in rows:
        totals[row["country"]] += word_count(row["title"])
        counts[row["country"]] += 1

    # calculate average headline length per country
    averages = {c: totals[c] / counts[c] for c in totals}

    print(divider("Q8 — Longest and Shortest Average Headline by Country"))
    for country, avg in sorted(averages.items(), key=lambda x: x[1], reverse=True):
        print(f"  {country:<12}: {avg:.2f} words/headline")

    longest = max(averages, key=averages.get)
    shortest = min(averages, key=averages.get)
    print(f"\n  Longest  : {longest} ({averages[longest]:.2f} words)")
    print(f"  Shortest : {shortest} ({averages[shortest]:.2f} words)")


def main():
    # all analysis is done by reading from the CSV, not from the API directly
    print("\nReading from CSV...")
    rows = load_csv(RAW_CSV)
    print(f"Loaded {len(rows)} row(s) from {RAW_CSV}.")

    q1_most_headlines_today(rows)
    q2_avg_words_per_country(rows)
    q3_cross_country_duplicates(rows)
    q4_top_source(rows)
    q5_recency_breakdown(rows)
    q6_duplicate_check(rows)
    q7_filter_long_titles(rows)
    q8_headline_length_by_country(rows)

    print(divider())
    print("  Done.")
    print(divider())


if __name__ == "__main__":
    main()