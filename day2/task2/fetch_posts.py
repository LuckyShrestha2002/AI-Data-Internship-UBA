import requests
import csv

# ─────────────────────────────────────────
# STEP 1: Fetch posts from the API
# ─────────────────────────────────────────
print("=" * 50)
print("Fetching posts from JSONPlaceholder API...")
print("=" * 50)

url = "https://jsonplaceholder.typicode.com/posts"
response = requests.get(url)

if response.status_code == 200:
    print(f"✅ Success! Status Code: {response.status_code}")
    posts = response.json()
    print(f"📦 Total posts fetched: {len(posts)}")
else:
    print(f"❌ Failed! Status Code: {response.status_code}")
    exit()

# ─────────────────────────────────────────
# STEP 2: Save all posts to posts.csv
# ─────────────────────────────────────────
with open("posts.csv", mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=["id", "title", "body"])
    writer.writeheader()
    for post in posts:
        writer.writerow({
            "id": post["id"],
            "title": post["title"],
            "body": post["body"]
        })

print(f"\n✅ Saved {len(posts)} posts to 'posts.csv'")

# ─────────────────────────────────────────
# STEP 3: Read back CSV using DictReader
# ─────────────────────────────────────────
print("\n" + "=" * 50)
print("Reading back and filtering posts...")
print("=" * 50)

filtered_posts = []

with open("posts.csv", mode="r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        word_count = len(row["title"].split())
        if word_count > 5:
            filtered_posts.append(row)

print(f"📋 Posts with title more than 5 words: {len(filtered_posts)}")

# ─────────────────────────────────────────
# STEP 4: Save filtered posts to new CSV
# ─────────────────────────────────────────
with open("filtered_posts.csv", mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=["id", "title", "body"])
    writer.writeheader()
    writer.writerows(filtered_posts)

print(f"✅ Saved {len(filtered_posts)} filtered posts to 'filtered_posts.csv'")
print("\n🎉 Task 2 Complete! Files created:")
print("   → posts.csv")
print("   → filtered_posts.csv")