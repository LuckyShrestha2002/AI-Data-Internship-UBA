# Day 2 - Task 2: Fetch & Save to CSV

## 📌 Objective
Fetch posts from JSONPlaceholder API, save to CSV, filter posts whose title contains more than 5 words, and save filtered results to a new CSV.

## 🛠️ Tools & Libraries Used
- Python 3
- `requests` — to fetch data from API
- `csv` — to read and write CSV files

## 📁 Files
| File | Description |
|------|-------------|
| `fetch_posts.py` | Main script |
| `posts.csv` | All 100 posts fetched from API |
| `filtered_posts.csv` | Posts with title more than 5 words |
| `Day2-Task2.png` | Terminal screenshot |

## 🚀 How to Run

### Install dependency
```bash
pip3 install requests
```

### Run the script
```bash
python3 fetch_posts.py
```

## 📊 Output
- Fetches 100 posts from `https://jsonplaceholder.typicode.com/posts`
- Saves all posts to `posts.csv` with columns: id, title, body
- Reads back CSV using `DictReader`
- Filters posts where title has more than 5 words
- Saves filtered posts to `filtered_posts.csv`

## ✅ Result
- Total posts fetched: 100
- Posts with title > 5 words: 40