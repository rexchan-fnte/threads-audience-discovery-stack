# Threads Scraper — ScrapeCreators API Toolkit

Python scripts for scraping Threads (Meta) data via the [ScrapeCreators](https://scrapecreators.com) unofficial API.  
Built for **keyword research, social listening,  and market research**.

---

## Quick start

```bash
# 1. Install dependency
pip install requests

# 2. Set your API key
export SCRAPECREATORS_API_KEY="your_key_here"
# Sign up at https://app.scrapecreators.com (100 free credits, no credit card)

# 3. Run the demo (costs ~5 credits)
python demo.py
```

---

## Project structure

```
threads_scraper/
├── threads_client.py       # Core API client library (all 5 endpoints)
├── demo.py                 # Quick-start demo — try all endpoints
├── keyword_monitor.py      # Track keywords over time (trend analysis)
├── competitor_analyzer.py  # Compare accounts (competitive intel)
├── requirements.txt        # Dependencies
└── README.md               # This file
```

---

## API endpoints covered

| Endpoint | Path | Credits | Description |
|----------|------|---------|-------------|
| Profile | `/v1/threads/profile` | 1 | Full public profile data |
| User posts | `/v1/threads/user/posts` | 1 | All recent posts from a user |
| Single post | `/v1/threads/post` | 1 | Post details by URL |
| Keyword search | `/v1/threads/search` | 1 | Search posts by keyword (~10 results) |
| User search | `/v1/threads/search/users` | 1 | Find users by name/bio keywords |

---

## Scripts

### 1. `demo.py` — Try all endpoints

```bash
python demo.py
```

Walks through every endpoint with example calls. Good for verifying your API key works.

### 2. `keyword_monitor.py` — Track keywords

**Single run** (one-shot search and export):
```bash
python keyword_monitor.py --keywords "AI,startup,行銷"
```

**Continuous monitoring** (every 30 min, 24 hours):
```bash
python keyword_monitor.py \
  --keywords "AI,startup,行銷" \
  --interval 1800 \
  --rounds 48 \
  --output data/
```

**With date range**:
```bash
python keyword_monitor.py \
  --keywords "product launch" \
  --start-date 2026-03-01 \
  --end-date 2026-03-26
```

Output: CSV + JSON files in `data/` directory with deduplicated posts.

### 3. `competitor_analyzer.py` — Compare accounts

```bash
python competitor_analyzer.py --handles "nike,adidas,puma"
```

Generates:
- **Comparison report** (printed + CSV/JSON) with followers, engagement rates, content patterns
- **Individual post exports** per account
- **Rankings** by followers, engagement rate, avg engagement

---

## Using as a library

```python
from threads_client import ThreadsClient

client = ThreadsClient(api_key="your_key")

# Get profile
profile = client.get_profile("zuck")
print(f"{profile.username}: {profile.follower_count:,} followers")

# Search posts
posts = client.search_posts("AI startup", start_date="2026-03-01")
for p in posts:
    print(f"@{p.username}: {p.text[:100]} ({p.like_count} likes)")

# Get user posts
posts = client.get_user_posts("techcrunch")
for p in posts:
    print(f"{p.timestamp}: {p.text[:80]}")

# Search users
users = client.search_users("marketing agency")
for u in users:
    print(u.get("username"), u.get("follower_count"))

# Export to CSV
from threads_client import save_csv
save_csv(posts, "output.csv")
```

---

## Data fields returned

### Post fields (ThreadsPost)
- `post_id`, `username`, `is_verified`
- `text` — full post content
- `like_count`, `reply_count`, `repost_count`, `quote_count`, `reshare_count`
- `is_reply` — whether it's a reply
- `permalink` — direct link to the post
- `timestamp` — Python datetime
- `raw` — full API response dict

### Profile fields (ThreadsProfile)
- `username`, `full_name`, `bio`
- `follower_count`, `following_count`
- `is_verified`, `profile_pic_url`
- `raw` — full API response dict

---

## Pricing reference

| Plan | Price | Credits |
|------|-------|---------|
| Free | $0 | 100 credits |
| Solo Dev | $10 | 2,500 credits |
| Freelance | $47 | 25,000 credits |
| Business | $497 | 500,000 credits |

Credits never expire. 1 credit = 1 API call for most endpoints.

---

## Tips

- **Keyword search returns ~10 posts per call** (Threads limitation). Run multiple times daily for better coverage.
- **Deduplication is built in** to the keyword monitor — safe to run repeatedly.
- **Combine with official API** for best results: use official Threads API for your own account analytics, ScrapeCreators for competitor data and broad keyword research.
- **Rate limits**: No rate limits from ScrapeCreators side. Add polite delays (1-2s) between calls to be a good citizen.

---

## Legal note

These scripts extract **publicly available data only**. No login credentials are used or required. Review the legal considerations discussed in our earlier analysis before using at scale. This toolkit is provided for educational and research purposes.
