"""
ScrapeCreators Threads API Client
==================================
A Python client for the ScrapeCreators unofficial Threads API.
Covers all 5 Threads endpoints:
  - GET /v1/threads/profile       — User profile data
  - GET /v1/threads/user/posts    — User's posts
  - GET /v1/threads/post          — Single post details
  - GET /v1/threads/search        — Keyword search
  - GET /v1/threads/search/users  — User search

Usage:
    from threads_client import ThreadsClient
    client = ThreadsClient(api_key="YOUR_API_KEY")
    results = client.search_posts(query="AI")

API Key: Sign up at https://app.scrapecreators.com (100 free credits)
Docs:    https://docs.scrapecreators.com/
"""

import os
import time
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

try:
    import requests
except ImportError:
    raise ImportError("Please install requests: pip install requests")

logger = logging.getLogger(__name__)

# ─── Configuration ───────────────────────────────────────────────

BASE_URL = "https://api.scrapecreators.com"

ENDPOINTS = {
    "profile":      "/v1/threads/profile",
    "user_posts":   "/v1/threads/user/posts",
    "post":         "/v1/threads/post",
    "search":       "/v1/threads/search",
    "search_users": "/v1/threads/search/users",
    "credit_balance": "/v1/credit-balance",
}


# ─── Data Classes ────────────────────────────────────────────────

@dataclass
class ThreadsPost:
    """Normalized representation of a Threads post."""
    post_id: str
    username: str
    text: str
    like_count: int = 0
    reply_count: int = 0
    repost_count: int = 0
    quote_count: int = 0
    reshare_count: int = 0
    media_type: Optional[int] = None
    permalink: Optional[str] = None
    timestamp: Optional[datetime] = None
    is_verified: bool = False
    is_reply: bool = False
    raw: dict = field(default_factory=dict, repr=False)

    @classmethod
    def from_api_response(cls, data: dict) -> "ThreadsPost":
        """Parse a single post from ScrapeCreators API response."""
        user = data.get("user", {})
        caption = data.get("caption") or {}
        app_info = data.get("text_post_app_info") or {}

        # Extract text from caption or text_fragments
        text = ""
        if caption and caption.get("text"):
            text = caption["text"]
        elif app_info:
            fragments = app_info.get("text_fragments", {}).get("fragments", [])
            text = " ".join(f.get("plaintext", "") for f in fragments if f.get("plaintext"))

        # Parse timestamp
        taken_at = data.get("taken_at")
        timestamp = datetime.fromtimestamp(taken_at, tz=timezone.utc) if taken_at else None

        # Build permalink from code
        code = data.get("code", "")
        username = user.get("username", "")
        permalink = f"https://www.threads.net/@{username}/post/{code}" if code and username else None

        return cls(
            post_id=str(data.get("id", data.get("pk", "")) or ""),
            username=username,
            text=text,
            like_count=data.get("like_count", 0) or 0,
            reply_count=app_info.get("direct_reply_count", 0) or 0,
            repost_count=app_info.get("repost_count", 0) or 0,
            quote_count=app_info.get("quote_count", 0) or 0,
            reshare_count=app_info.get("reshare_count", 0) or 0,
            media_type=data.get("media_type"),
            permalink=permalink,
            timestamp=timestamp,
            is_verified=user.get("is_verified", False),
            is_reply=app_info.get("is_reply", False),
            raw=data,
        )


@dataclass
class ThreadsProfile:
    """Normalized representation of a Threads user profile."""
    username: str
    full_name: str = ""
    bio: str = ""
    follower_count: int = 0
    following_count: int = 0
    is_verified: bool = False
    profile_pic_url: str = ""
    raw: dict = field(default_factory=dict, repr=False)

    @classmethod
    def from_api_response(cls, data: dict) -> "ThreadsProfile":
        """Parse profile from ScrapeCreators API response."""
        return cls(
            username=data.get("username", ""),
            full_name=data.get("full_name", data.get("name", "")),
            bio=data.get("biography", data.get("bio", "")),
            follower_count=data.get("follower_count", 0) or 0,
            following_count=data.get("following_count", 0) or 0,
            is_verified=data.get("is_verified", False),
            profile_pic_url=data.get("profile_pic_url", data.get("hd_profile_pic_url_info", {}).get("url", "")),
            raw=data,
        )


# ─── API Client ──────────────────────────────────────────────────

class ThreadsClient:
    """
    Client for ScrapeCreators Threads API.

    Args:
        api_key: Your ScrapeCreators API key. Falls back to
                 SCRAPECREATORS_API_KEY environment variable.
        timeout: Request timeout in seconds (default: 30).
        max_retries: Number of retries on transient errors (default: 3).
        retry_delay: Base delay between retries in seconds (default: 2).
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ):
        self.api_key = api_key or os.environ.get("SCRAPECREATORS_API_KEY", "")
        if not self.api_key:
            raise ValueError(
                "API key required. Pass api_key= or set SCRAPECREATORS_API_KEY env var.\n"
                "Sign up at https://app.scrapecreators.com for 100 free credits."
            )
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.session.headers.update({
            "x-api-key": self.api_key,
            "Accept": "application/json",
        })

    # ── Core request method ──────────────────────────────────────

    def _request(self, endpoint: str, params: dict) -> dict:
        """Make a GET request with retry logic."""
        # Remove None values from params
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{BASE_URL}{endpoint}"

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug(f"Request [{attempt}/{self.max_retries}]: GET {endpoint} params={params}")
                resp = self.session.get(url, params=params, timeout=self.timeout)

                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("credits_remaining") is not None:
                        logger.info(f"Credits remaining: {data['credits_remaining']}")
                    return data

                if resp.status_code == 400:
                    error = resp.json() if resp.text else {}
                    raise ValueError(f"Bad request (400): {error}")

                if resp.status_code == 401:
                    raise PermissionError("Invalid API key (401). Check your key at app.scrapecreators.com")

                if resp.status_code == 402:
                    raise PermissionError("Insufficient credits (402). Top up at app.scrapecreators.com")

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", self.retry_delay * attempt))
                    logger.warning(f"Rate limited (429), waiting {retry_after}s...")
                    if attempt < self.max_retries:
                        time.sleep(retry_after)
                        continue
                    raise ConnectionError(f"Rate limited (429) after {self.max_retries} retries")

                if resp.status_code >= 500:
                    logger.warning(f"Server error {resp.status_code}, retrying...")
                    if attempt < self.max_retries:
                        time.sleep(self.retry_delay * attempt)
                        continue
                    raise ConnectionError(f"Server error {resp.status_code} after {self.max_retries} retries")

                resp.raise_for_status()

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay * attempt)
                    continue
                raise

            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Connection error on attempt {attempt}: {e}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay * attempt)
                    continue
                raise

        raise ConnectionError(f"Request to {endpoint} failed after {self.max_retries} retries")

    # ── Endpoint methods ─────────────────────────────────────────

    def get_profile(self, handle: str) -> ThreadsProfile:
        """
        Get a Threads user's public profile.

        Args:
            handle: Threads username (without @).

        Returns:
            ThreadsProfile object.

        Example:
            profile = client.get_profile("zuck")
            print(f"{profile.username}: {profile.follower_count} followers")
        """
        data = self._request(ENDPOINTS["profile"], {"handle": handle})
        return ThreadsProfile.from_api_response(data)

    def get_user_posts(self, handle: str) -> list[ThreadsPost]:
        """
        Get all recent public posts from a user.

        Args:
            handle: Threads username (without @).

        Returns:
            List of ThreadsPost objects.

        Example:
            posts = client.get_user_posts("zuck")
            for p in posts:
                print(f"[{p.like_count} likes] {p.text[:80]}")
        """
        data = self._request(ENDPOINTS["user_posts"], {"handle": handle})
        posts_data = data.get("posts", data.get("threads", []))
        return [ThreadsPost.from_api_response(p) for p in posts_data]

    def get_post(self, url: str) -> ThreadsPost:
        """
        Get details for a single Threads post.

        Args:
            url: Full Threads post URL.

        Returns:
            ThreadsPost object.

        Example:
            post = client.get_post("https://www.threads.net/@zuck/post/ABC123")
            print(f"{post.text} — {post.like_count} likes")
        """
        data = self._request(ENDPOINTS["post"], {"url": url})
        post_data = data.get("post", data)
        return ThreadsPost.from_api_response(post_data)

    def search_posts(
        self,
        query: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trim: bool = False,
    ) -> list[ThreadsPost]:
        """
        Search Threads posts by keyword.

        Note: Threads publicly returns ~10 results per request.
        Run multiple times per day for broader coverage.

        Args:
            query:      Keyword(s) to search for.
            start_date: Filter start date (YYYY-MM-DD).
            end_date:   Filter end date (YYYY-MM-DD).
            trim:       If True, return a trimmed response.

        Returns:
            List of ThreadsPost objects.

        Example:
            posts = client.search_posts("AI startup", start_date="2026-03-01")
            for p in posts:
                print(f"@{p.username}: {p.text[:100]}")
        """
        params = {
            "query": query,
            "start_date": start_date,
            "end_date": end_date,
            "trim": str(trim).lower() if trim else None,
        }
        data = self._request(ENDPOINTS["search"], params)
        posts_data = data.get("posts", [])
        return [ThreadsPost.from_api_response(p) for p in posts_data]

    def search_users(self, query: str) -> list[dict]:
        """
        Search for Threads users by name or bio keywords.

        Args:
            query: Search term for usernames/bios.

        Returns:
            List of user dicts with profile info.

        Example:
            users = client.search_users("tech founder")
            for u in users:
                print(u.get("username"), u.get("follower_count"))
        """
        data = self._request(ENDPOINTS["search_users"], {"query": query})
        return data.get("users", [])

    def get_credit_balance(self) -> int:
        """Check remaining API credits."""
        data = self._request(ENDPOINTS["credit_balance"], {})
        return data.get("creditCount", data.get("credits", data.get("credits_remaining", 0)))


# ─── Utilities ───────────────────────────────────────────────────

def posts_to_dicts(posts: list[ThreadsPost]) -> list[dict]:
    """Convert ThreadsPost list to flat dicts for CSV/DataFrame export."""
    return [
        {
            "post_id": p.post_id,
            "username": p.username,
            "is_verified": p.is_verified,
            "text": p.text,
            "like_count": p.like_count,
            "reply_count": p.reply_count,
            "repost_count": p.repost_count,
            "quote_count": p.quote_count,
            "reshare_count": p.reshare_count,
            "is_reply": p.is_reply,
            "permalink": p.permalink,
            "taken_at": p.raw.get("taken_at"),
            "timestamp": p.timestamp.isoformat() if p.timestamp else None,
        }
        for p in posts
    ]


def save_json(data, filepath: str):
    """Save data to JSON file."""
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"Saved {filepath}")


def save_csv(posts: list[ThreadsPost], filepath: str):
    """Save posts to CSV file."""
    import csv
    rows = posts_to_dicts(posts)
    if not rows:
        logger.warning("No posts to save.")
        return
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Saved {len(rows)} posts to {filepath}")
