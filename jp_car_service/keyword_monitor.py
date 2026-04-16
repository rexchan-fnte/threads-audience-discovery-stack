"""
Threads Keyword Monitor
========================
Track keyword trends over time on Threads.
Runs periodic searches and accumulates results into a local dataset.

Usage:
    # One-shot: search and save
    python keyword_monitor.py --keywords "AI,startup,台灣" --output data/

    # Continuous monitoring (every 30 min, 48 rounds = 24 hours)
    python keyword_monitor.py --keywords "AI,startup" --interval 1800 --rounds 48

Environment:
    SCRAPECREATORS_API_KEY=your_key_here
"""

import argparse
import os
import sys
import time
import json
import csv
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# Add parent dir if running as script
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from threads_client import ThreadsClient, ThreadsPost, posts_to_dicts, save_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class KeywordMonitor:
    """
    Monitors keywords on Threads and accumulates unique posts.

    Features:
    - Deduplication by post_id across runs
    - CSV and JSON export
    - Summary statistics per keyword
    """

    def __init__(self, client: ThreadsClient, output_dir: str = "data"):
        self.client = client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Store seen post IDs to avoid duplicates across runs
        self.seen_ids: set[str] = set()
        # Accumulated posts per keyword
        self.all_posts: dict[str, list[ThreadsPost]] = {}

    def search_keyword(
        self,
        keyword: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> tuple[list[ThreadsPost], int]:
        """Search for a keyword and return new (unseen) posts.

        Returns:
            (new_posts, api_total): 新貼文列表 + API 回傳總筆數
        """
        try:
            posts = self.client.search_posts(
                query=keyword,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as e:
            logger.error(f"Error searching '{keyword}': {e}")
            return [], 0

        # Filter out already-seen posts
        new_posts = []
        for p in posts:
            if p.post_id not in self.seen_ids:
                self.seen_ids.add(p.post_id)
                new_posts.append(p)

        # Accumulate
        if keyword not in self.all_posts:
            self.all_posts[keyword] = []
        self.all_posts[keyword].extend(new_posts)

        logger.info(
            f"  '{keyword}': {len(posts)} results, {len(new_posts)} new "
            f"(total unique: {len(self.all_posts[keyword])})"
        )
        return new_posts, len(posts)

    def run_round(
        self,
        keywords: list[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        existing_post_ids: Optional[set[str]] = None,
    ) -> tuple[dict[str, list[ThreadsPost]], dict]:
        """Run one search round for all keywords.

        Args:
            existing_post_ids: 預載的 post_id set（BigQuery 去重用）

        Returns:
            (round_results, stats)
            stats = {"api_total": int, "new_count": int, "dup_ratio": float}
        """
        if existing_post_ids:
            self.seen_ids.update(existing_post_ids)

        round_results = {}
        api_total = 0
        new_total = 0
        logger.info(f"─── Search round: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ───")

        for kw in keywords:
            new_posts, kw_api_total = self.search_keyword(kw, start_date, end_date)
            round_results[kw] = new_posts
            api_total += kw_api_total
            new_total += len(new_posts)
            time.sleep(1)  # Polite delay between keywords

        dup_ratio = 1 - (new_total / api_total) if api_total > 0 else 0
        stats = {"api_total": api_total, "new_count": new_total, "dup_ratio": dup_ratio}
        return round_results, stats

    def run_adaptive(
        self,
        keywords: list[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        existing_post_ids: Optional[set[str]] = None,
        min_rounds: int = 2,
        max_rounds: int = 5,
        dup_threshold: float = 0.7,
    ) -> tuple[dict[str, list[ThreadsPost]], list[dict]]:
        """動態輪數爬蟲：先跑 min_rounds，之後根據 dup_ratio 決定是否繼續。

        Returns:
            (all_posts, round_stats_list)
        """
        round_stats = []

        for r in range(max_rounds):
            _, stats = self.run_round(
                keywords, start_date, end_date,
                existing_post_ids if r == 0 else None,
            )
            round_stats.append(stats)

            logger.info(
                f"Round {r+1}: API={stats['api_total']}, "
                f"new={stats['new_count']}, dup={stats['dup_ratio']:.1%}"
            )

            if r >= min_rounds - 1 and stats['dup_ratio'] >= dup_threshold:
                logger.info(
                    f"Stopping: dup_ratio {stats['dup_ratio']:.1%} >= {dup_threshold:.0%}"
                )
                break

        return self.all_posts, round_stats

    def export_results(self, prefix: str = "threads"):
        """Export all accumulated data to CSV and JSON."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for keyword, posts in self.all_posts.items():
            if not posts:
                continue

            safe_kw = keyword.replace(" ", "_").replace("/", "_")[:30]

            # CSV
            csv_path = self.output_dir / f"{prefix}_{safe_kw}_{timestamp}.csv"
            rows = posts_to_dicts(posts)
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
            logger.info(f"Exported {len(rows)} posts → {csv_path}")

            # JSON
            json_path = self.output_dir / f"{prefix}_{safe_kw}_{timestamp}.json"
            save_json(rows, str(json_path))

    def print_summary(self):
        """Print summary statistics."""
        print("\n" + "=" * 60)
        print("  KEYWORD MONITORING SUMMARY")
        print("=" * 60)
        for kw, posts in self.all_posts.items():
            if not posts:
                print(f"\n  '{kw}': No posts found")
                continue

            total_likes = sum(p.like_count for p in posts)
            total_replies = sum(p.reply_count for p in posts)
            total_reposts = sum(p.repost_count for p in posts)
            verified_count = sum(1 for p in posts if p.is_verified)
            avg_engagement = (total_likes + total_replies + total_reposts) / len(posts) if posts else 0

            print(f"\n  Keyword: '{kw}'")
            print(f"  ├─ Unique posts:      {len(posts)}")
            print(f"  ├─ Total likes:       {total_likes:,}")
            print(f"  ├─ Total replies:     {total_replies:,}")
            print(f"  ├─ Total reposts:     {total_reposts:,}")
            print(f"  ├─ Avg engagement:    {avg_engagement:,.1f}")
            print(f"  ├─ Verified authors:  {verified_count}")

            # Top post by likes
            top = max(posts, key=lambda p: p.like_count)
            print(f"  └─ Top post:          @{top.username} ({top.like_count:,} likes)")
            print(f"     \"{top.text[:80]}...\"")

        print("\n" + "=" * 60)
        balance = self.client.get_credit_balance()
        print(f"  Credits remaining: {balance}")
        print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Threads Keyword Monitor")
    parser.add_argument(
        "--keywords", "-k",
        required=True,
        help="Comma-separated keywords to track (e.g. 'AI,startup,marketing')",
    )
    parser.add_argument(
        "--output", "-o",
        default="data",
        help="Output directory (default: data/)",
    )
    parser.add_argument(
        "--start-date",
        help="Filter start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        help="Filter end date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=0,
        help="Seconds between rounds (0 = single run, default: 0)",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=1,
        help="Number of rounds to run (default: 1)",
    )
    parser.add_argument(
        "--api-key",
        help="ScrapeCreators API key (or set SCRAPECREATORS_API_KEY env var)",
    )

    args = parser.parse_args()
    keywords = [kw.strip() for kw in args.keywords.split(",") if kw.strip()]

    if not keywords:
        parser.error("At least one keyword is required.")

    client = ThreadsClient(api_key=args.api_key)
    monitor = KeywordMonitor(client, output_dir=args.output)

    logger.info(f"Tracking {len(keywords)} keywords: {keywords}")
    logger.info(f"Rounds: {args.rounds}, Interval: {args.interval}s")

    try:
        for i in range(args.rounds):
            if i > 0 and args.interval > 0:
                logger.info(f"Waiting {args.interval}s until next round...")
                time.sleep(args.interval)

            monitor.run_round(
                keywords,
                start_date=args.start_date,
                end_date=args.end_date,
            )

    except KeyboardInterrupt:
        logger.info("\nInterrupted by user. Saving collected data...")

    monitor.export_results()
    monitor.print_summary()


if __name__ == "__main__":
    main()
