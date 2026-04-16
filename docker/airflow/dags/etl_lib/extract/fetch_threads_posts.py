"""
Threads Posts Fetcher — Airflow Extract Task
=============================================
封裝爬蟲邏輯：根據排程時段計算 API 日期範圍 + 時間窗口，
搭配 BigQuery 多輪爬蟲+去重策略
"""

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from .threads_client import ThreadsClient, posts_to_dicts
from .keyword_monitor import KeywordMonitor

logger = logging.getLogger(__name__)

TZ_TPE = ZoneInfo("Asia/Taipei")
SCHEDULE_HOURS = [10, 15, 18]  # must match DAG cron: 0 10,15,18 * * *


def get_scrape_params(execution_dt: datetime) -> dict:
    """
    根據排程時段回傳 API 日期範圍 + post-filter 時間窗口。

    Args:
        execution_dt: 排程執行時間（Asia/Taipei 時區）

    Returns:
        {
            "start_date": "YYYY-MM-DD",   # API 參數
            "end_date":   "YYYY-MM-DD",   # API 參數
            "time_window": (start_dt, end_dt),  # post-filter datetime tuple
        }
    """
    now = execution_dt.astimezone(TZ_TPE)
    execution_hour = now.hour
    today_str = now.strftime('%Y-%m-%d')
    yesterday_str = (now - timedelta(days=1)).strftime('%Y-%m-%d')

    today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = now.replace(hour=execution_hour, minute=0, second=0, microsecond=0)

    # 找到最接近的排程時段
    closest_hour = min(SCHEDULE_HOURS, key=lambda h: abs(h - execution_hour))

    if closest_hour == 10:
        # 10:00：搜 d-1~today，只保留今天 00:00~10:00
        return {
            "start_date": yesterday_str,
            "end_date": today_str,
            "time_window": (today_midnight, now.replace(hour=10, minute=0, second=0, microsecond=0)),
        }
    else:
        # 14:00 / 18:00：搜 today，保留前一時段~當前時段
        idx = SCHEDULE_HOURS.index(closest_hour)
        prev_hour = SCHEDULE_HOURS[idx - 1]
        start_dt = now.replace(hour=prev_hour, minute=0, second=0, microsecond=0)
        end_dt = now.replace(hour=closest_hour, minute=0, second=0, microsecond=0)
        return {
            "start_date": today_str,
            "end_date": today_str,
            "time_window": (start_dt, end_dt),
        }


def get_existing_post_ids(bq_client, project_id: str, dataset_id: str,
                          table_id: str, start_date: str) -> set[str]:
    """查 BigQuery 近期已有的 post_id 做增量去重。"""
    full_table = f"{project_id}.{dataset_id}.{table_id}"
    query = f"""
        SELECT DISTINCT post_id
        FROM `{full_table}`
        WHERE post_date >= '{start_date}'
    """
    try:
        rows = bq_client.query(query).result()
        ids = {row.post_id for row in rows}
        logger.info(f"BigQuery 已有 {len(ids)} 個 post_id（since {start_date}）")
        return ids
    except Exception as e:
        logger.warning(f"BigQuery 查詢失敗（表格可能不存在）: {e}")
        return set()


def fetch_threads_posts(
    api_key: str,
    keywords: list[str],
    execution_dt: datetime,
    bq_client=None,
    bq_project_id: str = "",
    bq_dataset_id: str = "",
    bq_table_id: str = "",
    output_dir: str = "/opt/airflow/output",
    min_rounds: int = 2,
    max_rounds: int = 5,
    dup_threshold: float = 0.7,
    manual_mode: bool = False,
) -> tuple[pd.DataFrame, list[dict]]:
    """
    完整的 extract 流程：排程參數計算 → BQ 去重 → 動態輪數爬蟲 → 時間窗口過濾。

    Args:
        api_key: ScrapeCreators API key
        keywords: 關鍵字列表
        execution_dt: 排程執行時間
        bq_client: BigQuery client（若提供則查詢已有 post_id 做去重）
        bq_project_id / bq_dataset_id / bq_table_id: BigQuery 表格設定
        output_dir: 暫存輸出目錄
        min_rounds / max_rounds / dup_threshold: 動態輪數參數
        manual_mode: 手動觸發模式（抓今日全部貼文，不套排程時間窗口）

    Returns:
        (df, round_stats): 清理後的 DataFrame + 每輪統計
    """
    # 1. 計算排程參數
    if manual_mode:
        now = execution_dt.astimezone(TZ_TPE)
        today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        params = {
            "start_date": (now - timedelta(days=1)).strftime('%Y-%m-%d'),
            "end_date": now.strftime('%Y-%m-%d'),
            "time_window": (today_midnight, now),
        }
        logger.info(f"Manual mode: scrape full day up to {now.strftime('%H:%M')}")
    else:
        params = get_scrape_params(execution_dt)
    logger.info(
        f"Scrape params: API {params['start_date']}~{params['end_date']}, "
        f"time_window {params['time_window'][0].isoformat()} ~ {params['time_window'][1].isoformat()}"
    )

    # 2. 查 BQ 已有的 post_id
    existing_ids = set()
    if bq_client and bq_project_id:
        # 查近 2 天的 post_id
        lookup_start = (execution_dt - timedelta(days=2)).strftime('%Y-%m-%d')
        existing_ids = get_existing_post_ids(
            bq_client, bq_project_id, bq_dataset_id, bq_table_id, lookup_start
        )

    # 3. 動態輪數爬蟲
    client = ThreadsClient(api_key=api_key)
    monitor = KeywordMonitor(client, output_dir=output_dir)

    all_posts, round_stats = monitor.run_adaptive(
        keywords=keywords,
        start_date=params['start_date'],
        end_date=params['end_date'],
        existing_post_ids=existing_ids,
        min_rounds=min_rounds,
        max_rounds=max_rounds,
        dup_threshold=dup_threshold,
    )

    # 4. 轉為 DataFrame（含 search_keyword 欄位）
    rows = []
    for kw, posts in all_posts.items():
        for p in posts:
            d = posts_to_dicts([p])[0]
            d['search_keyword'] = kw
            rows.append(d)

    if not rows:
        logger.warning("No posts found in this round")
        return pd.DataFrame(), round_stats

    df = pd.DataFrame(rows).drop_duplicates(subset='post_id', keep='first')
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

    # 5. 時間窗口 post-filter
    start_dt, end_dt = params['time_window']
    # 轉 UTC 比對
    start_utc = start_dt.astimezone(ZoneInfo("UTC"))
    end_utc = end_dt.astimezone(ZoneInfo("UTC"))

    before_filter = len(df)
    df = df[(df['timestamp'] >= start_utc) & (df['timestamp'] < end_utc)].copy()
    logger.info(f"Time window filter: {before_filter} -> {len(df)} posts")

    # 6. 加入 metadata 欄位
    df['scrape_batch_ts'] = execution_dt.isoformat()

    # 計算 credits 消耗
    total_credits = sum(s['api_total'] for s in round_stats)
    logger.info(
        f"Extract complete: {len(df)} posts, "
        f"{len(round_stats)} rounds, {total_credits} API calls"
    )

    return df, round_stats
