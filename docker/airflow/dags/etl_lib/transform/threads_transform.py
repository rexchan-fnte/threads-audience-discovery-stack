"""
Threads Data Transform
=======================
清洗 + 意圖分類 + 商家文過濾。
轉換自 notebook cell 14 (id=2aac3dc2) 的邏輯。
"""

import logging
from datetime import datetime

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def transform_threads_data(
    df: pd.DataFrame,
    keywords: list[str],
    commercial_terms: list[str],
    charter_regex: str = r"包車|租車|司機|接送",
) -> pd.DataFrame:
    """
    清洗 + 轉換 Threads 爬蟲資料。

    處理步驟：
    1. 時間戳標準化 + post_date 欄位
    2. 互動指標計算 (total_engagement)
    3. 關鍵字命中匹配
    4. 意圖分類 (audience_type: t1/t2)
    5. 商家文過濾 (is_commercial)
    6. 加入 ingestion_ts

    Args:
        df: extract 階段的 DataFrame（需含 timestamp, text, search_keyword 等）
        keywords: KEYWORDS 列表，用於關鍵字命中計算
        commercial_terms: 商家文篩選詞列表
        charter_regex: t1 意圖分類 regex（預設 包車|租車|司機|接送）

    Returns:
        清洗後的 DataFrame（已排除商家文）
    """
    if df.empty:
        logger.warning("Empty DataFrame, skipping transform")
        return df

    df = df.copy()

    # 1. 時間戳標準化（post_date 以台北時間為準）
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df['post_date'] = df['timestamp'].dt.tz_convert('Asia/Taipei').dt.date.astype(str)
    df['text'] = df['text'].fillna('')
    df['text_length'] = df['text'].str.len()

    # 2. 互動指標
    engagement_cols = ['like_count', 'reply_count', 'repost_count', 'quote_count', 'reshare_count']
    for col in engagement_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    df['total_engagement'] = df[engagement_cols].sum(axis=1)

    # 3. 關鍵字命中
    df['matched_keywords'] = 0
    for kw in keywords:
        df['matched_keywords'] += df['text'].str.contains(kw, case=False, regex=False).astype(int)

    # 4. 意圖分類
    df['audience_type'] = np.where(
        df['text'].str.contains(charter_regex, regex=True),
        't1', 't2'
    )

    # 5. 商家文過濾
    if commercial_terms:
        commercial_pattern = '|'.join(commercial_terms)
        df['is_commercial'] = df['text'].str.contains(commercial_pattern, case=False)
    else:
        df['is_commercial'] = False

    before_filter = len(df)
    df_clean = df[~df['is_commercial']].copy()

    t1_count = (df_clean['audience_type'] == 't1').sum()
    t2_count = (df_clean['audience_type'] == 't2').sum()

    logger.info(
        f"Transform: {before_filter} -> {len(df_clean)} posts "
        f"(filtered {df['is_commercial'].sum()} commercial), "
        f"t1={t1_count}, t2={t2_count}"
    )

    # 6. 加入 ingestion timestamp
    df_clean['ingestion_ts'] = datetime.utcnow().isoformat()

    # 移除中間欄位
    df_clean = df_clean.drop(columns=['is_commercial'], errors='ignore')

    return df_clean
