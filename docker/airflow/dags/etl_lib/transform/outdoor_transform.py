"""
Outdoor Data Transform
=======================
清洗 + 多維分類：意圖分類 / 品牌偵測 / 品類偵測 / 噪音過濾。
基於 threads_transform.py 改造，適配戶外裝備市場分析用途。
"""

import logging
import re
from datetime import datetime

import pandas as pd

from etl_lib.configs.outdoor_brands_gear import BRAND_PATTERNS, GEAR_PATTERNS

logger = logging.getLogger(__name__)


def _build_keyword_tier_map(keywords_config: dict) -> dict[str, str]:
    """從 config keywords 群組建立 {keyword: tier_name} 反查 dict。"""
    mapping = {}
    for tier, kw_list in keywords_config.items():
        for kw in kw_list:
            mapping[kw] = tier
    return mapping


def _detect_patterns(text: str, patterns: dict[str, re.Pattern]) -> list[str]:
    """對 text 跑所有 regex pattern，回傳命中的 key 列表。"""
    matched = []
    for name, pat in patterns.items():
        if pat.search(text):
            matched.append(name)
    return matched


def _classify_intent(text: str, intent_patterns: dict[str, str]) -> str:
    """逐一跑 intent_patterns regex，回傳第一個命中的意圖名；無命中回傳「一般討論」。"""
    for intent_name, pattern_str in intent_patterns.items():
        if re.search(pattern_str, text, re.I):
            return intent_name
    return "一般討論"


def _is_noise(text: str, search_keyword: str, noise_filters: dict[str, str]) -> bool:
    """若 search_keyword 在 noise_filters 中有定義，且 text 命中噪音 regex，回傳 True。"""
    pattern_str = noise_filters.get(search_keyword)
    if pattern_str and re.search(pattern_str, text, re.I):
        return True
    return False


def transform_outdoor_data(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    清洗 + 轉換 Threads 戶外裝備爬蟲資料。

    處理步驟：
    1. 時間戳標準化 + post_date
    2. 互動指標 total_engagement
    3. 關鍵字命中 matched_keywords
    4. keyword_tier（config 反查）
    5. intent_type（多標籤 regex，取首個命中）
    6. 品牌偵測 mentioned_brands + brand_count
    7. 品類偵測 mentioned_gear + gear_count
    8. 商家文過濾
    9. 噪音過濾
    10. ingestion_ts

    Args:
        df: extract 階段的 DataFrame
        config: 完整的 outdoor_config.yaml parsed dict

    Returns:
        清洗後的 DataFrame（已排除商家文和噪音）
    """
    if df.empty:
        logger.warning("Empty DataFrame, skipping transform")
        return df

    df = df.copy()

    # ── 從 config 取出各設定 ──
    keywords_config = config.get('keywords', {})
    all_keywords = []
    for kw_list in keywords_config.values():
        all_keywords.extend(kw_list)

    commercial_terms = config.get('commercial_terms', [])
    transform_cfg = config.get('transform', {})
    intent_patterns = transform_cfg.get('intent_patterns', {})
    noise_filters = transform_cfg.get('noise_filters', {})

    keyword_tier_map = _build_keyword_tier_map(keywords_config)

    # ── 1. 時間戳標準化 ──
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df['post_date'] = df['timestamp'].dt.tz_convert('Asia/Taipei').dt.date.astype(str)
    df['text'] = df['text'].fillna('')
    df['text_length'] = df['text'].str.len()

    # ── 2. 互動指標 ──
    engagement_cols = ['like_count', 'reply_count', 'repost_count', 'quote_count', 'reshare_count']
    for col in engagement_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    df['total_engagement'] = df[engagement_cols].sum(axis=1)

    # ── 3. 關鍵字命中 ──
    df['matched_keywords'] = 0
    for kw in all_keywords:
        df['matched_keywords'] += df['text'].str.contains(kw, case=False, regex=False).astype(int)

    # ── 4. keyword_tier ──
    df['keyword_tier'] = df['search_keyword'].map(keyword_tier_map).fillna('unknown')

    # ── 5. intent_type ──
    df['intent_type'] = df['text'].apply(lambda t: _classify_intent(t, intent_patterns))

    # ── 6. 品牌偵測 ──
    brand_results = df['text'].apply(lambda t: _detect_patterns(t, BRAND_PATTERNS))
    df['mentioned_brands'] = brand_results.apply(lambda x: ','.join(x) if x else '')
    df['brand_count'] = brand_results.apply(len)

    # ── 7. 品類偵測 ──
    gear_results = df['text'].apply(lambda t: _detect_patterns(t, GEAR_PATTERNS))
    df['mentioned_gear'] = gear_results.apply(lambda x: ','.join(x) if x else '')
    df['gear_count'] = gear_results.apply(len)

    # ── 8. 商家文過濾 ──
    if commercial_terms:
        commercial_pattern = '|'.join(re.escape(t) for t in commercial_terms)
        df['is_commercial'] = df['text'].str.contains(commercial_pattern, case=False)
    else:
        df['is_commercial'] = False

    # ── 9. 噪音過濾 ──
    df['is_noise'] = df.apply(
        lambda row: _is_noise(row['text'], row.get('search_keyword', ''), noise_filters),
        axis=1,
    )

    before_filter = len(df)
    df_clean = df[~df['is_commercial'] & ~df['is_noise']].copy()

    commercial_count = df['is_commercial'].sum()
    noise_count = df['is_noise'].sum()

    # 統計各意圖分佈
    intent_dist = df_clean['intent_type'].value_counts().to_dict()
    intent_str = ', '.join(f"{k}={v}" for k, v in intent_dist.items())

    logger.info(
        f"Transform: {before_filter} -> {len(df_clean)} posts "
        f"(filtered {commercial_count} commercial, {noise_count} noise). "
        f"Intent: {intent_str}"
    )

    # ── 10. ingestion_ts ──
    df_clean['ingestion_ts'] = datetime.utcnow().isoformat()

    # 移除中間欄位
    df_clean = df_clean.drop(columns=['is_commercial', 'is_noise'], errors='ignore')

    return df_clean
