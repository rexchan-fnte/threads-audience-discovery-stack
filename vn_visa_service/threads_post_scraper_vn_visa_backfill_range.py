"""
backfill_range_0401_0410.py — 越南簽證/快速通關關鍵字日期區間逐日回補

用途：
  對 BACKFILL_START ~ BACKFILL_END 區間「每一天」各跑一次 API（MODE=day, FIXED_ROUNDS=1），
  合併所有貼文後套用與 notebook (threads_post_scraper_visa.ipynb) 完全一致的清洗/分類邏輯，
  輸出單一 CSV，並上傳到同一份 Google Sheet (worksheet=vn_visa_service)。

執行：
  cd vn_visa_service
  python backfill_range_0401_0410.py
"""
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

SHARED_DIR = (Path(__file__).parent / ".." / "shared").resolve()
sys.path.insert(0, str(SHARED_DIR))

from threads_client import ThreadsClient, posts_to_dicts
from keyword_monitor import KeywordMonitor

# ─── 設定（與 notebook 對齊）────────────────────
BACKFILL_START = date(2026, 3, 15)
BACKFILL_END   = date(2026, 3, 31)

KEYWORDS = [
    "越南簽證",
    "越南電子簽",
    "越南evisa",
    "越南快速通關",
    "快速通關",
]
PROJECT_TAG = "vietnam_visa_backfill"

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# ─── 初始化 Client ─────────────────────────────
load_dotenv(Path(__file__).parent / ".env", override=True)
api_key = os.environ.get("SCRAPECREATORS_API_KEY")
if not api_key:
    raise RuntimeError("SCRAPECREATORS_API_KEY 未設定，請檢查 .env")

client = ThreadsClient(api_key=api_key, timeout=30, max_retries=3)
monitor = KeywordMonitor(client, output_dir=str(DATA_DIR))

try:
    balance = client.get_credit_balance()
    print(f"[啟動] API 額度剩餘: {balance} credits")
except Exception as e:
    print(f"[啟動] ⚠ 額度查詢失敗: {e}")

total_days = (BACKFILL_END - BACKFILL_START).days + 1
estimated_credits = len(KEYWORDS) * total_days
print(f"[啟動] 範圍 {BACKFILL_START} ~ {BACKFILL_END}，共 {total_days} 天")
print(f"[啟動] 關鍵字 {len(KEYWORDS)} 個，估 credits 消耗 = {estimated_credits}")

# ─── 逐日爬取（MODE=day, FIXED_ROUNDS=1）──────
all_stats = []
day = BACKFILL_START
while day <= BACKFILL_END:
    day_str = day.strftime("%Y-%m-%d")
    print(f"\n=== {day_str} ===")
    _, stats = monitor.run_round(KEYWORDS, start_date=day_str, end_date=day_str)
    stats["scrape_day"] = day_str
    all_stats.append(stats)
    print(
        f"  API回傳={stats['api_total']}, 新增={stats['new_count']}, "
        f"重複率={stats['dup_ratio']:.1%}"
    )
    day += timedelta(days=1)
    time.sleep(2)

# ─── 合併 + 去重（跨日、跨關鍵字）──────────────
rows = []
for kw, posts in monitor.all_posts.items():
    for p in posts:
        d = posts_to_dicts([p])[0]
        d["search_keyword"] = kw
        rows.append(d)

df_out = pd.DataFrame(rows).drop_duplicates(subset="post_id", keep="first")
print(f"\n[彙整] 去重後 {len(df_out)} 篇")

# ─── 轉換層（= notebook cell a018084e）─────────
df = df_out.copy()
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
df["post_date"] = df["timestamp"].dt.date
df["text_length"] = df["text"].fillna("").str.len()

engagement_cols = ["like_count", "reply_count", "repost_count", "quote_count", "reshare_count"]
df["total_engagement"] = df[engagement_cols].sum(axis=1)

df["taken_at_tpe"] = (
    pd.to_datetime(df["taken_at"], unit="s", utc=True)
      .dt.tz_convert("Asia/Taipei")
      .dt.strftime("%Y-%m-%d %H:%M:%S")
)

# ─── 主題分類（= notebook cell 041e40b0）──────
THEME_PARK_TERMS = ["環球影城", "環球", "任天堂", "迪士尼", "樂天世界", "樂園"]

def categorize_post(text: str) -> str:
    t = text or ""
    has_fast_pass = "快速通關" in t
    if has_fast_pass and any(k in t for k in THEME_PARK_TERMS):
        return "主題樂園快速通關"
    return "其他"

df["post_category"] = df["text"].fillna("").apply(categorize_post)

# ─── 意圖分類 audience_type（= notebook cell 2aac3dc2）──
T1_INTENT_PATTERN = r"簽證|電子簽|落地簽|evisa|e-visa|批文|辦簽|辦證"
T1_CONDITIONAL    = r"快速通關"
VIETNAM_CONTEXT   = r"越南|vietnam"

_text = df["text"].fillna("").str
_direct = _text.contains(T1_INTENT_PATTERN, regex=True, case=False)
_conditional = _text.contains(T1_CONDITIONAL, regex=True, case=False) & _text.contains(
    VIETNAM_CONTEXT, regex=True, case=False
)
df["audience_type"] = np.where(_direct | _conditional, "t1", "t2")

# ─── 日期範圍過濾（保留 BACKFILL_START ~ END 內的貼文）──
x_df = df[(df["post_date"] < BACKFILL_START) | (df["post_date"] > BACKFILL_END)].copy()
df = df[(df["post_date"] >= BACKFILL_START) & (df["post_date"] <= BACKFILL_END)].copy()
print(f"[date filter] {BACKFILL_START} ~ {BACKFILL_END}：範圍內 {len(df)}，範圍外 drop {len(x_df)}")

# 關鍵字命中欄
for kw in KEYWORDS:
    df[f"has_{kw}"] = df["text"].fillna("").str.contains(kw, case=False)
df["matched_keywords"] = df[[f"has_{kw}" for kw in KEYWORDS]].sum(axis=1)

# ─── 商案文標記（= notebook cell 21a3a349）────
COMMERCIAL_TERMS = [
    "LINE ID", "LINE", "加入LINE", "加我", "歡迎諮詢", "招募", "互追", "漲粉",
    "立即預訂", "車隊", "限時", "超時費", "客製",
    " 點擊主頁連結", "點擊主頁", "點擊連結",
    "經營", "急單", "加急", "特急", "服務品質", "服務客人", "我的客人", "幫客人", "貴賓", "提前預約", "團體優惠",
    "免費幫忙", "免費協助", "免費諮詢", "為您", "領取折扣", "領取優惠券", "免費領取", "保證過件", "本公司", "我司",
    "留言", "傳給你", "歡迎詢問", "歡迎私訊", "私訊了解", "私訊", "洽詢我們", "聯繫我們", "聯繫我", "洽詢", "請洽", "可以追蹤", "找我們", "交給我們",
]
commercial_pattern = "|".join(COMMERCIAL_TERMS)
COMMERCIAL_REGEXES = [
    r"lin\.ee/[A-Za-z0-9]+",        # LINE 官方帳號短網址
    r"\d{4,}\s?起[/／]人",            # 旅行社團費「19900起/人」
    r"官方LINE[:：@]",                # 「官方LINE:@xxx」
]

def is_commercial(text: str) -> bool:
    t = text or ""
    if re.search(commercial_pattern, t, flags=re.IGNORECASE):
        return True
    return any(re.search(p, t, flags=re.IGNORECASE) for p in COMMERCIAL_REGEXES)

df["is_commercial"] = df["text"].fillna("").apply(is_commercial)
df.loc[df["is_commercial"], "post_category"] = "商案文"

# 商案文不刪，全部保留（與 notebook 一致）
df_clean = df.copy()

# ─── 輸出單一 CSV ─────────────────────────────
csv_path = (
    DATA_DIR
    / f"threads_{PROJECT_TAG}_{BACKFILL_START:%Y%m%d}_{BACKFILL_END:%Y%m%d}.csv"
)
df_clean.to_csv(csv_path, index=False, encoding="utf-8")

print(f"\n[OK] 輸出: {csv_path}")
print(f"[OK] 清洗後總貼文: {len(df_clean)}")
print(f"[OK] 商案文: {df_clean['is_commercial'].sum()} 篇（已標為 post_category='商案文'，未刪除）")
print(f"\n[OK] post_category 分布:")
print(df_clean["post_category"].value_counts().to_string())
print(f"\n[OK] audience_type 分布:")
print(df_clean["audience_type"].value_counts().to_string())
print(f"\n[OK] 逐日 API 回傳:")
for s in all_stats:
    print(f"  {s['scrape_day']}: api={s['api_total']}, new={s['new_count']}, dup={s['dup_ratio']:.1%}")
total_api = sum(s["api_total"] for s in all_stats)
print(f"\n[OK] {total_days} 天 API 回傳加總: {total_api}")
print(f"[OK] 估 credits 消耗: {estimated_credits}（以 Credits remaining 差值為準）")

# ─── 上傳 Google Sheets（= notebook cell dcpwe233sxv + c4043934）──
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
CREDS_PATH = Path(__file__).parent / "ads-de-01-757fc521ef01.json"
SHEET_ID = "1jgZI7nsdHxnTVOReytzCkINYGR-f8SEW3kJzZzOa7ts"
WORKSHEET_GID = 855855256

creds = Credentials.from_service_account_file(str(CREDS_PATH), scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
ws = sh.get_worksheet_by_id(WORKSHEET_GID)
print(f"\n[gsheet] 已連線 worksheet: {ws.title}")

# 篩選欄位（與 notebook 相同）
output_df = df_clean[[
    "username", "taken_at_tpe", "post_date", "text",
    "permalink", "search_keyword", "audience_type", "post_category",
]].copy()
output_df["scrape_date"] = datetime.now().strftime("%Y-%m-%d")

# 安全寫入：明確定位最後一行，不依賴 append_rows 的表格偵測
existing = ws.get_all_values()
next_row = len(existing) + 1

# 空表：先寫標題到 A1
if not existing or not any(str(cell).strip() for cell in existing[0]):
    ws.update(range_name="A1", values=[output_df.columns.tolist()], value_input_option="RAW")
    next_row = 2
    print("[gsheet] 已寫入標題列")

data_rows = output_df.astype(str).values.tolist()
ws.update(range_name=f"A{next_row}", values=data_rows, value_input_option="RAW")

print(f"[gsheet] 已寫入 {len(data_rows)} 筆到 A{next_row}~A{next_row + len(data_rows) - 1}")
print(f"[gsheet] Sheet 目前共 {next_row + len(data_rows) - 1} 行（含標題）")
print(f"[gsheet] https://docs.google.com/spreadsheets/d/{SHEET_ID}")
