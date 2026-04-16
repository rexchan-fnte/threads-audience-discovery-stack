# 戶外裝備社群聲量觀測 MVP — 實作計畫（v2）

## Context：這個 MVP 能促成什麼實際優化？

本系統的核心產出不是「看數據」，而是直接回答三個行銷決策問題：

### 1. Google Ads 關鍵字優化

| 系統產出 | 廣告決策 |
|---------|---------|
| `search_keyword` 命中篇數排行 | 社群討論量 ≈ 搜尋需求量，**高命中 = 值得加投的關鍵字** |
| 貼文中 regex 品類偵測的高頻詞 | **長尾關鍵字候選**，如「輕量攻頂包」「Gore-tex 登山鞋」 |
| `keyword_tier=ads_product` 貼文的互動率 | 互動率高 = 購買意圖強，**提高該關鍵字出價** |
| 品牌被提及次數 | **品牌關鍵字出價參考**（被討論多 = 有搜尋量） |

**具體例子**：系統偵測到「快乾毛巾」本週 15 篇討論且互動率高 → Google Ads 加投「快乾毛巾推薦」「登山快乾毛巾」→ 搶佔需求波段。

### 2. 廣告文案 & 素材方向

| 系統產出 | 文案決策 |
|---------|---------|
| `intent_type` 分佈（求推薦 60%, 抱怨 15%…） | 求推薦佔多數 → 文案用「TOP N 推薦」「精選清單」角度 |
| `mentioned_gear` 品類熱度排行 | 登山鞋 > 背包 > 帳篷 → **主力品類優先推廣** |
| 抱怨類貼文的具體痛點詞（regex 偵測） | 「太重」出現 8 次 → 文案主打「輕量」；「磨腳」→ 主打「舒適」 |
| 高互動貼文原文（Sheets 直接看） | 參考**使用者語言**寫文案，而非品牌語言 |

**具體例子**：「抱怨負評」類中「太重」最多 → 廣告文案改為「背包太重？XX 攻頂包只有 380g」→ 直擊痛點。

### 3. 競品聲量 & 受眾畫像

| 系統產出 | 定位決策 |
|---------|---------|
| `mentioned_brands` 品牌排行 | 消費者最常討論誰 → **差異化定位**或**搭順風車** |
| 品牌 × 品類交叉表 | Arc'teryx 在「外套」被提最多 → 你在外套品類避開正面競爭 |
| 高互動帳號列表 | **KOL 合作候選人** |
| `keyword_tier=ads_scenario` 貼文 | 受眾在什麼**情境**下討論裝備 → 廣告受眾定向設定 |

---

## 約束條件

- **資料源**：Threads only
- **LLM**：暫不啟用（純 regex 分析）
- **API 預算**：≤ 30 credits/日
- **排程**：每日 1 次（10:00 Asia/Taipei）

---

## 關鍵字策略（15 個 → ≤ 30 credits/日）

預算：15 keywords × max 2 rounds（自適應）= ≤ 30 credits/日

```yaml
keywords:
  ads_product:          # 直接對應 Google Ads 投放詞
    - "登山鞋推薦"
    - "登山包推薦"
    - "帳篷推薦"
    - "攻頂包"          # 使用者指定
    - "露營"            # 使用者指定
    - "快乾毛巾"        # 使用者指定
    - "防水"            # 使用者指定
    - "登山裝備"

  ads_scenario:         # 受眾發現 + 長尾關鍵字挖掘
    - "新手登山"
    - "百岳"
    - "裝備輕量化"

  ads_brand:            # 競品聲量監測
    - "始祖鳥"
    - "The North Face"
    - "迪卡儂 登山"
    - "Osprey"
```

**為什麼這 15 個**：
- `ads_product`（8 個）：每個都是潛在 Google Ads 投放詞，命中數直接反映搜尋需求
- `ads_scenario`（3 個）：「新手登山」= 入門受眾、「百岳」= 進階玩家、「裝備輕量化」= 裝備控，三種受眾的廣告文案不同
- `ads_brand`（4 個）：台灣戶外市場聲量最大的 4 個品牌，知道消費者怎麼談競品 → 你的廣告怎麼差異化

---

## 架構（無 LLM 版）

```
ScrapeCreators API（15 kw × max 2 rounds = ≤30 credits）
      ↓
  Extract（複用 threads_client + keyword_monitor + fetch_threads_posts）
      ↓
  Transform（新建 outdoor_transform.py）
  ├─ 品牌偵測 regex → mentioned_brands
  ├─ 品類偵測 regex → mentioned_gear
  ├─ 意圖分類 regex → intent_type
  ├─ keyword_tier 分層（ads_product / ads_scenario / ads_brand）
  └─ 商家文 + 噪音過濾
      ↓
  ├→ BigQuery（outdoor_threads_posts 表，長期累積）
  └→ Google Sheets（團隊每日瀏覽，欄位精選）
      ↓
  Email 通知（品牌排行 / 品類熱度 / 意圖分佈 / 高互動貼文）
```

---

## 新建檔案清單（5 個）

| # | 檔案路徑 | 用途 |
|---|---------|------|
| 1 | `docker/airflow/dags/etl_lib/configs/outdoor_config.yaml` | 關鍵字 / 商家詞 / 意圖 regex / 噪音 regex / 爬蟲 / BQ / Sheets 設定 |
| 2 | `docker/airflow/dags/etl_lib/configs/outdoor_brands_gear.py` | 品牌+品類預編譯 regex 常數（transform & report 共用） |
| 3 | `docker/airflow/dags/etl_lib/transform/outdoor_transform.py` | 清洗 + 多維分類（意圖/品牌/品類/噪音） |
| 4 | `docker/airflow/dags/etl_lib/schemas/outdoor_threads_posts_schema.json` | BQ 表格 schema |
| 5 | `docker/airflow/dags/outdoor_social_pipeline.py` | DAG 定義 + Email 報告（內嵌 build function） |

## 複用不修改（5 個）

`threads_client.py`, `keyword_monitor.py`, `fetch_threads_posts.py`, `load_bigquery.py`, `load_gsheets.py`

---

## 細部任務

### Task 1：outdoor_config.yaml

**檔案**：`docker/airflow/dags/etl_lib/configs/outdoor_config.yaml`
**參考**：`etl_lib/configs/threads_config.yaml`

完整內容：

```yaml
keywords:
  ads_product:
    - "登山鞋推薦"
    - "登山包推薦"
    - "帳篷推薦"
    - "攻頂包"
    - "露營"
    - "快乾毛巾"
    - "防水"
    - "登山裝備"
  ads_scenario:
    - "新手登山"
    - "百岳"
    - "裝備輕量化"
  ads_brand:
    - "始祖鳥"
    - "The North Face"
    - "迪卡儂 登山"
    - "Osprey"

commercial_terms:
  - "LINE ID"
  - "歡迎諮詢"
  - "限時優惠"
  - "團購"
  - "代購"
  - "蝦皮"
  - "免運"
  - "下單"
  - "售價"
  - "現貨"
  - "互追"
  - "漲粉"
  - "招募"
  - "接業配"
  - "合作邀約"
  - "批發"

transform:
  intent_patterns:
    求推薦: "推薦|求推|請問.*好用|有沒有人用過|哪裡買"
    開箱分享: "開箱|入手|敗家|心得|評測"
    抱怨負評: "踩雷|不推|難穿|太重|壞掉|退貨|失望|後悔"
    詢問比較: "比較|差在哪|選.*還是|VS|cp值"
    二手轉讓: "出清|二手|免費送|用不到|轉讓"
    揪團找伴: "揪|找山友|一起爬|徵伴"
    路線分享: "路線|行程|GPX|紀錄|遊記"
  noise_filters:
    防水: "(防水工程|抓漏|防水膠|房屋|屋頂|浴室)"
    露營: "(露營車出租|露營區訂位|露營區推薦.*LINE)"

scraper:
  min_rounds: 1
  max_rounds: 2
  dup_threshold: 0.7

bigquery:
  project_id: "dasp04"
  dataset_id: "social_media"
  table_id: "outdoor_threads_posts"

gsheets:
  sheet_id: "<TBD>"
  worksheet_name: "outdoor_raw"
  creds_path: "/opt/airflow/config/gsheets-key.json"
  columns:
    - "post_date"
    - "username"
    - "text"
    - "permalink"
    - "keyword_tier"
    - "search_keyword"
    - "intent_type"
    - "mentioned_brands"
    - "mentioned_gear"
    - "total_engagement"
    - "scrape_date"

notification:
  email_to: "rex.chan@fnte.com.tw"
```

**技術理由**：
- `min_rounds: 1`（從原版的 2 降為 1）：小眾關鍵字如「快乾毛巾」第一輪可能已觸及所有結果，第二輪 dup_ratio 會直接超 0.7 而停止，省 credit
- `noise_filters`：「防水」會抓到防水工程/抓漏；「露營」會抓到純訂位商家文。在 intent_type 分類之前先排除非戶外語境

---

### Task 2：outdoor_brands_gear.py

**檔案**：`docker/airflow/dags/etl_lib/configs/outdoor_brands_gear.py`
**來源**：從 `outdoor_social_media/threads_outdoor_scraper.ipynb` Cell 7b 抽取並擴充

```python
"""品牌 + 品類 regex 常數，供 transform 和 report 共用。"""
import re

BRAND_PATTERNS: dict[str, re.Pattern] = {
    "Arc'teryx":      re.compile(r"arc.?teryx|始祖鳥|鳥牌", re.I),
    "The North Face": re.compile(r"north\s?face|北臉|TNF", re.I),
    "Mammut":         re.compile(r"mammut|長毛象", re.I),
    "mont-bell":      re.compile(r"mont.?bell", re.I),
    "Salomon":        re.compile(r"salomon", re.I),
    "Merrell":        re.compile(r"merrell", re.I),
    "Columbia":       re.compile(r"columbia|哥倫比亞", re.I),
    "Osprey":         re.compile(r"osprey", re.I),
    "Gregory":        re.compile(r"gregory", re.I),
    "Black Diamond":  re.compile(r"black\s?diamond", re.I),
    "Patagonia":      re.compile(r"patagonia", re.I),
    "HOKA":           re.compile(r"hoka", re.I),
    "Decathlon":      re.compile(r"迪卡儂|decathlon", re.I),
    "Snow Peak":      re.compile(r"snow\s?peak", re.I),
    "Sea to Summit":  re.compile(r"sea\s?to\s?summit", re.I),
    "Nemo":           re.compile(r"nemo", re.I),
    "Lowa":           re.compile(r"lowa", re.I),
    "La Sportiva":    re.compile(r"la\s?sportiva", re.I),
    "Hilleberg":      re.compile(r"hilleberg", re.I),
    "MSR":            re.compile(r"\bMSR\b"),
}

GEAR_PATTERNS: dict[str, re.Pattern] = {
    "登山鞋":     re.compile(r"登山鞋|登山靴|健行鞋|防水鞋|gore.?tex.*鞋", re.I),
    "背包":       re.compile(r"背包|登山包|攻頂包", re.I),
    "外套":       re.compile(r"雨衣|風衣|防水外套|衝鋒衣|軟殼|硬殼", re.I),
    "登山杖":     re.compile(r"登山杖|手杖|trekking.?pole", re.I),
    "底層衣":     re.compile(r"排汗衣|底層衣|機能衣|速乾衣|羊毛衣|美麗諾", re.I),
    "帳篷":       re.compile(r"帳篷|天幕|內帳|外帳", re.I),
    "睡袋/睡墊":  re.compile(r"睡袋|睡墊|充氣墊", re.I),
    "頭燈":       re.compile(r"頭燈", re.I),
    "爐具":       re.compile(r"爐頭|攻頂爐|瓦斯罐|炊具", re.I),
    "保溫瓶":     re.compile(r"保溫瓶|保溫壺", re.I),
    "毛巾":       re.compile(r"快乾毛巾|速乾毛巾|運動毛巾", re.I),
    "露營桌椅":   re.compile(r"露營桌|露營椅|折疊桌|折疊椅", re.I),
}
```

**技術理由**：
- 預編譯 `re.Pattern`：transform 對每篇貼文跑全部 pattern，預編譯避免重複 compile
- `MSR` 用 `\b` word boundary：避免誤抓含 MSR 的無關文字
- `底層衣` regex 加「衣」字尾：避免「排汗」「速乾」單獨出現時誤抓非服裝語境
- 新增「毛巾」品類（配合「快乾毛巾」關鍵字）

---

### Task 3：outdoor_transform.py

**檔案**：`docker/airflow/dags/etl_lib/transform/outdoor_transform.py`
**基底**：複製 `etl_lib/transform/threads_transform.py`（98 行）並改造

**改動對照**：

| 步驟 | threads_transform（原版） | outdoor_transform（改造） |
|------|--------------------------|--------------------------|
| 1-2 | timestamp + engagement | 相同，沿用 |
| 3 | `matched_keywords` 布林計數 | 相同，沿用 |
| 4 | `audience_type` = t1/t2（單一 charter_regex） | **`keyword_tier`**（config lookup）+ **`intent_type`**（多標籤 regex） |
| 5 | 商家文過濾 | 相同 + **噪音過濾**（config noise_filters） |
| 新增 | — | **品牌偵測** → `mentioned_brands`, `brand_count` |
| 新增 | — | **品類偵測** → `mentioned_gear`, `gear_count` |

**函式簽名**（接收完整 config dict，取代多個獨立參數）：
```python
def transform_outdoor_data(df: pd.DataFrame, config: dict) -> pd.DataFrame:
```

**處理流程**：
1. 時間戳標準化 + `post_date`（沿用 threads_transform L49-53）
2. `total_engagement`（沿用 L56-59）
3. `matched_keywords`（沿用 L62-64）
4. **`keyword_tier`** — 建立 `{keyword: group_name}` 反查 dict，從 config keywords 群組對應 `search_keyword`
5. **`intent_type`** — 逐筆跑 config `intent_patterns` 各 regex，取第一個命中；無命中 = 「一般討論」
6. **品牌偵測** — 對 text 跑所有 `BRAND_PATTERNS`，命中品牌逗號串接 → `mentioned_brands`；計數 → `brand_count`
7. **品類偵測** — 同上，用 `GEAR_PATTERNS` → `mentioned_gear` + `gear_count`
8. 商家文過濾（沿用 L73-80）
9. **噪音過濾** — 對 `noise_filters` 中有定義的 `search_keyword`，text 命中噪音 regex 則排除
10. `ingestion_ts`（沿用 L92）

**輸出新增欄位**：`keyword_tier`, `intent_type`, `mentioned_brands`, `brand_count`, `mentioned_gear`, `gear_count`

---

### Task 4：outdoor_threads_posts_schema.json

**檔案**：`docker/airflow/dags/etl_lib/schemas/outdoor_threads_posts_schema.json`
**基底**：`etl_lib/schemas/threads_posts_schema.json`

相較原版的差異：
- **移除**：`audience_type`, `sentiment`, `reply_priority`, `suggested_reply`（LLM 欄位）
- **新增**：`keyword_tier` STRING, `intent_type` STRING, `mentioned_brands` STRING, `brand_count` INTEGER, `mentioned_gear` STRING, `gear_count` INTEGER

---

### Task 5：outdoor_social_pipeline.py（DAG + Email Report 內嵌）

**檔案**：`docker/airflow/dags/outdoor_social_pipeline.py`
**基底**：複製 `threads_social_pipeline.py`（444 行）並改造

**DAG 配置**：
```python
dag = DAG(
    'outdoor_social_pipeline',
    schedule='0 10 * * *',          # 每日 10:00 (Asia/Taipei) 一次
    start_date=datetime(2026, 4, 14),
    tags=['outdoor', 'threads', 'market-research'],
)
```

**與原版差異**：

| 項目 | threads_social_pipeline | outdoor_social_pipeline |
|------|------------------------|------------------------|
| Config | `threads_config.yaml` | `outdoor_config.yaml` |
| Schedule | `0 10,15,18 * * *`（3 次/日） | `0 10 * * *`（1 次/日，省 credit） |
| Extract | 排程時段邏輯 | **`manual_mode=True`**（全天抓取，BQ 去重） |
| Transform | `threads_transform` | `outdoor_transform` |
| LLM task | bypass 中 | **不建立** |
| BQ table | `threads_posts` | `outdoor_threads_posts` |

**Task chain**（無 LLM）：
```
extract → transform → [bq_load, sheets_load] → build_email → send_email
```

**Extract 使用 `manual_mode=True` 的理由**：`fetch_threads_posts.py` 硬編碼 `SCHEDULE_HOURS = [10, 15, 18]`。outdoor DAG 排程不同，用 manual_mode 繞過排程時段計算，抓「昨天+今天」全部貼文。BQ `post_id` 去重確保不重複。**不需修改共用 extract 模組。**

**Email 報告內容**（`build_notification_content` 函式內嵌於 DAG）：

| 區塊 | 內容 | 對應行銷決策 |
|------|------|-------------|
| 品牌聲量 Top N | 品牌名 + 提及數 | 競品監測、品牌 Ads 出價 |
| 品類熱度排行 | 品類名 + 提及數 | 主力品類選擇、Ads 品類詞 |
| 意圖分佈 | 求推薦 N / 開箱 N / 抱怨 N … | 文案角度（推薦型 vs 痛點型） |
| 高互動貼文 Top 5 | 原文摘要 + permalink + engagement | 文案靈感、KOL 發現 |
| 關鍵字表現 | 各 keyword 命中數 + 平均互動 | Google Ads 關鍵字調整 |
| 爬蟲效率 | 輪數 / credits / 去重率 | 系統健康度 |

---

## 實作順序

| # | 內容 | 預估 |
|---|------|------|
| 1 | `outdoor_config.yaml` | 20 min |
| 2 | `outdoor_brands_gear.py` | 15 min |
| 3 | `outdoor_transform.py` | 1 hr |
| 4 | `outdoor_threads_posts_schema.json` | 15 min |
| 5 | `outdoor_social_pipeline.py`（含 Email report） | 1.5 hr |
| 6 | 手動建 Google Sheet + 填入 sheet_id | 15 min |
| 7 | Docker build + 觸發 DAG + 端到端驗證 | 45 min |

---

## 成本估算（月）

| 項目 | 成本 |
|------|------|
| ScrapeCreators API | ≤ 30 credits/日 × 30 = 900 credits/月 |
| LLM | $0（暫不啟用） |
| BigQuery | 免費額度內（< 10GB） |
| Airflow | 既有 Docker，無額外成本 |

---

## 驗證方式

1. `docker compose build` → 新檔案打包進映像
2. Airflow UI 確認 `outdoor_social_pipeline` DAG 出現
3. 手動觸發 DAG，逐項檢查：
   - Extract：15 個關鍵字完成，≤ 30 credits
   - Transform：`keyword_tier`, `intent_type`, `mentioned_brands`, `mentioned_gear` 欄位正確填入
   - BQ：`outdoor_threads_posts` 表有資料
   - Sheets：新 Sheet 有正確欄位和資料
   - Email：品牌排行/品類熱度/意圖分佈/高互動貼文呈現正確

---

## 後續擴充路徑（不在本次 MVP）

| 階段 | 功能 | 觸發時機 |
|------|------|---------|
| v1.1 | 啟用 LLM（痛點萃取、品牌暱稱偵測、受眾分群） | regex 不足以捕捉暱稱和隱性痛點時 |
| v1.2 | 週報自動生成（BQ week-over-week 比較） | 累積 2+ 週資料後 |
| v1.3 | 關鍵字擴充（頭燈推薦、睡袋推薦…） | API credit 預算提高時 |
| v1.4 | Streamlit 儀表板（品牌 SOV 趨勢、品類時序圖） | 團隊需要互動式篩選時 |