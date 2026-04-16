# vn_visa_service — 越南簽證 / 快速通關 Threads 爬蟲

## 爬蟲目標

監控 Threads 上與越南簽證、快速通關相關的貼文。

## KEYWORDS (3 個)

```
越南簽證
越南快速通關
快速通關
```

`PROJECT_TAG = "vietnam_visa"`

## 目錄架構

```
vn_visa_service/
├── threads_post_scraper_visa.ipynb   # 主 notebook（Run All 執行）
├── requirements.txt                   # Python 依賴
├── ads-de-01-757fc521ef01.json        # Google Sheets service account
├── .env                               # SCRAPECREATORS_API_KEY
├── README.md                          # 本檔
└── data/                              # 執行時自動建立，輸出 CSV/JSON
```

**共用模組** 從 `../shared/` 導入：

- `shared/threads_client.py` — ScrapeCreators Threads API 客戶端
- `shared/keyword_monitor.py` — 關鍵字監控框架

## 執行方式

1. 確認根目錄 `../shared/` 存在
2. `.env` 中填入 `SCRAPECREATORS_API_KEY=...`
3. 開啟 `threads_post_scraper_visa.ipynb`，Run All

## 輸出

- **本地**：`data/threads_vietnam_visa_{YYYYMMDD_HHMMSS}.csv`、`.json`
- **Google Sheets**：`SHEET_ID = 1jgZI7nsdHxnTVOReytzCkINYGR-f8SEW3kJzZzOa7ts`

### CSV 欄位順序（關鍵）

| 欄位 | 說明 |
|------|------|
| post_id | Threads 貼文 ID |
| username | 發文者 |
| is_verified | 驗證帳號 |
| text | 貼文內容 |
| like_count ~ reshare_count | 互動數據 |
| is_reply | 是否為回覆貼文 |
| permalink | 貼文連結 |
| **taken_at** | **原始 Unix 時間戳（秒數）** |
| timestamp | UTC 時間 ISO 8601 |

## 與 jp_car_service 的差異

| 項目 | jp_car_service | vn_visa_service |
|------|---------------|-----------------|
| 地點驗證 (LOCATIONS) | 有（沖繩/富士山等） | 移除 |
| audience_type 分類 | 有（t1/t2） | 移除 |
| car_service 判斷 | 有 | 移除 |
| 繁中過濾 | 有 | 保留 |
| 商業文過濾 (is_commercial) | 有 | 保留 |
| taken_at 欄位 | 有（新增，不使用亦可） | 有（必要） |
