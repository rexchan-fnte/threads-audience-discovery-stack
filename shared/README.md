# Shared Modules

共用的爬蟲模組，被多個子服務（jp_car_service, vn_visa_service 等）共享。

## 模組清單

### `threads_client.py`

**ScrapeCreators Threads API 客戶端**

主要類別：
- `ThreadsClient` — API 客戶端，提供搜尋、查詢帳號等功能
- `ThreadsPost` — 爬蟲貼文的數據結構
- `ThreadsProfile` — 使用者個人資料的數據結構

主要方法：
- `search_posts(query, start_date, end_date)` — 搜尋貼文
- `get_post(url)` — 取得單篇貼文詳情
- `get_user_posts(handle)` — 取得用戶所有貼文
- `get_profile(handle)` — 取得用戶個人資料
- `search_users(query)` — 搜尋用戶

工具函數：
- `posts_to_dicts(posts)` — 將 ThreadsPost 物件轉為 CSV 友善的字典列表
- `save_csv(posts, filepath)` — 直接保存為 CSV
- `save_json(data, filepath)` — 直接保存為 JSON

### `keyword_monitor.py`

**關鍵字監控框架**

主要類別：
- `KeywordMonitor` — 管理關鍵字搜尋、去重、累積、導出的業務邏輯

主要方法：
- `search_keyword(keyword, start_date, end_date)` — 單個關鍵字搜尋（自動去重）
- `run_round(keywords, ...)` — 一輪完整的多關鍵字搜尋
- `export_results()` — 導出 CSV + JSON

## 使用方式

### 在子服務中導入

```python
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

from threads_client import ThreadsClient, ThreadsPost
from keyword_monitor import KeywordMonitor
```

或使用代理模組（如 vn_visa_service/threads_client.py）：

```python
from threads_client import ThreadsClient  # 自動指向 shared 版本
```

### 在 Notebook 中使用

```python
import sys
sys.path.insert(0, '../shared')

from threads_client import ThreadsClient

client = ThreadsClient(api_key=os.environ["SCRAPECREATORS_API_KEY"])
posts = client.search_posts("越南簽證")
```

## API Credentials

所有模組都需要環境變數：

```bash
export SCRAPECREATORS_API_KEY="your_api_key"
```

從 [ScrapeCreators](https://app.scrapecreators.com) 取得（免費 100 額度）。

## 修改共用模組的注意事項

1. 修改 `shared/threads_client.py` 或 `shared/keyword_monitor.py` 時，所有導入這些模組的子服務都會受影響
2. 建議修改前與各子服務負責人溝通
3. 測試時確保向下相容性（不要刪除已有的方法簽名）
4. 大型功能變更建議新開分支

## 相關文檔

- [ScrapeCreators API 官方文檔](https://docs.scrapecreators.com/)
- [jp_car_service 爬蟲](../jp_car_service/)
- [vn_visa_service 爬蟲](../vn_visa_service/)
