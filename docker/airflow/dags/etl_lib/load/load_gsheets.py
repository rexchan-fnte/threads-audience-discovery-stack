"""
Google Sheets 載入模組
======================
透過 gspread + service account 上傳 DataFrame 到 Google Sheets。
寫入策略：append（增量追加），與 notebook Cell 17 一致。
"""

import logging
from typing import Optional

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def upload_to_gsheets(
    df: pd.DataFrame,
    sheet_id: str,
    creds_path: str,
    worksheet_name: str = "Sheet1",
    columns: Optional[list[str]] = None,
) -> dict:
    """
    上傳 DataFrame 到 Google Sheets（增量 append）。

    邏輯與 notebook Cell 17 一致：
    - 工作表為空 → 寫入 header + data
    - 工作表已有資料 → append_rows（不含 header）

    Args:
        df: 要上傳的 DataFrame
        sheet_id: Google Sheets ID
        creds_path: Service account JSON 路徑
        worksheet_name: 工作表名稱（預設 Sheet1）
        columns: 指定上傳的欄位（None = 全部）

    Returns:
        上傳結果資訊
    """
    if df.empty:
        logger.warning("Empty DataFrame, skipping Sheets upload")
        return {"rows_uploaded": 0, "status": "skipped"}

    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)

    # 取得或建立 worksheet
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=len(df) + 1, cols=20)

    # 選擇欄位
    upload_df = df[columns] if columns else df

    # 轉換為字串避免序列化問題
    data = upload_df.astype(str).values.tolist()
    header = upload_df.columns.tolist()

    # Append 模式：空表寫 header+data，有資料則只 append data
    existing = ws.get_all_values()
    if not existing:
        ws.update([header] + data)
    else:
        ws.append_rows(data, value_input_option="USER_ENTERED")

    result = {
        "sheet_id": sheet_id,
        "worksheet": worksheet_name,
        "rows_uploaded": len(data),
        "columns": len(header),
    }
    logger.info(f"Sheets upload: {result}")
    return result
