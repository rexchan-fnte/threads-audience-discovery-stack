"""測試執行 notebook：跳過 Google Sheets 上傳 cell，結果另存成 _test_run.ipynb 供人工檢視。"""
import nbformat
from nbclient import NotebookClient
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE / "threads_post_scraper_visa.ipynb"
OUT = HERE / "threads_post_scraper_visa_test_run.ipynb"
SKIP_CELL_IDS = {"dcpwe233sxv", "508e6f2b"}  # Sheets 上傳 + 測試 CSV 載入覆蓋

nb = nbformat.read(str(SRC), as_version=4)
nb.cells = [c for c in nb.cells if c.get("id") not in SKIP_CELL_IDS]

print(f"執行 {len(nb.cells)} 個 cell（已跳過 {len(SKIP_CELL_IDS)} 個 Sheets/測試覆蓋 cell）")
client = NotebookClient(nb, timeout=600, kernel_name="python3")
client.execute(cwd=str(HERE))

nbformat.write(nb, str(OUT))
print(f"[OK] 已寫入 {OUT}")
