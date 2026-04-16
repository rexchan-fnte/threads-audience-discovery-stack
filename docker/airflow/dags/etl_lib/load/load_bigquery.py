import pandas as pd
from google.cloud import bigquery
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)


def upload_to_bigquery(
    df: pd.DataFrame,
    table_id: str,
    project_id: str,
    dataset_id: str,
    write_disposition: str = "WRITE_APPEND" #預設
) -> Dict[str, any]:
    """
    上傳 DataFrame 到 BigQuery
    
    Args:
        df: 要上傳的 DataFrame
        table_id: BigQuery 表格名稱
        project_id: GCP 專案 ID
        dataset_id: BigQuery dataset ID
        write_disposition: 寫入模式
            - WRITE_APPEND: 附加資料（預設）
            - WRITE_TRUNCATE: 覆蓋資料
            - WRITE_EMPTY: 僅在表格為空時寫入
    
    Returns:
        上傳結果資訊
    """
    
    client = bigquery.Client(project=project_id)
    
    # 完整的表格路徑
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    
    # 設定載入設定
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        # 自動偵測 schema（若表格不存在）
        autodetect=True,
    )
    
    logger.info(f"開始上傳到 BigQuery: {full_table_id}")
    logger.info(f"資料筆數: {len(df)}")
    
    # 執行載入
    job = client.load_table_from_dataframe(
        df, full_table_id, job_config=job_config
    )
    
    # 等待完成
    job.result()
    
    # 取得表格資訊
    table = client.get_table(full_table_id)
    
    result = {
        'table_id': full_table_id,
        'rows_uploaded': len(df),
        'total_rows': table.num_rows,
        'job_id': job.job_id,
    }
    
    logger.info(f"上傳成功: {result}")
    
    return result


def batch_upload_to_bigquery(
    data_dict: Dict[str, pd.DataFrame],
    project_id: str,
    dataset_id: str,
    write_disposition: str = "WRITE_APPEND"
) -> Dict[str, Dict]:
    """
    批次上傳多個 DataFrame 到 BigQuery
    
    Args:
        data_dict: {table_name: dataframe} 字典
        project_id: GCP 專案 ID
        dataset_id: BigQuery dataset ID
        write_disposition: 寫入模式
    
    Returns:
        各表格的上傳結果
    """
    results = {}
    
    for table_name, df in data_dict.items():
        try:
            result = upload_to_bigquery(
                df=df,
                table_id=table_name,
                project_id=project_id,
                dataset_id=dataset_id,
                write_disposition=write_disposition
            )
            results[table_name] = result
        except Exception as e:
            logger.error(f"上傳 {table_name} 失敗: {str(e)}")
            results[table_name] = {'error': str(e)}
    
    return results
