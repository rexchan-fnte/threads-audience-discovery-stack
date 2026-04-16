"""
Threads Social Pipeline DAG
=============================
排程：每日 10:00, 15:00, 18:00 (Asia/Taipei)
流程：Extract → Transform → Load BQ + Sheets → Email

增量策略：
- 10:00 → API d-1~today, filter 00:00~10:00
- 15:00 → API today,    filter 10:00~15:00
- 18:00 → API today,    filter 15:00~18:00
- BigQuery post_id 去重 + 動態輪數爬蟲（dup_ratio 自適應）
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

import sys
import os
sys.path.append('/opt/airflow/dags')


# ── Config loader ────────────────────────────────────────────────

def _load_config():
    import yaml
    config_path = '/opt/airflow/dags/etl_lib/configs/threads_config.yaml'
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def _get_all_keywords(config):
    """從 config 的 keywords 分類中合併所有關鍵字為單一 list。"""
    kw_groups = config.get('keywords', {})
    all_kw = []
    for group_keywords in kw_groups.values():
        all_kw.extend(group_keywords)
    return all_kw


# ── Task 1: Extract ──────────────────────────────────────────────

def extract_threads_posts(**context):
    """抽取 Threads 貼文（動態輪數 + BQ 去重 + 時間窗口過濾）"""
    from etl_lib.extract.fetch_threads_posts import fetch_threads_posts
    from google.cloud import bigquery
    import pandas as pd
    from zoneinfo import ZoneInfo

    config = _load_config()
    keywords = _get_all_keywords(config)
    bq_cfg = config['bigquery']
    scraper_cfg = config['scraper']

    execution_dt = datetime.now(tz=ZoneInfo("Asia/Taipei"))

    # 偵測手動觸發 → 全天模式（不套排程時間窗口）
    dag_run = context.get('dag_run')
    manual_mode = dag_run is not None and dag_run.run_type == 'manual'
    print(f"Extract start: {execution_dt.isoformat()}, {len(keywords)} keywords, manual={manual_mode}")

    # BigQuery client for dedup
    bq_client = bigquery.Client(project=bq_cfg['project_id'])

    api_key = os.environ.get('SCRAPECREATORS_API_KEY', '')

    df, round_stats = fetch_threads_posts(
        api_key=api_key,
        keywords=keywords,
        execution_dt=execution_dt,
        bq_client=bq_client,
        bq_project_id=bq_cfg['project_id'],
        bq_dataset_id=bq_cfg['dataset_id'],
        bq_table_id=bq_cfg['table_id'],
        output_dir='/opt/airflow/output',
        min_rounds=scraper_cfg.get('min_rounds', 2),
        max_rounds=scraper_cfg.get('max_rounds', 5),
        dup_threshold=scraper_cfg.get('dup_threshold', 0.7),
        manual_mode=manual_mode,
    )

    # 若無任何貼文 → skip 所有下游 tasks（避免空 CSV 炸掉 transform）
    if df.empty:
        from airflow.exceptions import AirflowSkipException
        print("Extract got 0 posts — skipping downstream tasks")
        raise AirflowSkipException("No posts extracted (API credits exhausted or no matching posts)")

    batch_date = context['ds']
    temp_path = f'/opt/airflow/output/threads_raw_{batch_date}_{execution_dt.strftime("%H%M")}.csv'
    df.to_csv(temp_path, index=False, encoding='utf-8-sig')

    # 推送統計到 XCom
    total_credits = sum(s.get('api_total', 0) for s in round_stats)
    dup_threshold = scraper_cfg.get('dup_threshold', 0.7)
    stop_reason = (
        'dup_threshold' if round_stats and round_stats[-1].get('dup_ratio', 0) >= dup_threshold
        else 'max_rounds'
    )
    keyword_counts = df.groupby('search_keyword').size().to_dict() if not df.empty else {}

    context['task_instance'].xcom_push(key='extract_stats', value={
        'raw_count': len(df),
        'rounds': len(round_stats),
        'total_credits': total_credits,
        'stop_reason': stop_reason,
        'round_details': round_stats,
        'keyword_counts': keyword_counts,
    })

    print(f"Extract done: {len(df)} posts, {len(round_stats)} rounds, {total_credits} credits")
    return temp_path


# ── Task 2: Transform ───────────────────────────────────────────

def transform_threads_data(**context):
    """清洗 + 意圖分類 + 商家文過濾"""
    from etl_lib.transform.threads_transform import transform_threads_data as do_transform
    import pandas as pd

    config = _load_config()
    keywords = _get_all_keywords(config)
    commercial_terms = config.get('commercial_terms', [])
    charter_regex = config.get('transform', {}).get('charter_regex', r'包車|租車|司機|接送')

    ti = context['task_instance']
    raw_path = ti.xcom_pull(task_ids='extract_threads_posts')

    if not raw_path or not os.path.exists(raw_path):
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException("No raw CSV from extract task")

    df = pd.read_csv(raw_path, encoding='utf-8-sig')
    if df.empty:
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException("Raw CSV is empty — nothing to transform")
    print(f"Transform input: {len(df)} posts")

    df_clean = do_transform(
        df=df,
        keywords=keywords,
        commercial_terms=commercial_terms,
        charter_regex=charter_regex,
    )

    batch_date = context['ds']
    output_path = f'/opt/airflow/output/threads_transformed_{batch_date}.csv'
    df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')

    t1_count = int((df_clean['audience_type'] == 't1').sum()) if not df_clean.empty else 0
    t2_count = int((df_clean['audience_type'] == 't2').sum()) if not df_clean.empty else 0

    ti.xcom_push(key='transform_stats', value={
        'input_count': len(df),
        'output_count': len(df_clean),
        'filtered': len(df) - len(df_clean),
        't1_count': t1_count,
        't2_count': t2_count,
    })

    print(f"Transform done: {len(df)} -> {len(df_clean)} posts")
    return output_path


# ── Task 3: LLM Reply Generation（暫時 bypass，保留供後續啟用）──

def generate_llm_replies(**context):
    """Claude Sonnet 生成情緒標籤 + 建議回覆"""
    from etl_lib.analyze.llm_reply_generator import enrich_df_with_replies
    import pandas as pd

    config = _load_config()
    llm_cfg = config.get('llm', {})

    ti = context['task_instance']
    transformed_path = ti.xcom_pull(task_ids='transform_threads_data')

    df = pd.read_csv(transformed_path, encoding='utf-8-sig')
    print(f"LLM input: {len(df)} posts")

    if df.empty:
        print("No posts to process, skipping LLM")
        output_path = transformed_path
    else:
        anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '')
        df = enrich_df_with_replies(
            df=df,
            anthropic_api_key=anthropic_key,
            model=llm_cfg.get('model', 'claude-sonnet-4-20250514'),
            batch_size=llm_cfg.get('batch_size', 8),
        )

        batch_date = context['ds']
        output_path = f'/opt/airflow/output/threads_enriched_{batch_date}.csv'
        df.to_csv(output_path, index=False, encoding='utf-8-sig')

    ti.xcom_push(key='llm_stats', value={
        'total_posts': len(df),
        'with_replies': int(df['suggested_reply'].notna().sum()) if not df.empty else 0,
    })

    print(f"LLM done: {len(df)} posts enriched")
    return output_path


# ── Task 4: Load to BigQuery ─────────────────────────────────────

def load_to_bigquery(**context):
    """上傳到 BigQuery（WRITE_APPEND 增量模式）"""
    from etl_lib.load.load_bigquery import upload_to_bigquery
    import pandas as pd

    config = _load_config()
    bq_cfg = config['bigquery']

    ti = context['task_instance']
    enriched_path = ti.xcom_pull(task_ids='transform_threads_data')  # LLM bypass 中，接 transform

    df = pd.read_csv(enriched_path, encoding='utf-8-sig')
    print(f"BQ upload: {len(df)} posts -> {bq_cfg['project_id']}.{bq_cfg['dataset_id']}.{bq_cfg['table_id']}")

    if df.empty:
        print("No data to upload, skipping BQ")
        return {'rows_uploaded': 0}

    result = upload_to_bigquery(
        df=df,
        table_id=bq_cfg['table_id'],
        project_id=bq_cfg['project_id'],
        dataset_id=bq_cfg['dataset_id'],
        write_disposition="WRITE_APPEND",
    )

    print(f"BQ upload done: {result}")
    return result


# ── Task 5: Upload to Google Sheets ──────────────────────────────

def upload_to_gsheets(**context):
    """上傳到 Google Sheets（增量 append）"""
    from etl_lib.load.load_gsheets import upload_to_gsheets as do_upload
    import pandas as pd

    config = _load_config()
    gs_cfg = config['gsheets']

    ti = context['task_instance']
    enriched_path = ti.xcom_pull(task_ids='transform_threads_data')  # LLM bypass 中，接 transform

    df = pd.read_csv(enriched_path, encoding='utf-8-sig')
    print(f"Sheets upload: {len(df)} posts")

    if df.empty:
        print("No data to upload, skipping Sheets")
        return {'rows_uploaded': 0}

    # 加入 scrape_date（與 notebook Cell 17 一致）
    df['scrape_date'] = datetime.now().strftime('%Y-%m-%d')

    result = do_upload(
        df=df,
        sheet_id=gs_cfg['sheet_id'],
        creds_path=gs_cfg['creds_path'],
        worksheet_name=gs_cfg.get('worksheet_name', 'raw_posts'),
        columns=gs_cfg.get('columns'),
    )

    print(f"Sheets upload done: {result}")
    return result


# ── Task 6: Build Email Content ──────────────────────────────────

def build_notification_content(**context):
    """組合通知 Email 內容（含爬蟲效率統計報告）"""
    ti = context['task_instance']
    extract_stats = ti.xcom_pull(task_ids='extract_threads_posts', key='extract_stats') or {}
    transform_stats = ti.xcom_pull(task_ids='transform_threads_data', key='transform_stats') or {}

    batch_date = context['ds']
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')

    # ── Extract 數據 ──
    raw_count = extract_stats.get('raw_count', 0)
    rounds = extract_stats.get('rounds', 0)
    total_credits = extract_stats.get('total_credits', 0)
    stop_reason = extract_stats.get('stop_reason', '-')
    round_details = extract_stats.get('round_details', [])
    keyword_counts = extract_stats.get('keyword_counts', {})

    credit_per_post = f"{total_credits / raw_count:.1f}" if raw_count > 0 else '-'
    stop_label = '重複率觸發（dup_threshold）' if stop_reason == 'dup_threshold' else '達最大輪數（max_rounds）'

    # ── Transform 數據 ──
    input_count = transform_stats.get('input_count', 0)
    output_count = transform_stats.get('output_count', 0)
    filtered = transform_stats.get('filtered', 0)
    t1_count = transform_stats.get('t1_count', 0)
    t2_count = transform_stats.get('t2_count', 0)
    filter_rate = f"{filtered / input_count:.0%}" if input_count > 0 else '-'

    # ── 多輪明細 HTML ──
    round_rows = ''.join(
        '<tr><td>第 {r} 輪</td><td>{api}</td><td>{new}</td><td>{dup}</td></tr>'.format(
            r=i + 1,
            api=s.get('api_total', 0),
            new=s.get('new_count', 0),
            dup='{:.0%}'.format(s.get('dup_ratio', 0)),
        )
        for i, s in enumerate(round_details)
    ) or '<tr><td colspan="4">無資料</td></tr>'

    # ── 關鍵字命中 HTML（降冪排序）──
    kw_rows = ''.join(
        '<tr><td>{kw}</td><td>{cnt}</td></tr>'.format(kw=kw, cnt=cnt)
        for kw, cnt in sorted(keyword_counts.items(), key=lambda x: -x[1])
    ) or '<tr><td colspan="2">無資料</td></tr>'

    subject = f"[Threads Pipeline] {batch_date} 執行完成 - {raw_count} 篇"

    body = (
        '<h2>Threads Social Pipeline 執行報告</h2>'
        '<p><b>日期:</b> ' + batch_date + ' | <b>完成時間:</b> ' + now_str + '</p>'

        '<h3>爬蟲效率</h3>'
        '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse">'
        '<tr><th>指標</th><th>數值</th></tr>'
        '<tr><td>有效貼文（時間窗口後）</td><td>' + str(raw_count) + ' 篇</td></tr>'
        '<tr><td>爬蟲輪數</td><td>' + str(rounds) + ' 輪</td></tr>'
        '<tr><td>停止原因</td><td>' + stop_label + '</td></tr>'
        '<tr><td>API Credits 消耗</td><td>' + str(total_credits) + '</td></tr>'
        '<tr><td>每篇有效貼文成本</td><td>' + credit_per_post + ' credits</td></tr>'
        '</table>'

        '<h4>多輪明細</h4>'
        '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse">'
        '<tr><th>輪次</th><th>API 回傳</th><th>新增</th><th>重複率</th></tr>'
        + round_rows +
        '</table>'

        '<h3>資料品質</h3>'
        '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse">'
        '<tr><th>指標</th><th>數值</th></tr>'
        '<tr><td>清洗前</td><td>' + str(input_count) + ' 篇</td></tr>'
        '<tr><td>清洗後（商家文過濾）</td><td>' + str(output_count) + ' 篇</td></tr>'
        '<tr><td>過濾商家文</td><td>' + str(filtered) + ' 篇（' + filter_rate + '）</td></tr>'
        '<tr><td>t1 包車/接送意圖</td><td>' + str(t1_count) + ' 篇</td></tr>'
        '<tr><td>t2 泛旅遊意圖</td><td>' + str(t2_count) + ' 篇</td></tr>'
        '</table>'

        '<h3>關鍵字命中分佈</h3>'
        '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse">'
        '<tr><th>關鍵字</th><th>篇數</th></tr>'
        + kw_rows +
        '</table>'

        '<p style="margin-top:16px">'
        '<a href="https://docs.google.com/spreadsheets/d/1Jk9YRIQ6nFWGIh2fHJxEbaRWSpCCOxnSXumuoysmbdQ">'
        '查看 Google Sheets</a></p>'
    )

    ti.xcom_push(key='email_subject', value=subject)
    ti.xcom_push(key='email_body', value=body)
    return subject


# ── DAG Definition ───────────────────────────────────────────────

default_args = {
    'owner': 'fnte-data',
    'email': ['rex.chan@fnte.com.tw'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

dag = DAG(
    'threads_social_pipeline',
    default_args=default_args,
    description='Threads posts scraper + LLM reply generator pipeline',
    schedule='0 10,15,18 * * *',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=['threads', 'social', 'scraper', 'llm'],
)

# Tasks
extract_task = PythonOperator(
    task_id='extract_threads_posts',
    python_callable=extract_threads_posts,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_threads_data',
    python_callable=transform_threads_data,
    dag=dag,
)

# LLM task 暫時 bypass，保留定義供後續啟用
# llm_task = PythonOperator(
#     task_id='generate_llm_replies',
#     python_callable=generate_llm_replies,
#     execution_timeout=timedelta(minutes=15),
#     dag=dag,
# )

bq_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=dag,
)

sheets_task = PythonOperator(
    task_id='upload_to_gsheets',
    python_callable=upload_to_gsheets,
    dag=dag,
)

build_email_task = PythonOperator(
    task_id='build_notification',
    python_callable=build_notification_content,
    dag=dag,
)

send_email_task = EmailOperator(
    task_id='send_notification_email',
    to='rex.chan@fnte.com.tw',
    subject="{{ task_instance.xcom_pull(task_ids='build_notification', key='email_subject') }}",
    html_content="{{ task_instance.xcom_pull(task_ids='build_notification', key='email_body') }}",
    dag=dag,
)

# Task dependencies (LLM bypass 中)
#
# extract → transform → [bq, sheets] → build_email → send_email
# llm_task 暫時移出 chain，恢復時改回：transform >> llm_task >> [bq, sheets]
#
extract_task >> transform_task >> [bq_task, sheets_task] >> build_email_task >> send_email_task
