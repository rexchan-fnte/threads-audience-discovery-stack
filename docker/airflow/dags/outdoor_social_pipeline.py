"""
Outdoor Social Pipeline DAG
=============================
排程：每日 10:00 (Asia/Taipei)
流程：Extract → Transform → Load BQ + Sheets → Email

戶外裝備市場聲量觀測，用於 Google Ads 關鍵字優化、文案素材方向、競品監測。
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
    config_path = '/opt/airflow/dags/etl_lib/configs/outdoor_config.yaml'
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

def extract_outdoor_posts(**context):
    """抽取 Threads 戶外裝備貼文（manual_mode 全天抓取 + BQ 去重）"""
    from etl_lib.extract.fetch_threads_posts import fetch_threads_posts
    from google.cloud import bigquery
    import pandas as pd
    from zoneinfo import ZoneInfo

    config = _load_config()
    keywords = _get_all_keywords(config)
    bq_cfg = config['bigquery']
    scraper_cfg = config['scraper']

    execution_dt = datetime.now(tz=ZoneInfo("Asia/Taipei"))
    print(f"Extract start: {execution_dt.isoformat()}, {len(keywords)} keywords")

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
        min_rounds=scraper_cfg.get('min_rounds', 1),
        max_rounds=scraper_cfg.get('max_rounds', 2),
        dup_threshold=scraper_cfg.get('dup_threshold', 0.7),
        manual_mode=True,  # 全天抓取，繞過 SCHEDULE_HOURS 邏輯
    )

    if df.empty:
        from airflow.exceptions import AirflowSkipException
        print("Extract got 0 posts — skipping downstream tasks")
        raise AirflowSkipException("No posts extracted")

    batch_date = context['ds']
    temp_path = f'/opt/airflow/output/outdoor_raw_{batch_date}_{execution_dt.strftime("%H%M")}.csv'
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

def transform_outdoor_data(**context):
    """清洗 + 意圖分類 + 品牌/品類偵測 + 商家文/噪音過濾"""
    from etl_lib.transform.outdoor_transform import transform_outdoor_data as do_transform
    import pandas as pd

    config = _load_config()

    ti = context['task_instance']
    raw_path = ti.xcom_pull(task_ids='extract_outdoor_posts')

    if not raw_path or not os.path.exists(raw_path):
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException("No raw CSV from extract task")

    df = pd.read_csv(raw_path, encoding='utf-8-sig')
    if df.empty:
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException("Raw CSV is empty")
    print(f"Transform input: {len(df)} posts")

    df_clean = do_transform(df=df, config=config)

    batch_date = context['ds']
    output_path = f'/opt/airflow/output/outdoor_transformed_{batch_date}.csv'
    df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')

    # 統計 intent 分佈
    intent_dist = df_clean['intent_type'].value_counts().to_dict() if not df_clean.empty else {}
    # 統計品牌提及
    brand_counts = {}
    if not df_clean.empty:
        for brands_str in df_clean['mentioned_brands'].dropna():
            if brands_str:
                for b in brands_str.split(','):
                    brand_counts[b.strip()] = brand_counts.get(b.strip(), 0) + 1
    # 統計品類提及
    gear_counts = {}
    if not df_clean.empty:
        for gear_str in df_clean['mentioned_gear'].dropna():
            if gear_str:
                for g in gear_str.split(','):
                    gear_counts[g.strip()] = gear_counts.get(g.strip(), 0) + 1

    ti.xcom_push(key='transform_stats', value={
        'input_count': len(df),
        'output_count': len(df_clean),
        'filtered': len(df) - len(df_clean),
        'intent_dist': intent_dist,
        'brand_counts': brand_counts,
        'gear_counts': gear_counts,
    })

    print(f"Transform done: {len(df)} -> {len(df_clean)} posts")
    return output_path


# ── Task 3: Load to BigQuery ─────────────────────────────────────

def load_to_bigquery(**context):
    """上傳到 BigQuery（WRITE_APPEND 增量模式）"""
    from etl_lib.load.load_bigquery import upload_to_bigquery
    import pandas as pd

    config = _load_config()
    bq_cfg = config['bigquery']

    ti = context['task_instance']
    transformed_path = ti.xcom_pull(task_ids='transform_outdoor_data')

    df = pd.read_csv(transformed_path, encoding='utf-8-sig')
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


# ── Task 4: Upload to Google Sheets ──────────────────────────────

def upload_to_gsheets(**context):
    """上傳到 Google Sheets（增量 append）"""
    from etl_lib.load.load_gsheets import upload_to_gsheets as do_upload
    import pandas as pd

    config = _load_config()
    gs_cfg = config['gsheets']

    ti = context['task_instance']
    transformed_path = ti.xcom_pull(task_ids='transform_outdoor_data')

    df = pd.read_csv(transformed_path, encoding='utf-8-sig')
    print(f"Sheets upload: {len(df)} posts")

    if df.empty:
        print("No data to upload, skipping Sheets")
        return {'rows_uploaded': 0}

    df['scrape_date'] = datetime.now().strftime('%Y-%m-%d')

    result = do_upload(
        df=df,
        sheet_id=gs_cfg['sheet_id'],
        creds_path=gs_cfg['creds_path'],
        worksheet_name=gs_cfg.get('worksheet_name', 'outdoor_raw'),
        columns=gs_cfg.get('columns'),
    )

    print(f"Sheets upload done: {result}")
    return result


# ── Task 5: Build Email Content ──────────────────────────────────

def build_notification_content(**context):
    """組合通知 Email — 行銷洞察為主（品牌排行/品類熱度/意圖分佈/高互動貼文）"""
    import pandas as pd

    ti = context['task_instance']
    extract_stats = ti.xcom_pull(task_ids='extract_outdoor_posts', key='extract_stats') or {}
    transform_stats = ti.xcom_pull(task_ids='transform_outdoor_data', key='transform_stats') or {}

    batch_date = context['ds']
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')

    TABLE_STYLE = 'border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse"'

    # ── Extract 數據 ──
    raw_count = extract_stats.get('raw_count', 0)
    rounds = extract_stats.get('rounds', 0)
    total_credits = extract_stats.get('total_credits', 0)
    stop_reason = extract_stats.get('stop_reason', '-')
    round_details = extract_stats.get('round_details', [])
    keyword_counts = extract_stats.get('keyword_counts', {})

    credit_per_post = f"{total_credits / raw_count:.1f}" if raw_count > 0 else '-'
    stop_label = '重複率觸發' if stop_reason == 'dup_threshold' else '達最大輪數'

    # ── Transform 數據 ──
    output_count = transform_stats.get('output_count', 0)
    filtered = transform_stats.get('filtered', 0)
    intent_dist = transform_stats.get('intent_dist', {})
    brand_counts = transform_stats.get('brand_counts', {})
    gear_counts = transform_stats.get('gear_counts', {})

    # ── 品牌聲量排行 HTML ──
    brand_rows = ''.join(
        '<tr><td>{b}</td><td>{c}</td></tr>'.format(b=b, c=c)
        for b, c in sorted(brand_counts.items(), key=lambda x: -x[1])
    ) or '<tr><td colspan="2">無品牌提及</td></tr>'

    # ── 品類熱度排行 HTML ──
    gear_rows = ''.join(
        '<tr><td>{g}</td><td>{c}</td></tr>'.format(g=g, c=c)
        for g, c in sorted(gear_counts.items(), key=lambda x: -x[1])
    ) or '<tr><td colspan="2">無品類提及</td></tr>'

    # ── 意圖分佈 HTML ──
    intent_rows = ''.join(
        '<tr><td>{intent}</td><td>{cnt}</td></tr>'.format(intent=intent, cnt=cnt)
        for intent, cnt in sorted(intent_dist.items(), key=lambda x: -x[1])
    ) or '<tr><td colspan="2">無資料</td></tr>'

    # ── 關鍵字表現 HTML（降冪排序）──
    kw_rows = ''.join(
        '<tr><td>{kw}</td><td>{cnt}</td></tr>'.format(kw=kw, cnt=cnt)
        for kw, cnt in sorted(keyword_counts.items(), key=lambda x: -x[1])
    ) or '<tr><td colspan="2">無資料</td></tr>'

    # ── 高互動貼文 Top 5 ──
    top_posts_html = ''
    transformed_path = ti.xcom_pull(task_ids='transform_outdoor_data')
    if transformed_path and os.path.exists(transformed_path):
        try:
            df_top = pd.read_csv(transformed_path, encoding='utf-8-sig')
            if not df_top.empty:
                df_top = df_top.nlargest(5, 'total_engagement')
                top_rows = ''
                for _, row in df_top.iterrows():
                    text_preview = str(row.get('text', ''))[:80].replace('\n', ' ')
                    permalink = row.get('permalink', '#')
                    engagement = row.get('total_engagement', 0)
                    brands = row.get('mentioned_brands', '')
                    gear = row.get('mentioned_gear', '')
                    top_rows += (
                        '<tr>'
                        f'<td><a href="{permalink}">{text_preview}…</a></td>'
                        f'<td>{engagement}</td>'
                        f'<td>{brands}</td>'
                        f'<td>{gear}</td>'
                        '</tr>'
                    )
                top_posts_html = (
                    f'<h3>高互動貼文 Top 5（文案靈感 / KOL 發現）</h3>'
                    f'<table {TABLE_STYLE}>'
                    '<tr><th>內容摘要</th><th>互動數</th><th>品牌</th><th>品類</th></tr>'
                    + top_rows +
                    '</table>'
                )
        except Exception:
            top_posts_html = ''

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

    subject = f"[Outdoor] {batch_date} 聲量報告 — {output_count} 篇有效貼文"

    body = (
        '<h2>戶外裝備社群聲量日報</h2>'
        '<p><b>日期:</b> ' + batch_date + ' | <b>完成時間:</b> ' + now_str + '</p>'

        # 品牌聲量（→ 競品監測、品牌 Ads 出價）
        '<h3>品牌聲量排行（→ Google Ads 品牌關鍵字出價）</h3>'
        f'<table {TABLE_STYLE}>'
        '<tr><th>品牌</th><th>提及數</th></tr>'
        + brand_rows +
        '</table>'

        # 品類熱度（→ 主力品類、Ads 品類詞）
        '<h3>品類熱度排行（→ 主力品類選擇）</h3>'
        f'<table {TABLE_STYLE}>'
        '<tr><th>品類</th><th>提及數</th></tr>'
        + gear_rows +
        '</table>'

        # 意圖分佈（→ 文案角度）
        '<h3>貼文意圖分佈（→ 文案角度決策）</h3>'
        f'<table {TABLE_STYLE}>'
        '<tr><th>意圖類型</th><th>篇數</th></tr>'
        + intent_rows +
        '</table>'

        # 高互動貼文
        + top_posts_html +

        # 關鍵字表現（→ Google Ads 關鍵字調整）
        '<h3>關鍵字命中分佈（→ Google Ads 關鍵字調整）</h3>'
        f'<table {TABLE_STYLE}>'
        '<tr><th>關鍵字</th><th>篇數</th></tr>'
        + kw_rows +
        '</table>'

        # 爬蟲效率
        '<h3>爬蟲效率</h3>'
        f'<table {TABLE_STYLE}>'
        '<tr><th>指標</th><th>數值</th></tr>'
        '<tr><td>原始貼文</td><td>' + str(raw_count) + ' 篇</td></tr>'
        '<tr><td>過濾後有效</td><td>' + str(output_count) + ' 篇</td></tr>'
        '<tr><td>爬蟲輪數</td><td>' + str(rounds) + ' 輪</td></tr>'
        '<tr><td>停止原因</td><td>' + stop_label + '</td></tr>'
        '<tr><td>API Credits</td><td>' + str(total_credits) + '</td></tr>'
        '<tr><td>每篇成本</td><td>' + credit_per_post + ' credits</td></tr>'
        '</table>'

        '<h4>多輪明細</h4>'
        f'<table {TABLE_STYLE}>'
        '<tr><th>輪次</th><th>API 回傳</th><th>新增</th><th>重複率</th></tr>'
        + round_rows +
        '</table>'
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
    'outdoor_social_pipeline',
    default_args=default_args,
    description='Outdoor gear social listening — brand SOV, gear trends, intent analysis',
    schedule='0 10 * * *',
    start_date=datetime(2026, 4, 14),
    catchup=False,
    max_active_runs=1,
    tags=['outdoor', 'threads', 'market-research'],
)

# Tasks
extract_task = PythonOperator(
    task_id='extract_outdoor_posts',
    python_callable=extract_outdoor_posts,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_outdoor_data',
    python_callable=transform_outdoor_data,
    dag=dag,
)

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

# extract → transform → [bq, sheets] → build_email → send_email
extract_task >> transform_task >> [bq_task, sheets_task] >> build_email_task >> send_email_task
