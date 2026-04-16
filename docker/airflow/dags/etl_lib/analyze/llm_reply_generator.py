"""
LLM Reply Generator — Claude Sonnet
=====================================
對每則 Threads 貼文生成結構化 JSON 輸出：
- suggested_reply: 友善自然的 Threads 回覆（200 字內）
- sentiment: positive / neutral / negative / seeking_help
- reply_priority: 1-5（5 = 最值得回覆）

使用 Anthropic Python SDK，批次處理以減少 API 呼叫。
"""

import json
import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """你是一位專業的日本包車旅遊服務社群行銷專員。
你的品牌提供沖繩、北海道、大阪、富士山、福岡、九州等地的包車、機場接送、景點接送服務。

你的任務是針對 Threads 上與日本旅遊/包車相關的貼文，進行分析並生成建議回覆。

回覆原則：
1. 友善、自然、不帶銷售壓力
2. 先回應貼文的情境或問題，再自然帶入旅遊建議
3. 繁體中文，口語化但不失專業
4. 200 字以內
5. 不主動提及品牌名、LINE ID、價格等商業資訊
6. 針對 seeking_help 或 negative sentiment 的貼文優先回覆"""

BATCH_PROMPT_TEMPLATE = """以下是 {count} 則 Threads 貼文，請對每則進行分析並生成建議回覆。

{posts_text}

請以 JSON array 格式回覆，每個元素包含：
- "post_id": 貼文 ID（字串）
- "sentiment": "positive" | "neutral" | "negative" | "seeking_help"
- "reply_priority": 1-5 的整數（5 = 最值得回覆，seeking_help/negative 通常 4-5）
- "suggested_reply": 建議回覆文字（200 字內，繁體中文）

只回覆 JSON array，不要其他文字。"""


def _format_posts_for_prompt(posts: list[dict]) -> str:
    """將貼文列表格式化為 prompt 輸入。"""
    lines = []
    for i, p in enumerate(posts, 1):
        text = p.get('text', '')[:500]  # 截取前 500 字
        username = p.get('username', 'unknown')
        post_id = p.get('post_id', '')
        kw = p.get('search_keyword', '')
        lines.append(
            f"[{i}] post_id={post_id}\n"
            f"    @{username} | keyword={kw}\n"
            f"    {text}\n"
        )
    return "\n".join(lines)


def _parse_llm_response(response_text: str, expected_ids: list[str]) -> list[dict]:
    """解析 LLM 回覆的 JSON，處理格式異常。"""
    # 嘗試從回覆中提取 JSON array
    text = response_text.strip()
    # 移除 markdown code block 包裝
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        text = text.strip()

    try:
        results = json.loads(text)
        if isinstance(results, list):
            return results
    except json.JSONDecodeError:
        logger.warning(f"Failed to parse LLM JSON response: {text[:200]}...")

    # fallback: 為每個 post_id 回傳空結果
    return [
        {
            "post_id": pid,
            "sentiment": "neutral",
            "reply_priority": 1,
            "suggested_reply": None,
        }
        for pid in expected_ids
    ]


def generate_replies_batch(
    posts: list[dict],
    anthropic_api_key: str,
    model: str = "claude-sonnet-4-20250514",
    max_tokens: int = 4096,
    batch_size: int = 8,
) -> list[dict]:
    """
    批次生成 LLM 回覆。

    Args:
        posts: 貼文 dict 列表（需含 post_id, text, username, search_keyword）
        anthropic_api_key: Anthropic API key
        model: Claude model ID
        max_tokens: 最大 output tokens
        batch_size: 每批處理幾則貼文

    Returns:
        list[dict]，每個 dict 含 post_id, sentiment, reply_priority, suggested_reply
    """
    import anthropic

    client = anthropic.Anthropic(api_key=anthropic_api_key)
    all_results = []

    for i in range(0, len(posts), batch_size):
        batch = posts[i:i + batch_size]
        posts_text = _format_posts_for_prompt(batch)
        expected_ids = [p['post_id'] for p in batch]

        prompt = BATCH_PROMPT_TEMPLATE.format(
            count=len(batch),
            posts_text=posts_text,
        )

        try:
            message = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )

            response_text = message.content[0].text
            batch_results = _parse_llm_response(response_text, expected_ids)
            all_results.extend(batch_results)

            logger.info(
                f"LLM batch {i // batch_size + 1}: "
                f"{len(batch)} posts -> {len(batch_results)} replies "
                f"(tokens: {message.usage.input_tokens}+{message.usage.output_tokens})"
            )

        except Exception as e:
            logger.error(f"LLM batch {i // batch_size + 1} failed: {e}")
            # fallback
            for p in batch:
                all_results.append({
                    "post_id": p['post_id'],
                    "sentiment": "neutral",
                    "reply_priority": 1,
                    "suggested_reply": None,
                })

    return all_results


def enrich_df_with_replies(
    df: pd.DataFrame,
    anthropic_api_key: str,
    model: str = "claude-sonnet-4-20250514",
    batch_size: int = 8,
) -> pd.DataFrame:
    """
    將 LLM 生成的 sentiment, reply_priority, suggested_reply 合併回 DataFrame。

    Args:
        df: 含 post_id, text, username, search_keyword 的 DataFrame
        anthropic_api_key: Anthropic API key
        model: Claude model ID
        batch_size: 每批處理幾則貼文

    Returns:
        新增 sentiment, reply_priority, suggested_reply 三欄的 DataFrame
    """
    if df.empty:
        df['sentiment'] = None
        df['reply_priority'] = None
        df['suggested_reply'] = None
        return df

    posts = df[['post_id', 'text', 'username', 'search_keyword']].to_dict('records')

    results = generate_replies_batch(
        posts=posts,
        anthropic_api_key=anthropic_api_key,
        model=model,
        batch_size=batch_size,
    )

    results_df = pd.DataFrame(results)
    results_df['post_id'] = results_df['post_id'].astype(str)
    df['post_id'] = df['post_id'].astype(str)

    df = df.merge(
        results_df[['post_id', 'sentiment', 'reply_priority', 'suggested_reply']],
        on='post_id',
        how='left',
    )

    logger.info(
        f"LLM enrichment: {len(df)} posts, "
        f"{df['suggested_reply'].notna().sum()} with replies"
    )

    return df
