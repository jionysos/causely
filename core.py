# core.py
import os
import json
from datetime import date, timedelta

import pandas as pd
from openai import OpenAI


def _client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY 환경변수가 설정되어 있지 않습니다. "
            "터미널에서 export OPENAI_API_KEY='...'(또는 쉘 설정 파일에 추가) 후 다시 실행하세요."
        )
    return OpenAI(api_key=api_key)


def _to_day(ts_series: pd.Series) -> pd.Series:
    return pd.to_datetime(ts_series).dt.date


def build_evidence(
    today: date,
    orders: pd.DataFrame,
    items: pd.DataFrame,
    adj: pd.DataFrame,
    products: pd.DataFrame,
) -> dict:
    """
    3) Evidence packet 생성:
    - KPI (Gross/Refund/Net) today vs yday
    - Drivers: Gross 증가 Top(influencer), Refund 악화 Top(product/seller/reason)
    """
    yday = today - timedelta(days=1)

    items = items.copy()
    adj = adj.copy()

    # 날짜 컬럼 파싱
    if "order_ts" not in items.columns:
        raise ValueError("order_items.csv에 order_ts 컬럼이 필요합니다.")
    if "event_ts" not in adj.columns:
        raise ValueError("adjustments.csv에 event_ts 컬럼이 필요합니다.")

    items["d"] = _to_day(items["order_ts"])
    adj["d"] = _to_day(adj["event_ts"])

    # KPI 계산
    if "net_sales_amount" not in items.columns:
        raise ValueError("order_items.csv에 net_sales_amount 컬럼이 필요합니다.")
    if "amount" not in adj.columns:
        raise ValueError("adjustments.csv에 amount 컬럼이 필요합니다.")

    gross_today = float(items.loc[items["d"] == today, "net_sales_amount"].sum())
    gross_yday = float(items.loc[items["d"] == yday, "net_sales_amount"].sum())

    refund_today = float(adj.loc[adj["d"] == today, "amount"].sum())  # 음수
    refund_yday = float(adj.loc[adj["d"] == yday, "amount"].sum())

    net_today = gross_today + refund_today
    net_yday = gross_yday + refund_yday

    # Driver 1) Gross 증가 Top: influencer_id 기준
    influencer_col = "influencer_id"
    if influencer_col in items.columns:
        it = items.copy()
        it[influencer_col] = it[influencer_col].fillna("NONE")

        g_today = it[it["d"] == today].groupby(influencer_col)["net_sales_amount"].sum()
        g_yday = it[it["d"] == yday].groupby(influencer_col)["net_sales_amount"].sum()

        idx = sorted(set(g_today.index) | set(g_yday.index))
        g_delta = (g_today.reindex(idx, fill_value=0) - g_yday.reindex(idx, fill_value=0)).sort_values(
            ascending=False
        )

        gross_top = (
            g_delta[g_delta.index != "NONE"]
            .head(5)
            .reset_index()
            .rename(columns={0: "delta_gross", "net_sales_amount": "delta_gross"})
        )
        # pandas 버전에 따라 컬럼명이 달라질 수 있어서 강제
        if gross_top.columns.tolist() == [influencer_col, 0]:
            gross_top.columns = [influencer_col, "delta_gross"]

        gross_top = gross_top.to_dict(orient="records")
    else:
        gross_top = []

    # Driver 2) Refund 악화 Top: product_id 기준 (더 음수로 가는 delta가 악화)
    if "product_id" in adj.columns and "product_id" in products.columns:
        r_today = adj[adj["d"] == today].groupby("product_id")["amount"].sum()
        r_yday = adj[adj["d"] == yday].groupby("product_id")["amount"].sum()

        idx = sorted(set(r_today.index) | set(r_yday.index))
        r_delta = (r_today.reindex(idx, fill_value=0) - r_yday.reindex(idx, fill_value=0)).sort_values()
        refund_top_raw = r_delta.head(5).reset_index()
        refund_top_raw.columns = ["product_id", "delta_refund"]

        refund_top = []
        for _, row in refund_top_raw.iterrows():
            if float(row["delta_refund"]) >= 0:
                continue
            pid = row["product_id"]
            p_rows = products[products["product_id"] == pid]
            pinfo = p_rows.iloc[0].to_dict() if len(p_rows) else {}

            # reason_code breakdown (있으면)
            reasons = []
            if "reason_code" in adj.columns:
                reasons = (
                    adj[(adj["d"] == today) & (adj["product_id"] == pid)]
                    .groupby("reason_code")["amount"]
                    .sum()
                    .sort_values()
                    .head(3)
                    .reset_index()
                    .to_dict(orient="records")
                )

            refund_top.append(
                {
                    "product_id": pid,
                    "product_name": pinfo.get("product_name"),
                    "seller_id": pinfo.get("seller_id"),
                    "delta_refund": float(row["delta_refund"]),
                    "today_refund": float(r_today.reindex([pid], fill_value=0).iloc[0]),
                    "yday_refund": float(r_yday.reindex([pid], fill_value=0).iloc[0]),
                    "top_reasons": reasons,
                }
            )
    else:
        refund_top = []

    return {
        "date": str(today),
        "compare_to": str(yday),
        "kpis": {
            "gross_today": gross_today,
            "gross_yday": gross_yday,
            "gross_delta": gross_today - gross_yday,
            "refund_today": refund_today,
            "refund_yday": refund_yday,
            "refund_delta": refund_today - refund_yday,
            "net_today": net_today,
            "net_yday": net_yday,
            "net_delta": net_today - net_yday,
        },
        "drivers": {
            "gross_increase_top": gross_top,
            "refund_worsen_top": refund_top,
        },
    }


BRIEFING_JSON_SCHEMA = {
    "name": "daily_briefing",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "headline": {"type": "string"},
            "key_findings": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 3,
                "maxItems": 5,
            },
            "actions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "title": {"type": "string"},
                        "why": {"type": "string"},
                        "expected_impact": {"type": "string"},
                    },
                    "required": ["title", "why"],
                },
                "minItems": 2,
                "maxItems": 4,
            },
        },
        "required": ["headline", "key_findings", "actions"],
    },
    "strict": True,
}

def generate_briefing(evidence: dict, model: str = "gpt-4o-mini") -> dict:
    client = _client()

    system = (
        "You are an operations analyst for an ecommerce CEO. "
        "Use ONLY the provided evidence. Do not invent facts. "
        "Return ONLY valid JSON. No markdown, no extra text."
    )

    user = {
        "task": "Write a concise daily briefing and action plan.",
        "output_schema": {
            "headline": "string",
            "key_findings": ["string (3-5 items)"],
            "actions": [
                {"title": "string", "why": "string", "expected_impact": "string (optional)"}
            ]
        },
        "evidence": evidence
    }

    # chat.completions는 거의 모든 버전에서 동작
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)}
        ],
        temperature=0.2
    )

    text = resp.choices[0].message.content.strip()

    # 혹시 앞뒤에 잡텍스트 붙으면 JSON 부분만 최대한 추출
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError(f"Model did not return JSON. Output was:\n{text}")

    return json.loads(text[start:end+1])
