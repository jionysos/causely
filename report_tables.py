# report_tables.py — Key metric, 비용 상세, IV 랭킹, LLM용 components
from __future__ import annotations
from datetime import date
from typing import Any, List, Optional

import numpy as np
import pandas as pd
from woe_iv import woe_iv


def _to_day(ts: pd.Series) -> pd.Series:
    return pd.to_datetime(ts).dt.date


def _sales_col(items: pd.DataFrame) -> str:
    if "gross_amount" in items.columns:
        return "gross_amount"
    return "net_sales_amount"


def build_key_metric_table(
    today: date,
    compare_date: date,
    items: pd.DataFrame,
    adj: pd.DataFrame,
    ad_costs: Optional[pd.DataFrame] = None,
    influencer_costs: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Key metric: 오늘/기준일 총매출, 총비용, 순이익."""
    items = items.copy()
    adj = adj.copy()
    items["d"] = _to_day(items["order_ts"])
    adj["d"] = _to_day(adj["event_ts"])
    col = _sales_col(items)

    def gross(d: date) -> float:
        return float(items.loc[items["d"] == d, col].sum())

    def refund_abs(d: date) -> float:
        return abs(float(adj.loc[adj["d"] == d, "amount"].sum()))

    def coupon(d: date) -> float:
        if "discount_amount" not in items.columns:
            return 0.0
        return float(items.loc[items["d"] == d, "discount_amount"].sum())

    def ad_cost(d: date) -> float:
        if ad_costs is None or ad_costs.empty:
            return 0.0
        dc = ad_costs.copy()
        if "event_ts" in dc.columns:
            dc["date"] = _to_day(dc["event_ts"])
        elif "date" not in dc.columns:
            return 0.0
        amt_col = "amount" if "amount" in dc.columns else "cost"
        if amt_col not in dc.columns:
            return 0.0
        return float(dc.loc[dc["date"] == d, amt_col].sum())

    def inf_cost(d: date) -> float:
        if influencer_costs is None or influencer_costs.empty:
            return 0.0
        dc = influencer_costs.copy()
        if "event_ts" in dc.columns:
            dc["date"] = _to_day(dc["event_ts"])
        elif "date" not in dc.columns:
            return 0.0
        if "date" not in dc.columns and "event_ts" in dc.columns:
            dc["date"] = _to_day(dc["event_ts"])
        amt_col = "amount" if "amount" in dc.columns else "cost"
        if amt_col not in dc.columns:
            return 0.0
        return float(dc.loc[dc["date"] == d, amt_col].sum())

    rows = []
    for label, d in [("오늘", today), ("기준일", compare_date)]:
        총매출 = gross(d)
        환불 = refund_abs(d)
        쿠폰 = coupon(d)
        광고비 = ad_cost(d)
        인플비 = inf_cost(d)
        총비용 = 쿠폰 + 인플비 + 광고비 + 환불
        순이익 = 총매출 - 총비용
        rows.append({"구분": label, "날짜": str(d), "총매출": 총매출, "총비용": 총비용, "순이익": 순이익})
    return pd.DataFrame(rows)


def build_cost_detail_table(
    today: date,
    compare_date: date,
    items: pd.DataFrame,
    adj: pd.DataFrame,
    ad_costs: Optional[pd.DataFrame] = None,
    influencer_costs: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """비용 상세 요약 (기본: 빈 DataFrame 또는 간단 요약)."""
    return pd.DataFrame()


def _stack_binary(items: pd.DataFrame, today: date, compare_date: date, dim_col: str) -> tuple:
    items = items.copy()
    items["d"] = _to_day(items["order_ts"])
    if dim_col not in items.columns:
        items["_bin"] = 0
    else:
        v = items[dim_col]
        valid = v.notna() & (v.astype(str).str.strip() != "") & (v.astype(str).str.upper() != "NONE")
        items["_bin"] = valid.astype(int)
    t = items[items["d"] == today][["_bin"]].copy()
    t["_y"] = 1
    b = items[items["d"] == compare_date][["_bin"]].copy()
    b["_y"] = 0
    stacked = pd.concat([t, b], ignore_index=True)
    return stacked[["_bin"]], stacked["_y"]


def _iv_cost_decile(df: pd.DataFrame, date_col: str, value_col: str, today: date, compare_date: date, bins: int = 10) -> float:
    df = df.copy()
    df["d"] = _to_day(df[date_col])
    sub = df[df["d"].isin([today, compare_date])].copy()
    if sub.empty or value_col not in sub.columns:
        return 0.0
    sub["v"] = pd.to_numeric(sub[value_col], errors="coerce").fillna(0)
    bench = sub[sub["d"] == compare_date]["v"]
    if len(bench) < 2:
        return 0.0
    try:
        q = np.percentile(bench, np.linspace(0, 100, bins + 1)[1:-1])
        q = np.unique(q)
        if len(q) < 2:
            return 0.0
        sub["bin"] = pd.cut(sub["v"], bins=np.concatenate([[-np.inf], q, [np.inf]]), labels=False).astype(str)
        sub["_y"] = (sub["d"] == today).astype(int)
        WoE, IV_df = woe_iv(sub[["bin"]], sub["_y"], bins=len(q) + 1)
        if IV_df is None or IV_df.empty:
            return 0.0
        return float(IV_df["IV"].iloc[0])
    except Exception:
        return 0.0


def _iv_categorical(items: pd.DataFrame, today: date, compare_date: date, dim_col: str, max_bins: int = 50) -> float:
    """카테고리형 변수(예: product_id)에 대한 IV. 기준일 vs 비교기준일 건수 구성비 차이."""
    items = items.copy()
    items["d"] = _to_day(items["order_ts"])
    sub = items[items["d"].isin([today, compare_date])].copy()
    if sub.empty or dim_col not in sub.columns:
        return 0.0
    sub[dim_col] = sub[dim_col].astype(str).replace("nan", "__NA__").replace("", "__NA__")
    data = sub[[dim_col]]
    target = (sub["d"] == today).astype(int)
    try:
        n_unique = sub[dim_col].nunique()
        bins = min(max_bins, max(2, n_unique))
        _, IV_df = woe_iv(data, target, bins=bins)
        if IV_df is None or IV_df.empty:
            return 0.0
        return float(IV_df["IV"].iloc[0])
    except Exception:
        return 0.0


def get_iv_ranking(
    items: pd.DataFrame,
    adj: pd.DataFrame,
    today: date,
    compare_date: date,
) -> dict:
    """채널/광고/인플루언서 여부 이진 IV + 상품(product_id) IV + 쿠폰/환불 비용 IV. 반환: { ranking: [(name, iv), ...] }."""
    ranking = []
    if "product_id" in items.columns:
        iv_p = _iv_categorical(items, today, compare_date, "product_id")
        ranking.append(("상품 (매출)", iv_p))
    for name, col in [("채널 여부 (매출)", "channel"), ("광고 여부 (매출)", "ad_id"), ("인플루언서 여부 (매출)", "influencer_id")]:
        if col not in items.columns:
            ranking.append((name, 0.0))
            continue
        try:
            data, target = _stack_binary(items, today, compare_date, col)
            _, IV_df = woe_iv(data, target, bins=2)
            iv = float(IV_df["IV"].iloc[0]) if IV_df is not None and not IV_df.empty else 0.0
            ranking.append((name, iv))
        except Exception:
            ranking.append((name, 0.0))
    if "discount_amount" in items.columns and "order_ts" in items.columns:
        iv_c = _iv_cost_decile(items, "order_ts", "discount_amount", today, compare_date)
        ranking.append(("쿠폰비용 (비용)", iv_c))
    if "event_ts" in adj.columns and "amount" in adj.columns:
        iv_r = _iv_cost_decile(adj, "event_ts", "amount", today, compare_date)
        ranking.append(("환불액 (비용)", iv_r))
    ranking.sort(key=lambda x: -x[1])
    return {"ranking": ranking}


def _detail_table(items: pd.DataFrame, today: date, compare_date: date, id_col: str, id_label: str, top_n: int = 5) -> pd.DataFrame:
    items = items.copy()
    items["d"] = _to_day(items["order_ts"])
    col = _sales_col(items)
    if id_col not in items.columns:
        return pd.DataFrame(columns=[id_label, "오늘자 매출", "기준일 매출"])
    g_t = items[items["d"] == today].groupby(id_col, dropna=False)[col].sum()
    g_b = items[items["d"] == compare_date].groupby(id_col, dropna=False)[col].sum()
    idx = g_t.index.union(g_b.index).unique()
    df = pd.DataFrame({
        id_label: idx,
        "오늘자 매출": g_t.reindex(idx, fill_value=0).values,
        "기준일 매출": g_b.reindex(idx, fill_value=0).values,
    })
    df = df.sort_values("오늘자 매출", ascending=False).head(top_n)
    return df.reset_index(drop=True)


def get_high_iv_detail_tables(
    items: pd.DataFrame,
    adj: pd.DataFrame,
    today: date,
    compare_date: date,
    iv_result: dict,
    cost_detail_df: pd.DataFrame,
    threshold: float = 20,
    top_n: int = 5,
    products: Optional[pd.DataFrame] = None,
) -> List[dict]:
    """IV가 threshold 초과인 요인만 상세 테이블 2벌(요약+상세) 반환."""
    ranking = iv_result.get("ranking", [])
    high = [(name, iv) for name, iv in ranking if iv > threshold]
    col = _sales_col(items)
    items_d = items.copy()
    items_d["d"] = _to_day(items_d["order_ts"])
    total_t = float(items_d.loc[items_d["d"] == today, col].sum())
    total_b = float(items_d.loc[items_d["d"] == compare_date, col].sum())
    summary_sales = pd.DataFrame({
        "날짜": [str(today), str(compare_date)],
        "매출": [total_t, total_b],
    })

    def _strip(s: str) -> str:
        return s.replace(" (매출)", "").replace(" (비용)", "").strip()

    out = []
    for name, iv in high:
        key = _strip(name)
        if key == "상품" and "product_id" in items.columns:
            detail = _detail_table(items, today, compare_date, "product_id", "상품 id", top_n)
            if products is not None and not products.empty and "product_id" in products.columns and "product_name" in products.columns:
                pid_to_name = products.set_index("product_id")["product_name"].astype(str).to_dict()
                id_col = detail.columns[0]
                detail = detail.copy()
                detail["상품명"] = detail[id_col].map(lambda x: pid_to_name.get(x, str(x)))
                detail = detail[["상품명", "오늘자 매출", "기준일 매출"]]
            out.append({"factor": name, "iv": iv, "summary_table": summary_sales, "detail_table": detail})
        elif key == "인플루언서 여부" and "influencer_id" in items.columns:
            detail = _detail_table(items, today, compare_date, "influencer_id", "인플루언서 id", top_n)
            out.append({"factor": name, "iv": iv, "summary_table": summary_sales, "detail_table": detail})
        elif key == "채널 여부" and "channel" in items.columns:
            detail = _detail_table(items, today, compare_date, "channel", "채널구분", top_n)
            out.append({"factor": name, "iv": iv, "summary_table": summary_sales, "detail_table": detail})
        elif key == "광고 여부" and "ad_id" in items.columns:
            detail = _detail_table(items, today, compare_date, "ad_id", "광고 id", top_n)
            out.append({"factor": name, "iv": iv, "summary_table": summary_sales, "detail_table": detail})
        elif key == "환불액":
            adj_d = adj.copy()
            adj_d["d"] = _to_day(adj_d["event_ts"])
            summary = pd.DataFrame({
                "날짜": [str(today), str(compare_date)],
                "환불액": [
                    abs(float(adj_d.loc[adj_d["d"] == today, "amount"].sum())),
                    abs(float(adj_d.loc[adj_d["d"] == compare_date, "amount"].sum())),
                ],
            })
            id_col = "product_id" if "product_id" in adj.columns else "index"
            if id_col == "index":
                detail = pd.DataFrame(columns=["환불상품명", "오늘자 환불액", "기준일 환불액"])
            else:
                g_t = adj_d[adj_d["d"] == today].groupby(id_col)["amount"].sum()
                g_b = adj_d[adj_d["d"] == compare_date].groupby(id_col)["amount"].sum()
                idx = g_t.index.union(g_b.index).unique()
                detail = pd.DataFrame({
                    "환불상품 id": idx,
                    "오늘자 환불액": g_t.reindex(idx, fill_value=0).values,
                    "기준일 환불액": g_b.reindex(idx, fill_value=0).values,
                }).sort_values("오늘자 환불액", ascending=True).head(top_n)
                if products is not None and not products.empty and "product_id" in products.columns and "product_name" in products.columns:
                    pid_to_name = products.set_index("product_id")["product_name"].astype(str).to_dict()
                    detail["환불상품명"] = detail["환불상품 id"].map(lambda x: pid_to_name.get(x, str(x)))
                    detail = detail[["환불상품명", "오늘자 환불액", "기준일 환불액"]]
            out.append({"factor": name, "iv": iv, "summary_table": summary, "detail_table": detail})
        elif key == "쿠폰비용":
            summary = pd.DataFrame({"날짜": [str(today), str(compare_date)], "쿠폰비용": [
                float(items_d.loc[items_d["d"] == today, "discount_amount"].sum()) if "discount_amount" in items_d.columns else 0,
                float(items_d.loc[items_d["d"] == compare_date, "discount_amount"].sum()) if "discount_amount" in items_d.columns else 0
            ]})
            detail = pd.DataFrame(columns=["쿠폰 id", "오늘자 비용", "기준일 비용"])
            out.append({"factor": name, "iv": iv, "summary_table": summary, "detail_table": detail})
    return out


def build_components_for_llm(
    key_metric_df: pd.DataFrame,
    iv_ranking: dict,
    high_iv_tables: List[dict],
    threshold: float = 20,
) -> dict:
    """LLM 리포트용 components. key_metric, 증감_요약, IV_전체_순위, IV_20_이상_요인_순, IV_20_이상_상세_테이블."""
    ranking = iv_ranking.get("ranking", [])
    high_ranking = [{"요인": name, "IV": iv} for name, iv in ranking if iv > threshold]
    full_ranking = [{"요인": name, "IV": iv} for name, iv in ranking]

    def _records(df: Optional[pd.DataFrame]) -> list:
        if df is None or df.empty:
            return []
        return df.to_dict(orient="records")

    components = {
        "key_metric": key_metric_df.to_dict(orient="records"),
        "증감_요약": {},
        "IV_전체_순위": full_ranking,
        "IV_20_이상_요인_순": high_ranking,
        "IV_20_이상_상세_테이블": [
            {
                "factor": t["factor"],
                "iv": t["iv"],
                "summary": _records(t.get("summary_table")),
                "detail": _records(t.get("detail_table")),
            }
            for t in high_iv_tables
        ],
    }
    if len(key_metric_df) >= 2:
        row_t = key_metric_df[key_metric_df["구분"] == "오늘"].iloc[0]
        row_b = key_metric_df[key_metric_df["구분"] == "기준일"].iloc[0]
        for c in ["총매출", "총비용", "순이익"]:
            if c in row_t and c in row_b:
                a, b = row_t[c], row_b[c]
                pct = ((a - b) / b * 100) if b != 0 else 0
                components["증감_요약"][c] = {"오늘": a, "기준일": b, "증감_pct": round(pct, 1)}
    return components
