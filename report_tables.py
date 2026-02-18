# report_tables.py — Key metric / 매출 상세 / 비용 상세 테이블 및 IV 기반 차이 분석
from __future__ import annotations
from datetime import date, timedelta
from typing import Any, Optional

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
    items_discount_col: str = "discount_amount",
    ad_costs: Optional[pd.DataFrame] = None,
    influencer_costs: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Key metric 테이블: 기준일, 총매출, 총비용, 순이익.
    총매출 = items 매출 합계, 총비용 = 쿠폰+인플루언서비용+광고비+결제수수료+환불절대값, 순이익 = 총매출 - 총비용.
    """
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
        if items_discount_col not in items.columns:
            return 0.0
        return float(items.loc[items["d"] == d, items_discount_col].sum())

    def ad_cost(d: date) -> float:
        if ad_costs is None or ad_costs.empty or "date" not in ad_costs.columns:
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
        elif "date" not in dc.columns and "event_ts" not in dc.columns:
            return 0.0
        if "date" not in dc.columns:
            dc["date"] = _to_day(dc["event_ts"])
        amt_col = "amount" if "amount" in dc.columns else "cost"
        if amt_col not in dc.columns:
            return 0.0
        return float(dc.loc[dc["date"] == d, amt_col].sum())

    # 결제 수수료: orders 등에 있으면 추가. 없으면 0
    payment_fee = 0.0

    rows = []
    for label, d in [("오늘", today), ("기준일", compare_date)]:
        총매출 = gross(d)
        환불 = refund_abs(d)
        쿠폰 = coupon(d)
        광고비 = ad_cost(d)
        인플루언서비용 = inf_cost(d)
        총비용 = 쿠폰 + 인플루언서비용 + 광고비 + payment_fee + 환불
        순이익 = 총매출 - 총비용
        rows.append({"구분": label, "날짜": str(d), "총매출": 총매출, "총비용": 총비용, "순이익": 순이익})
    return pd.DataFrame(rows)


def _norm_fill_null(items: pd.DataFrame, col: str, has_col: bool) -> pd.DataFrame:
    """해당 없으면 pd.NA(Null), 있으면 원값. 테이블용."""
    if not has_col:
        items[col] = pd.NA
        return items
    s = items[col].astype(str).str.strip()
    items[col] = items[col].where(
        items[col].notna() & (s != "") & (s.str.upper() != "NONE"),
        pd.NA,
    )
    return items


def build_sales_detail_channel(
    today: date,
    compare_date: date,
    items: pd.DataFrame,
    channel_col: str = "channel",
) -> pd.DataFrame:
    """채널별 매출 상세: 기준일, 채널구분, sum(gross_amount). 해당 없으면 Null."""
    items = items.copy()
    items["d"] = _to_day(items["order_ts"])
    col = _sales_col(items)
    has_channel = channel_col in items.columns
    if has_channel:
        items = _norm_fill_null(items, channel_col, True)
        items["채널구분"] = items[channel_col]
    else:
        items["채널구분"] = pd.NA
    group_col = "채널구분"
    rows = []
    for label, d in [("오늘", today), ("기준일", compare_date)]:
        g = items[items["d"] == d].groupby(group_col, dropna=False)[col].sum().reset_index()
        g.columns = ["채널구분", "매출"]
        g.insert(0, "기준일", str(d))
        g.insert(0, "구분", label)
        rows.append(g)
    return pd.concat(rows, ignore_index=True)


def build_sales_detail_ad(
    today: date,
    compare_date: date,
    items: pd.DataFrame,
    ad_col: str = "ad_id",
) -> pd.DataFrame:
    """광고별 매출 상세: 기준일, 광고id, sum(gross_amount). 해당 없으면 Null."""
    items = items.copy()
    items["d"] = _to_day(items["order_ts"])
    col = _sales_col(items)
    if ad_col not in items.columns:
        rows = []
        for label, d in [("오늘", today), ("기준일", compare_date)]:
            rows.append(pd.DataFrame({"구분": [label], "기준일": [str(d)], "광고id": [pd.NA], "매출": [0.0]}))
        return pd.concat(rows, ignore_index=True)
    items = _norm_fill_null(items, ad_col, True)
    items["광고id"] = items[ad_col]
    rows = []
    for label, d in [("오늘", today), ("기준일", compare_date)]:
        g = items[items["d"] == d].groupby("광고id", dropna=False)[col].sum().reset_index()
        g.columns = ["광고id", "매출"]
        g.insert(0, "기준일", str(d))
        g.insert(0, "구분", label)
        rows.append(g)
    return pd.concat(rows, ignore_index=True)


def build_sales_detail_influencer(
    today: date,
    compare_date: date,
    items: pd.DataFrame,
    influencer_col: str = "influencer_id",
) -> pd.DataFrame:
    """인플루언서별 매출 상세: 기준일, 인플루언서id, sum(gross_amount). 해당 없으면 Null."""
    items = items.copy()
    items["d"] = _to_day(items["order_ts"])
    col = _sales_col(items)
    if influencer_col not in items.columns:
        rows = []
        for label, d in [("오늘", today), ("기준일", compare_date)]:
            rows.append(pd.DataFrame({"구분": [label], "기준일": [str(d)], "인플루언서id": [pd.NA], "매출": [0.0]}))
        return pd.concat(rows, ignore_index=True)
    items = _norm_fill_null(items, influencer_col, True)
    items["인플루언서id"] = items[influencer_col]
    rows = []
    for label, d in [("오늘", today), ("기준일", compare_date)]:
        g = items[items["d"] == d].groupby("인플루언서id", dropna=False)[col].sum().reset_index()
        g.columns = ["인플루언서id", "매출"]
        g.insert(0, "기준일", str(d))
        g.insert(0, "구분", label)
        rows.append(g)
    return pd.concat(rows, ignore_index=True)


def build_cost_detail_table(
    today: date,
    compare_date: date,
    items: pd.DataFrame,
    adj: pd.DataFrame,
    ad_costs: Optional[pd.DataFrame] = None,
    influencer_costs: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """비용 상세: 날짜 | 쿠폰비용 | 인플루언서비용 | 광고비 | 결제수수료 | 환불액 | 총 비용."""
    items = items.copy()
    adj = adj.copy()
    items["d"] = _to_day(items["order_ts"])
    adj["d"] = _to_day(adj["event_ts"])
    coupon_col = "discount_amount" if "discount_amount" in items.columns else None

    def refund_abs(d: date) -> float:
        return abs(float(adj.loc[adj["d"] == d, "amount"].sum()))

    def coupon(d: date) -> float:
        if not coupon_col:
            return 0.0
        return float(items.loc[items["d"] == d, coupon_col].sum())

    def ad_cost(d: date) -> float:
        if ad_costs is None or ad_costs.empty:
            return 0.0
        dc = ad_costs.copy()
        if "event_ts" in dc.columns:
            dc["d"] = _to_day(dc["event_ts"])
        elif "date" in dc.columns:
            dc["d"] = pd.to_datetime(dc["date"]).dt.date
        else:
            return 0.0
        amt = "amount" if "amount" in dc.columns else "cost"
        if amt not in dc.columns:
            return 0.0
        return float(dc.loc[dc["d"] == d, amt].sum())

    def inf_cost(d: date) -> float:
        if influencer_costs is None or influencer_costs.empty:
            return 0.0
        dc = influencer_costs.copy()
        if "event_ts" in dc.columns:
            dc["d"] = _to_day(dc["event_ts"])
        elif "date" in dc.columns:
            dc["d"] = pd.to_datetime(dc["date"]).dt.date
        else:
            return 0.0
        amt = "amount" if "amount" in dc.columns else "cost"
        if amt not in dc.columns:
            return 0.0
        return float(dc.loc[dc["d"] == d, amt].sum())

    rows = []
    for d in [today, compare_date]:
        쿠폰비용 = coupon(d)
        인플루언서비용 = inf_cost(d)
        광고비 = ad_cost(d)
        결제수수료 = 0.0
        환불액 = refund_abs(d)
        총비용 = 쿠폰비용 + 인플루언서비용 + 광고비 + 결제수수료 + 환불액
        rows.append({
            "날짜": str(d),
            "쿠폰비용": 쿠폰비용,
            "인플루언서비용": 인플루언서비용,
            "광고비": 광고비,
            "결제 수수료": 결제수수료,
            "환불액": 환불액,
            "총 비용": 총비용,
        })
    return pd.DataFrame(rows)


def _stack_binary_for_iv(
    items: pd.DataFrame,
    today: date,
    compare_date: date,
    dimension_col: str,
    binary_col: str,
) -> tuple[pd.DataFrame, pd.Series]:
    """채널/비채널, 광고/비광고, 인플/비인플 이진 변수로 스택. has_X=1(해당), 0(비해당)."""
    items = items.copy()
    items["d"] = _to_day(items["order_ts"])
    if dimension_col not in items.columns:
        items[binary_col] = 0
    else:
        val = items[dimension_col]
        valid = val.notna() & (val.astype(str).str.strip() != "") & (val.astype(str).str.upper() != "NONE")
        items[binary_col] = valid.astype(int)
    today_df = items[items["d"] == today][[binary_col]].copy()
    today_df["is_today"] = 1
    bench_df = items[items["d"] == compare_date][[binary_col]].copy()
    bench_df["is_today"] = 0
    stacked = pd.concat([today_df, bench_df], ignore_index=True)
    return stacked[[binary_col]], stacked["is_today"]


def compute_iv_binary(
    items: pd.DataFrame,
    today: date,
    compare_date: date,
    dimension_col: str,
    binary_name: str,
    bins: int = 2,
) -> float:
    """채널/비채널, 광고/비광고, 인플/비인플 이진(해당 vs 비해당) IV."""
    data, target = _stack_binary_for_iv(items, today, compare_date, dimension_col, binary_name)
    try:
        _, IV_df = woe_iv(data, target, bins=bins)
        if IV_df is None or IV_df.empty:
            return 0.0
        return float(IV_df["IV"].iloc[0])
    except Exception:
        return 0.0


def _iv_cost_by_decile(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    today: date,
    compare_date: date,
    bins: int = 10,
) -> float:
    """
    비용(금액) IV: 기준일(compare_date) 값 기준 10% 구간화 후,
    각 구간에 들어가는 건수(행 수)의 오늘 vs 기준일 구성비율 차이로 IV 계산.
    df: 각 행 = 주문건 또는 환불건, date_col=날짜, value_col=금액 컬럼명.
    """
    df = df.copy()
    df["d"] = _to_day(df[date_col])
    sub = df[df["d"].isin([today, compare_date])].copy()
    if sub.empty or value_col not in sub.columns:
        return 0.0
    sub["value"] = pd.to_numeric(sub[value_col], errors="coerce").fillna(0)
    bench = sub[sub["d"] == compare_date]["value"]
    if len(bench) < 2:
        return 0.0
    try:
        quantiles = np.percentile(bench, np.linspace(0, 100, bins + 1)[1:-1])
        quantiles = np.unique(quantiles)
        if len(quantiles) < 2:
            return 0.0
        sub["bin"] = pd.cut(sub["value"], bins=np.concatenate([[-np.inf], quantiles, [np.inf]]), labels=False)
        sub["bin"] = sub["bin"].astype(str)
        sub["is_today"] = (sub["d"] == today).astype(int)
        data = sub[["bin"]].copy()
        target = sub["is_today"]
        _, IV_df = woe_iv(data, target, bins=len(quantiles) + 1)
        if IV_df is None or IV_df.empty:
            return 0.0
        return float(IV_df["IV"].iloc[0])
    except Exception:
        return 0.0


def compute_iv_for_cost_columns(
    items: pd.DataFrame,
    adj: pd.DataFrame,
    today: date,
    compare_date: date,
) -> pd.DataFrame:
    """
    비용 IV: 쿠폰비용·환불액은 기준일 10% 구간화 후 구간별 건수 구성비 차이로 IV.
    쿠폰비용 = order_items 건수 기준 구간별 구성, 환불액 = adjustments 건수 기준.
    """
    out = []
    if "discount_amount" in items.columns and "order_ts" in items.columns:
        iv_coupon = _iv_cost_by_decile(items, "order_ts", "discount_amount", today, compare_date, bins=10)
        out.append({"Var_name": "쿠폰비용", "IV": iv_coupon})
    if "event_ts" in adj.columns and "amount" in adj.columns:
        iv_refund = _iv_cost_by_decile(adj, "event_ts", "amount", today, compare_date, bins=10)
        out.append({"Var_name": "환불액", "IV": iv_refund})
    if not out:
        return pd.DataFrame()
    return pd.DataFrame(out)


def get_iv_ranking(
    items: pd.DataFrame,
    adj: pd.DataFrame,
    today: date,
    compare_date: date,
    channel_col: str = "channel",
    ad_col: str = "ad_id",
    influencer_col: str = "influencer_id",
) -> dict[str, Any]:
    """
    채널/비채널, 광고/비광고, 인플/비인플 이진 IV (매출) + 쿠폰/환불 비용 IV (기준일 10% 구간별 건수 구성).
    반환: { "channel_iv", "ad_iv", "influencer_iv", "cost_iv_df", "ranking" }
    ranking 라벨에 (매출)/(비용) 구분 포함.
    """
    channel_iv = compute_iv_binary(items, today, compare_date, channel_col, "has_channel")
    ad_iv = compute_iv_binary(items, today, compare_date, ad_col, "has_ad")
    influencer_iv = compute_iv_binary(items, today, compare_date, influencer_col, "has_influencer")
    cost_iv_df = compute_iv_for_cost_columns(items, adj, today, compare_date)
    ranking = []
    ranking.append(("채널/비채널 (매출)", channel_iv))
    ranking.append(("광고/비광고 (매출)", ad_iv))
    ranking.append(("인플루언서/비인플루언서 (매출)", influencer_iv))
    if not cost_iv_df.empty:
        for _, row in cost_iv_df.iterrows():
            ranking.append((f"{row['Var_name']} (비용)", float(row["IV"])))
    ranking.sort(key=lambda x: -x[1])
    return {
        "channel_iv": channel_iv,
        "ad_iv": ad_iv,
        "influencer_iv": influencer_iv,
        "cost_iv_df": cost_iv_df,
        "ranking": ranking,
    }


def _detail_table_today_base(
    items: pd.DataFrame,
    today: date,
    compare_date: date,
    id_col: str,
    id_label: str,
    top_n: int = 5,
) -> pd.DataFrame:
    """id_col 기준 오늘/기준일 매출 합계 → [id_label | 오늘자 매출 | 기준일 매출], 오늘자 매출 내림차순 top_n."""
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


def _summary_two_rows(today: date, compare_date: date, col_name: str, val_today: float, val_compare: float) -> pd.DataFrame:
    """요약표: 2행 (날짜 | col_name)."""
    return pd.DataFrame({
        "날짜": [str(today), str(compare_date)],
        col_name: [val_today, val_compare],
    })


def _refund_detail_top5(adj: pd.DataFrame, today: date, compare_date: date, top_n: int = 5) -> tuple[pd.DataFrame, pd.DataFrame]:
    """환불액: 요약(날짜|환불액) + 상세(환불상품id|오늘자 환불액|기준일 환불액) 오늘자 환불액 내림차순 top5."""
    adj = adj.copy()
    adj["d"] = _to_day(adj["event_ts"])
    id_col = "product_id" if "product_id" in adj.columns else (adj.index.name or "index")
    if id_col == "index":
        adj = adj.reset_index()
    summary = _summary_two_rows(
        today, compare_date, "환불액",
        float(adj.loc[adj["d"] == today, "amount"].sum()),
        float(adj.loc[adj["d"] == compare_date, "amount"].sum()),
    )
    g_t = adj[adj["d"] == today].groupby(id_col)["amount"].sum()
    g_b = adj[adj["d"] == compare_date].groupby(id_col)["amount"].sum()
    idx = g_t.index.union(g_b.index).unique()
    detail = pd.DataFrame({
        "환불상품 id": idx,
        "오늘자 환불액": g_t.reindex(idx, fill_value=0).values,
        "기준일 환불액": g_b.reindex(idx, fill_value=0).values,
    })
    detail = detail.sort_values("오늘자 환불액", ascending=True).head(top_n).reset_index(drop=True)
    return summary, detail


def _coupon_detail_top5(items: pd.DataFrame, today: date, compare_date: date, top_n: int = 5) -> tuple[pd.DataFrame, pd.DataFrame]:
    """쿠폰비용: 요약(날짜|쿠폰비용) + 상세(쿠폰 id|오늘자 비용|기준일 비용) 오늘자 비용 내림차순 top5."""
    items = items.copy()
    items["d"] = _to_day(items["order_ts"])
    col = "discount_amount" if "discount_amount" in items.columns else None
    if not col:
        return _summary_two_rows(today, compare_date, "쿠폰비용", 0.0, 0.0), pd.DataFrame()
    id_col = "coupon_id" if "coupon_id" in items.columns else ("coupon_code" if "coupon_code" in items.columns else None)
    summary = _summary_two_rows(
        today, compare_date, "쿠폰비용",
        float(items.loc[items["d"] == today, col].sum()),
        float(items.loc[items["d"] == compare_date, col].sum()),
    )
    if not id_col:
        return summary, pd.DataFrame(columns=["쿠폰 id", "오늘자 비용", "기준일 비용"])
    g_t = items[items["d"] == today].groupby(id_col, dropna=False)[col].sum()
    g_b = items[items["d"] == compare_date].groupby(id_col, dropna=False)[col].sum()
    idx = g_t.index.union(g_b.index).unique()
    detail = pd.DataFrame({
        "쿠폰 id": idx,
        "오늘자 비용": g_t.reindex(idx, fill_value=0).values,
        "기준일 비용": g_b.reindex(idx, fill_value=0).values,
    })
    detail = detail.sort_values("오늘자 비용", ascending=False).head(top_n).reset_index(drop=True)
    return summary, detail


def get_high_iv_detail_tables(
    items: pd.DataFrame,
    adj: pd.DataFrame,
    today: date,
    compare_date: date,
    iv_ranking: dict[str, Any],
    cost_detail_df: pd.DataFrame,
    threshold: float = 20,
    top_n: int = 5,
) -> list[dict[str, Any]]:
    """
    IV가 threshold 초과인 요인만 골라, 각 요인별 표 2벌 반환.
    - summary_table: 날짜 | 지표 (2행)
    - detail_table: id | 오늘자 값 | 기준일 값, 오늘자 내림차순 top_n (매출/비용 공통 폼)
    """
    ranking = iv_ranking.get("ranking", [])
    high = [(name, iv) for name, iv in ranking if iv > threshold]
    col = _sales_col(items)
    items_d = items.copy()
    items_d["d"] = _to_day(items_d["order_ts"])
    total_today = float(items_d.loc[items_d["d"] == today, col].sum())
    total_compare = float(items_d.loc[items_d["d"] == compare_date, col].sum())
    summary_sales = _summary_two_rows(today, compare_date, "매출", total_today, total_compare)

    def _strip_label(s: str) -> str:
        return s.replace(" (매출)", "").replace(" (비용)", "").strip()

    out = []
    for name, iv in high:
        key = _strip_label(name)
        if key == "인플루언서/비인플루언서" and "influencer_id" in items.columns:
            detail = _detail_table_today_base(items, today, compare_date, "influencer_id", "인플루언서 id", top_n)
            out.append({"factor": name, "iv": iv, "summary_table": summary_sales, "detail_table": detail})
        elif key == "채널/비채널" and "channel" in items.columns:
            detail = _detail_table_today_base(items, today, compare_date, "channel", "채널구분", top_n)
            out.append({"factor": name, "iv": iv, "summary_table": summary_sales, "detail_table": detail})
        elif key == "광고/비광고" and "ad_id" in items.columns:
            detail = _detail_table_today_base(items, today, compare_date, "ad_id", "광고 id", top_n)
            out.append({"factor": name, "iv": iv, "summary_table": summary_sales, "detail_table": detail})
        elif key == "환불액":
            summary, detail = _refund_detail_top5(adj, today, compare_date, top_n)
            out.append({"factor": name, "iv": iv, "summary_table": summary, "detail_table": detail})
        elif key == "쿠폰비용":
            summary, detail = _coupon_detail_top5(items, today, compare_date, top_n)
            out.append({"factor": name, "iv": iv, "summary_table": summary, "detail_table": detail})
    return out


def build_components_for_llm(
    key_metric_df: pd.DataFrame,
    iv_ranking: dict[str, Any],
    high_iv_tables: list[dict[str, Any]],
    threshold: float = 20,
) -> dict[str, Any]:
    """IV 20 초과 요인 + 요약/상세 테이블 2벌만 LLM 리포트용으로 정리."""
    ranking = iv_ranking.get("ranking", [])
    high_ranking = [{"요인": name, "IV": iv} for name, iv in ranking if iv > threshold]
    def _records(df: Optional[pd.DataFrame]) -> list:
        if df is None or df.empty:
            return []
        return df.to_dict(orient="records")
    full_ranking = [{"요인": name, "IV": iv} for name, iv in ranking]
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
        row_today = key_metric_df[key_metric_df["구분"] == "오늘"].iloc[0]
        row_base = key_metric_df[key_metric_df["구분"] == "기준일"].iloc[0]
        for col in ["총매출", "총비용", "순이익"]:
            if col in row_today and col in row_base:
                a, b = row_today[col], row_base[col]
                pct = ((a - b) / b * 100) if b != 0 else 0
                components["증감_요약"][col] = {"오늘": a, "기준일": b, "증감_pct": round(pct, 1)}
    return components
