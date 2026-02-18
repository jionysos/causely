# core.py
import os
import json
from datetime import date, timedelta
from typing import Optional

import pandas as pd
from openai import OpenAI


def _first_day(d: date) -> date:
    return d.replace(day=1)


def _last_day_of_month(d: date) -> date:
    """다음 달 1일 - 1일 = 이번 달 마지막 날."""
    next_month = d.replace(day=28) + timedelta(days=4)
    return next_month.replace(day=1) - timedelta(days=1)


def get_monthly_sales_series(
    today: date,
    items: pd.DataFrame,
    adj: pd.DataFrame,
) -> dict:
    """
    이번 달·지난달 일별 매출 및 누적 매출 계산 (today 기준, 하드코딩 없음).
    매출 = order_items net_sales_amount + adjustments amount (환불 등).
    반환: this_month / last_month 각각 daily, cumulative 리스트 (일자 1~말일, 금액).
    """
    if "order_ts" not in items.columns or "net_sales_amount" not in items.columns:
        raise ValueError("order_items에 order_ts, net_sales_amount 컬럼이 필요합니다.")
    if "event_ts" not in adj.columns or "amount" not in adj.columns:
        raise ValueError("adjustments에 event_ts, amount 컬럼이 필요합니다.")

    items = items.copy()
    adj = adj.copy()
    items["d"] = _to_day(items["order_ts"])
    adj["d"] = _to_day(adj["event_ts"])

    def daily_net(df_items: pd.DataFrame, df_adj: pd.DataFrame, day: date) -> float:
        g = float(df_items.loc[df_items["d"] == day, "net_sales_amount"].sum())
        r = float(df_adj.loc[df_adj["d"] == day, "amount"].sum())
        return g + r

    this_start = _first_day(today)
    this_end = min(today, _last_day_of_month(today))
    last_end = this_start - timedelta(days=1)
    last_start = _first_day(last_end)

    def series_for_range(start: date, end: date):
        days = []
        daily = []
        cum = 0.0
        cumulative = []
        d = start
        while d <= end:
            amt = daily_net(items, adj, d)
            days.append(d.day)
            daily.append(amt)
            cum += amt
            cumulative.append(cum)
            d += timedelta(days=1)
        return {"days": days, "daily": daily, "cumulative": cumulative}

    this_series = series_for_range(this_start, this_end)
    last_series = series_for_range(last_start, last_end)

    return {
        "this_month": this_series,
        "last_month": last_series,
    }


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


def get_comparison_kpis(
    today: date,
    n_days: int,
    items: pd.DataFrame,
    adj: pd.DataFrame,
) -> dict:
    """
    D-0(오늘) vs D-n(비교일) 일별 KPI 계산.
    n_days: 1, 7, 28 등. 비교일 = today - n_days.
    반환: base_date, compare_date, n_days, kpis { net, gross, refund, marketing } 각각
    current, compare, delta, pct.
    """
    compare_date = today - timedelta(days=n_days)
    items = items.copy()
    adj = adj.copy()
    items["d"] = _to_day(items["order_ts"])
    adj["d"] = _to_day(adj["event_ts"])

    def _sum_items(df: pd.DataFrame, d: date, col: str = "net_sales_amount") -> float:
        return float(df.loc[df["d"] == d, col].sum())

    def _sum_adj(df: pd.DataFrame, d: date) -> float:
        return float(df.loc[df["d"] == d, "amount"].sum())

    gross_current = _sum_items(items, today)
    gross_compare = _sum_items(items, compare_date)
    refund_current = _sum_adj(adj, today)
    refund_compare = _sum_adj(adj, compare_date)
    net_current = gross_current + refund_current
    net_compare = gross_compare + refund_compare

    # 마케팅매출: 인플루언서 등 (influencer_id가 있는 주문)
    influencer_col = "influencer_id"
    if influencer_col in items.columns:
        it = items[items[influencer_col].notna() & (items[influencer_col].astype(str).str.strip() != "")]
        m_current = float(it.loc[it["d"] == today, "net_sales_amount"].sum())
        m_compare = float(it.loc[it["d"] == compare_date, "net_sales_amount"].sum())
    else:
        m_current = m_compare = 0.0

    def _row(current: float, compare: float) -> dict:
        delta = current - compare
        if compare != 0:
            pct = round((delta / abs(compare)) * 100, 1)
        else:
            pct = 100.0 if delta > 0 else (0.0 if delta == 0 else -100.0)
        return {"current": current, "compare": compare, "delta": delta, "pct": pct}

    return {
        "base_date": str(today),
        "compare_date": str(compare_date),
        "n_days": n_days,
        "kpis": {
            "net": _row(net_current, net_compare),
            "gross": _row(gross_current, gross_compare),
            "refund": _row(refund_current, refund_compare),
            "marketing": _row(m_current, m_compare),
        },
    }


def get_top_three_metrics(
    today: date,
    n_days: int,
    items: pd.DataFrame,
    adj: pd.DataFrame,
) -> dict:
    """
    상단 3지표: 매출(총매출), 비용(환불 절대값+쿠폰), 손익비율(이익/비용).
    각 current, compare, delta, pct 반환.
    """
    compare_date = today - timedelta(days=n_days)
    items = items.copy()
    adj = adj.copy()
    items["d"] = _to_day(items["order_ts"])
    adj["d"] = _to_day(adj["event_ts"])

    def _gross(d: date) -> float:
        return float(items.loc[items["d"] == d, "net_sales_amount"].sum())

    def _refund(d: date) -> float:
        return float(adj.loc[adj["d"] == d, "amount"].sum())

    def _coupon(d: date) -> float:
        if "discount_amount" not in items.columns:
            return 0.0
        return float(items.loc[items["d"] == d, "discount_amount"].sum())

    매출_cur = _gross(today)
    매출_cmp = _gross(compare_date)
    refund_cur = _refund(today)
    refund_cmp = _refund(compare_date)
    coupon_cur = _coupon(today)
    coupon_cmp = _coupon(compare_date)
    비용_cur = abs(refund_cur) + coupon_cur
    비용_cmp = abs(refund_cmp) + coupon_cmp
    이익_cur = 매출_cur - 비용_cur
    이익_cmp = 매출_cmp - 비용_cmp
    손익비율_cur = (이익_cur / 비용_cur * 100) if 비용_cur != 0 else 0.0
    손익비율_cmp = (이익_cmp / 비용_cmp * 100) if 비용_cmp != 0 else 0.0

    def _row(c: float, p: float) -> dict:
        d = c - p
        pct = round((d / p) * 100, 1) if p != 0 else (100.0 if d > 0 else 0.0)
        return {"current": c, "compare": p, "delta": d, "pct": pct}

    return {
        "base_date": str(today),
        "compare_date": str(compare_date),
        "n_days": n_days,
        "매출": _row(매출_cur, 매출_cmp),
        "비용": _row(비용_cur, 비용_cmp),
        "손익비율": _row(손익비율_cur, 손익비율_cmp),
    }


def get_sales_decomposition(
    today: date,
    n_days: int,
    items: pd.DataFrame,
    orders: pd.DataFrame,
) -> dict:
    """
    매출 = 유입량(주문수) × 전환율(1) × 객단가 로 쪼개서,
    전체 매출 변동에 대한 기여도 분석. (유입량은 주문수로 근사, 전환율=1)
    반환: current/compare for revenue, order_count, aov; contrib_orders, contrib_aov;
    main_driver ("주문수" | "객단가"), main_driver_contrib_pct.
    """
    compare_date = today - timedelta(days=n_days)
    items = items.copy()
    orders = orders.copy()
    items["d"] = _to_day(items["order_ts"])
    if "order_ts" in orders.columns and orders["order_ts"].notna().any():
        orders["d"] = _to_day(orders["order_ts"])
    elif "order_id" in orders.columns and "order_id" in items.columns:
        order_dates = items.groupby("order_id")["d"].first()
        orders["d"] = orders["order_id"].map(order_dates)
    else:
        orders["d"] = pd.NaT

    def _revenue(d: date) -> float:
        return float(items.loc[items["d"] == d, "net_sales_amount"].sum())

    def _order_count(d: date) -> float:
        return float(orders.loc[orders["d"] == d, "order_id"].nunique())

    def _items_count(d: date) -> float:
        return float((items["d"] == d).sum())

    r0 = _revenue(compare_date)
    r1 = _revenue(today)
    n0 = _order_count(compare_date)
    n1 = _order_count(today)
    i0 = _items_count(compare_date)
    i1 = _items_count(today)
    if n0 == 0:
        aov0 = 0.0
        conv0 = 0.0
    else:
        aov0 = r0 / n0
        conv0 = i0 / n0
    if n1 == 0:
        aov1 = 0.0
        conv1 = 0.0
    else:
        aov1 = r1 / n1
        conv1 = i1 / n1

    delta_r = r1 - r0
    contrib_aov = n0 * (aov1 - aov0)
    contrib_orders = (n1 - n0) * aov1
    if abs(delta_r) < 1e-9:
        main_driver = "동일"
        main_driver_contrib_pct = 0.0
    else:
        if abs(contrib_orders) >= abs(contrib_aov):
            main_driver = "주문수"
            main_driver_contrib_pct = round((contrib_orders / delta_r) * 100, 1)
        else:
            main_driver = "객단가"
            main_driver_contrib_pct = round((contrib_aov / delta_r) * 100, 1)

    def _row(c: float, p: float) -> dict:
        d = c - p
        pct = round((d / p) * 100, 1) if p != 0 else (100.0 if d > 0 else 0.0)
        return {"current": c, "compare": p, "delta": d, "pct": pct}

    return {
        "base_date": str(today),
        "compare_date": str(compare_date),
        "n_days": n_days,
        "revenue": {"current": r1, "compare": r0, "delta": delta_r},
        "order_count": {"current": n1, "compare": n0, "delta": n1 - n0},
        "aov": {"current": aov1, "compare": aov0, "delta": aov1 - aov0},
        "conversion": _row(conv1, conv0),
        "유입량": _row(n1, n0),
        "전환율": _row(conv1, conv0),
        "객단가": _row(aov1, aov0),
        "contrib_orders": contrib_orders,
        "contrib_aov": contrib_aov,
        "main_driver": main_driver,
        "main_driver_contrib_pct": main_driver_contrib_pct,
    }


def get_sales_narrative(decomp: dict) -> str:
    """
    매출 분해 결과로 한 문장 내러티브.
    예: "매출은 5% 올랐지만, 전환율이 10% 급락했습니다. 유입량이 20% 폭증해서..."
    """
    r = decomp["revenue"]
    rev_pct = round((r["delta"] / r["compare"]) * 100, 1) if r["compare"] != 0 else 0
    유입 = decomp["유입량"]
    전환 = decomp["전환율"]
    객단가 = decomp["객단가"]
    main = decomp["main_driver"]
    if decomp["main_driver"] == "동일":
        return "매출과 구성 지표가 비교일과 동일합니다."
    rev_up = rev_pct > 0
    conv_drop = 전환["pct"] < -5
    inflow_surge = 유입["pct"] > 10
    parts = []
    parts.append(f"매출은 {rev_pct:+.1f}% {'올랐습니다' if rev_up else '내렸습니다'}")
    if conv_drop:
        parts.append(f"전환율(주문당 상품수)이 {전환['pct']:+.1f}% 급락했습니다")
    if inflow_surge and conv_drop:
        parts.append(f"유입량(주문수)이 {유입['pct']:+.1f}% 늘어나 매출 하락을 겨우 막고 있는 위험한 상황입니다")
    elif inflow_surge:
        parts.append(f"유입량이 {유입['pct']:+.1f}% 폭증했습니다")
    if 객단가["pct"] <= -10:
        parts.append(f"객단가가 {객단가['pct']:+.1f}% 하락했습니다")
    if not parts[1:]:
        parts.append(f"가장 큰 요인은 **{main}**입니다 (기여도 약 {abs(decomp['main_driver_contrib_pct']):.1f}%)")
    return "사장님, " + ". ".join(parts) + "."


def get_14day_series(
    today: date,
    items: pd.DataFrame,
    orders: pd.DataFrame,
    metric: str,
) -> list:
    """
    최근 14일 일별 시계열. metric: "order_count" | "aov" | "conversion"
    반환: [{"date": str, "value": float}, ...] (과거→오늘 순).
    """
    items = items.copy()
    orders = orders.copy()
    items["d"] = _to_day(items["order_ts"])
    if "order_ts" in orders.columns and orders["order_ts"].notna().any():
        orders["d"] = _to_day(orders["order_ts"])
    elif "order_id" in orders.columns and "order_id" in items.columns:
        order_dates = items.groupby("order_id")["d"].first()
        orders["d"] = orders["order_id"].map(order_dates)
    else:
        orders["d"] = pd.NaT

    start = today - timedelta(days=13)
    out = []
    for i in range(14):
        d = start + timedelta(days=i)
        rev = float(items.loc[items["d"] == d, "net_sales_amount"].sum())
        n = float(orders.loc[orders["d"] == d, "order_id"].nunique())
        cnt = float((items["d"] == d).sum())
        if metric == "order_count":
            val = n
        elif metric == "aov":
            val = rev / n if n else 0.0
        else:
            val = cnt / n if n else 0.0
        out.append({"date": str(d), "value": round(val, 2)})
    return out


def get_worst_dropped_metric(decomp: dict) -> str:
    """기여도/전환율 중 가장 크게 떨어진 지표 키 (14일 차트용)."""
    유입 = decomp["유입량"]
    전환 = decomp["전환율"]
    객단가 = decomp["객단가"]
    candidates = [
        ("order_count", 유입["pct"]),
        ("conversion", 전환["pct"]),
        ("aov", 객단가["pct"]),
    ]
    worst = min(candidates, key=lambda x: x[1])
    return worst[0]


def get_focus_summary(
    today: date,
    n_days: int,
    items: pd.DataFrame,
    adj: pd.DataFrame,
    products: pd.DataFrame,
    orders: pd.DataFrame,
) -> dict:
    """
    전일 대비 변동폭이 큰 상위 3개 상품, 상위 2개 채널(인플루언서)만 요약.
    '사장님, 여기만 보세요'용.
    """
    compare_date = today - timedelta(days=n_days)
    items = items.copy()
    items["d"] = _to_day(items["order_ts"])

    # 상품별 매출 (order_items에 product_id 있으면)
    top_3_products = []
    if "product_id" in items.columns:
        g_today = items[items["d"] == today].groupby("product_id")["net_sales_amount"].sum()
        g_compare = items[items["d"] == compare_date].groupby("product_id")["net_sales_amount"].sum()
        idx = sorted(set(g_today.index) | set(g_compare.index))
        delta = (g_today.reindex(idx, fill_value=0) - g_compare.reindex(idx, fill_value=0)).reindex(idx, fill_value=0)
        delta = delta.sort_values(ascending=True)
        # 변동폭 큰 순: 절대값 기준 상위 3
        by_abs = delta.reindex(delta.abs().sort_values(ascending=False).index)
        for pid in by_abs.head(3).index:
            cur = float(g_today.reindex([pid], fill_value=0).iloc[0])
            cmp = float(g_compare.reindex([pid], fill_value=0).iloc[0])
            d = float(delta.reindex([pid], fill_value=0).iloc[0])
            pct = round((d / cmp) * 100, 1) if cmp != 0 else (100.0 if d > 0 else 0.0)
            name = pid
            if products is not None and "product_id" in products.columns and "product_name" in products.columns:
                p = products[products["product_id"] == pid]
                if len(p):
                    name = p.iloc[0].get("product_name", pid)
            top_3_products.append({"product_id": pid, "name": name, "current": cur, "compare": cmp, "delta": d, "pct": pct})

    # 채널(인플루언서)별 매출, 상위 2개
    top_2_channels = []
    influencer_col = "influencer_id"
    if influencer_col in items.columns:
        it = items.copy()
        it[influencer_col] = it[influencer_col].fillna("NONE")
        g_today = it[it["d"] == today].groupby(influencer_col)["net_sales_amount"].sum()
        g_compare = it[it["d"] == compare_date].groupby(influencer_col)["net_sales_amount"].sum()
        idx = sorted(set(g_today.index) | set(g_compare.index))
        delta = (g_today.reindex(idx, fill_value=0) - g_compare.reindex(idx, fill_value=0)).reindex(idx, fill_value=0)
        delta = delta[delta.index != "NONE"].sort_values(ascending=False)
        by_abs = delta.reindex(delta.abs().sort_values(ascending=False).index)
        for ch in by_abs.head(2).index:
            cur = float(g_today.reindex([ch], fill_value=0).iloc[0])
            cmp = float(g_compare.reindex([ch], fill_value=0).iloc[0])
            d = float(delta.reindex([ch], fill_value=0).iloc[0])
            pct = round((d / cmp) * 100, 1) if cmp != 0 else (100.0 if d > 0 else 0.0)
            top_2_channels.append({"channel": str(ch), "current": cur, "compare": cmp, "delta": d, "pct": pct})

    return {"top_3_products": top_3_products, "top_2_channels": top_2_channels}


def get_cause_summary(kpis: dict) -> str:
    """
    순매출 변동의 핵심 원인 한 문장.
    하락 시: 가장 크게 기여한 하위 요인(총매출/환불)과 증감률.
    상승 시: 가장 크게 기여한 요인과 증감률.
    """
    net = kpis["kpis"]["net"]
    gross = kpis["kpis"]["gross"]
    refund = kpis["kpis"]["refund"]
    net_delta = net["delta"]
    gross_delta = gross["delta"]
    refund_delta = refund["delta"]

    if net_delta == 0:
        return "사장님, 오늘 순매출은 비교 기간과 동일합니다."

    if net_delta < 0:
        # 순매출 하락: 총매출 하락 vs 환불 증가(음수 확대) 중 더 큰 기여
        if abs(gross_delta) >= abs(refund_delta):
            factor, pct = "총매출", gross["pct"]
            return f"사장님, 오늘 순매출 하락의 핵심 원인은 **총매출**의 {pct:+.1f}% 하락 때문입니다."
        else:
            factor, pct = "환불", refund["pct"]
            return f"사장님, 오늘 순매출 하락의 핵심 원인은 **환불**의 {abs(pct):.1f}% 증가 때문입니다."
    else:
        if abs(gross_delta) >= abs(refund_delta):
            pct = gross["pct"]
            return f"사장님, 오늘 순매출 상승의 핵심 요인은 **총매출**의 {pct:+.1f}% 증가 때문입니다."
        else:
            pct = refund["pct"]
            return f"사장님, 오늘 순매출 상승의 핵심 요인은 **환불**의 {abs(pct):.1f}% 감소 때문입니다."


def build_evidence(
    today: date,
    orders: pd.DataFrame,
    items: pd.DataFrame,
    adj: pd.DataFrame,
    products: pd.DataFrame,
    compare_date: Optional[date] = None,
) -> dict:
    """
    Evidence packet 생성: KPI 및 드라이버.
    compare_date가 None이면 전일(today-1), 아니면 해당 비교일 사용.
    """
    yday = (today - timedelta(days=1)) if compare_date is None else compare_date

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
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "finding": {"type": "string"},
                        "supporting_data": {
                            "oneOf": [
                                {"type": "object", "additionalProperties": True},
                                {"type": "array", "items": {"type": "object", "additionalProperties": True}},
                            ]
                        },
                    },
                    "required": ["finding"],
                },
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
        "Respond entirely in Korean (headline, key_findings, actions). "
        "Return ONLY valid JSON. No markdown, no extra text."
    )

    user = {
        "task": "한글로 일일 브리핑과 액션 플랜을 작성하세요. 각 key_finding마다 그 근거가 되는 evidence를 정형 데이터로 요약해 supporting_data에 넣어 주세요.",
        "output_schema": {
            "headline": "string (한글)",
            "key_findings": [
                {
                    "finding": "string (한글, 3~5개)",
                    "supporting_data": "object 또는 object[] — 해당 finding의 근거가 되는 수치/데이터. 표로 보여줄 수 있게 키-값 객체 하나 또는 행 배열로 요약. 컬럼명은 반드시 '기준일'(비교일 값) 사용. 예: {\"구분\":\"순매출\", \"오늘\":1150, \"기준일\":1000, \"증감\":150} 또는 [{\"인플루언서\":\"A\", \"매출증가\":100}, ...]"
                }
            ],
            "actions": [
                {"title": "string (한글)", "why": "string (한글)", "expected_impact": "string (한글, optional)"}
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


def generate_iv_report(components: dict, model: str = "gpt-4o-mini") -> dict:
    """
    IV 기반 차이 분석 구성요소를 LLM에 보내 리포트 형식으로 생성.
    components: report_tables.build_components_for_llm() 반환값 (IV 20 초과 요인·상세 테이블 포함).
    """
    client = _client()
    system = (
        "You are an operations analyst for an ecommerce CEO. "
        "Use ONLY the provided data. Do not invent numbers. "
        "Respond in Korean. Return ONLY valid JSON: { \"headline\": string, \"sections\": [ {\"title\": string, \"body\": string} ] }."
    )
    user = {
        "task": (
            "아래 데이터는 오늘 vs 기준일 비교 결과입니다. 반드시 다음 구조로 서술해 주세요.\n"
            "1) 총매출 변동 요약: 총매출이 얼마나 변했는지, 그 원인이 **매출 자체의 증가/감소**인지 **비용의 증가/감소**인지 판단해 서술.\n"
            "2) 기여 요인 순서: 제공된 'IV_20_이상_요인_순'과 'IV_20_이상_상세_테이블'만 사용해, 총매출·순이익 차이에 **가장 크게 기여한 요소**를 IV 내림차순으로 서술.\n"
            "3) 보완·강화 제안: 위 분석을 바탕으로, 문제 해결 또는 매출 증진을 위해 **무엇을 보완하고 무엇을 강화**해야 하는지 구체적으로 서술.\n"
            "headline은 한 문장으로 요약. sections에는 위 1~3에 대응하는 3개 섹션(title + body)을 넣어 주세요."
        ),
        "data": components,
    }
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
        temperature=0.2,
    )
    text = resp.choices[0].message.content.strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end <= start:
        return {"headline": text[:500], "sections": []}
    return json.loads(text[start : end + 1])
