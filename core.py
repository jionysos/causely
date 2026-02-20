# core.py
import os
import json
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple
# core.py에 추가할 것 1: SQLite 로드 + SQL 실행
import sqlite3

import pandas as pd
from openai import OpenAI
from woe_iv import woe_iv

# core.py에 추가할 것 1: SQLite 로드 + SQL 실행
import sqlite3

def _load_sqlite(orders, items, adj, products) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    for name, df in [("orders", orders), ("order_items", items), 
                     ("adjustments", adj), ("products", products)]:
        if df is not None and not df.empty:
            df.to_sql(name, conn, index=False, if_exists="replace")
    return conn

def _get_schema(conn: sqlite3.Connection) -> str:
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cursor.fetchall()]
    lines = []
    for t in tables:
        cursor.execute(f"PRAGMA table_info({t})")
        cols = ", ".join(f"{c[1]}({c[2]})" for c in cursor.fetchall())
        cursor.execute(f"SELECT * FROM {t} LIMIT 1")
        sample = cursor.fetchone()
        lines.append(f"TABLE {t}: {cols}")
        if sample:
            lines.append(f"  SAMPLE: {sample}")
    return "\n".join(lines)

def _text_to_sql(question: str, schema: str, model: str = "gpt-4o") -> str:
    client = _client()
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": 
             "Return ONLY a SQLite SQL query. No explanation. No markdown.\n"
             "Business context:\n"
             "- order_items.net_sales_amount: 판매금액\n"
             "- adjustments.amount: 환불금액(음수)\n"
             "- adjustments.reason_code: DEFECT/SIZE/CHANGE_MIND/DELIVERY\n"
             "- order_items.influencer_id: 인플루언서 ID (NULL=일반구매)\n"
             "- order_items.coupon_id: 쿠폰 ID (NULL=미사용)\n"
             "- products.seller_id: 셀러 ID"},
            {"role": "user", "content": f"Schema:\n{schema}\n\nQuestion: {question}"}
        ],
        temperature=0
    )
    sql = resp.choices[0].message.content.strip()
    return sql.replace("```sql", "").replace("```", "").strip()

def _context_cell(k: str, v: Any) -> str:
    """Human-readable cell: numbers with thousands sep, rest as-is."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return f"{k}: "
    if isinstance(v, (int, float)):
        return f"{k}: {v:,}" if isinstance(v, int) or v == int(v) else f"{k}: {v:,.2f}"
    try:
        n = float(v)
        return f"{k}: {n:,.0f}" if n == int(n) else f"{k}: {n:,.2f}"
    except (TypeError, ValueError):
        pass
    return f"{k}: {v}"


def build_llm_context(components: Dict[str, Any], today=None, compare_date=None) -> str:
    lines = []
    증감 = components.get("증감_요약", {})
    summary_lines = []

    # 0-0) 분석 기준일 — 가장 먼저 명시
    if today or compare_date:
        lines.append("## [분석 기준]")
        lines.append(f"- 기준일(오늘): {today}")
        lines.append(f"- 비교일(기준일): {compare_date}")
        lines.append("※ 모든 분석은 위 기준일 데이터 기준. 다른 날짜 추정 금지.")
        lines.append("")

    # 0) KEY NUMBERS — 본문에 반드시 인용할 수치 (맨 앞에 배치)
    lines.append("## [필수] KEY NUMBERS — 아래 수치를 본문에 반드시 넣어라 (없으면 리포트 실격)")
    key_nums = []
    for col in ["총매출", "총비용", "순이익"]:
        v = 증감.get(col)
        if isinstance(v, dict) and "오늘" in v and "기준일" in v:
            pct = v.get("증감_pct", 0)
            방향 = "증가" if pct > 0 else "감소" if pct < 0 else "동일"
            key_nums.append(f"{col} 오늘 {v['오늘']:,}원 기준일 {v['기준일']:,}원 ({pct:+.1f}% {방향})")
            summary_lines.append((col, 방향, pct))
    lines.append(" | ".join(key_nums))
    iv_top = components.get("IV_20_이상_요인_순", [])[:5]
    if iv_top:
        lines.append("IV 상위 요인(반드시 분석에 사용): " + ", ".join(f"{r.get('요인','')}(IV {r.get('IV',0):.1f})" for r in iv_top))
    lines.append("")

    # 1) 총매출·총비용·순이익 변화 + 맥락 해석
    lines.append("## 총매출·총비용·순이익 변화 (오늘 vs 기준일)")
    for col in ["총매출", "총비용", "순이익"]:
        v = 증감.get(col)
        if isinstance(v, dict) and "오늘" in v and "기준일" in v:
            pct = v.get("증감_pct", 0)
            방향 = "증가" if pct > 0 else "감소" if pct < 0 else "동일"
            lines.append(f"- {col}: 오늘 {v['오늘']:,}원, 기준일 {v['기준일']:,}원 → {방향} ({pct:+.1f}%)")

    # 주목 패턴: 매출↑ 순이익↓ / 매출↓ 순이익↑ 등 → 심도 분석 필수
    매출방향 = next((s[1] for s in summary_lines if s[0] == "총매출"), None)
    이익방향 = next((s[1] for s in summary_lines if s[0] == "순이익"), None)
    if 매출방향 == "증가" and 이익방향 == "감소":
        lines.append("⚠️ 주목 패턴: 매출은 증가했으나 순이익이 감소 → 비용·환불 요인 집중 분석 필요 (반드시 심도 분석)")
    elif 매출방향 == "감소" and 이익방향 == "증가":
        lines.append("⚠️ 주목 패턴: 매출은 감소했으나 순이익이 증가 → 비용 절감·효율화 효과 가능성 (반드시 심도 분석)")
    elif 매출방향 == "동일" and 이익방향 == "감소":
        lines.append("⚠️ 주목 패턴: 매출은 안정이나 순이익 감소 → 내부 상쇄 요인 존재 가능성 (반드시 심도 분석)")
    lines.append("※ 위와 같이 '매출↑ 순이익↓' 또는 '매출↓ 순이익↑' 등 주목 패턴이 있으면 리포트에서 반드시 심도 있게 분석할 것.")
    lines.append("")

    # 2) IV 기여도 전체 순위
    lines.append("## IV 기여도 (전체 순위) — IV가 높을수록 오늘 변화를 더 많이 설명하는 요인")
    iv_순위 = components.get("IV_전체_순위", components.get("IV_20_이상_요인_순", []))
    for r in iv_순위:
        name = r.get("요인", r.get("name", ""))
        iv = r.get("IV", r.get("iv", 0))
        방향힌트 = ""
        if "비용" in name or "환불" in name:
            방향힌트 = "(비용/환불 계열: 높으면 지출 변화가 큰 것)"
        elif "매출" in name or "인플루언서" in name:
            방향힌트 = "(매출 계열: 높으면 수익 변화가 큰 것)"
        lines.append(f"- [IV {iv:.1f}] {name} {방향힌트}")
    lines.append("")

    # 3) IV 20 초과 요인 상세 — 수치 변화의 의미를 같이 서술
    lines.append("## IV 20 초과 요인 상세 (전부)")
    for t in components.get("IV_20_이상_상세_테이블", []):
        factor = t.get("factor", "")
        iv = t.get("iv", 0)
        lines.append(f"### 요인: {factor} (IV {iv:.1f})")

        summary = t.get("summary", [])
        detail = t.get("detail", [])

        if summary:
            lines.append("  [요약표] — 오늘 vs 기준일 수치 변화")
            for row in summary:
                if isinstance(row, dict):
                    row_str = " | ".join(
                        _context_cell(k, v) for k, v in row.items()
                    )
                    lines.append(f"    {row_str}")
                else:
                    lines.append(f"    {row}")

            # 요약에서 오늘/기준일 수치 추출해서 배율 자동 계산
            try:
                row = summary[-1] if summary else {}
                gen_t = (
                    abs(float(str(v).replace(",", "")))
                    for k, v in (row.items() if isinstance(row, dict) else {}.items())
                    if "오늘" in str(k) and v not in [None, 0, ""]
                )
                gen_b = (
                    abs(float(str(v).replace(",", "")))
                    for k, v in (row.items() if isinstance(row, dict) else {}.items())
                    if "기준" in str(k) and v not in [None, 0, ""]
                )
                오늘값 = next(gen_t, None)
                기준값 = next(gen_b, None)
                if 오늘값 and 기준값 and 기준값 > 0:
                    배율 = 오늘값 / 기준값
                    lines.append(f"  → 오늘 수치가 기준일 대비 {배율:.1f}배 수준")
            except Exception:
                pass

        if detail:
            lines.append("  [상세표 Top5] — 가장 큰 영향을 준 세부 항목")
            for row in detail:
                if isinstance(row, dict):
                    row_str = " | ".join(
                        _context_cell(k, v) for k, v in row.items()
                    )
                    lines.append(f"    {row_str}")
                else:
                    lines.append(f"    {row}")
        lines.append("")
    
    # 4) 상쇄 패턴 + 인과관계 자동 감지
    lines.append("## 🔍 자동 감지된 인과관계 — 반드시 리포트에 포함할 것")
    try:
        환불_block = next((t for t in components.get("IV_20_이상_상세_테이블", [])
                           if "환불" in t.get("factor", "")), None)
        매출_block = next((t for t in components.get("IV_20_이상_상세_테이블", [])
                           if "인플루언서" in t.get("factor", "")), None)

        if 환불_block and 매출_block:
            # 오늘 환불 최대 상품
            top_환불 = next(
                (r for r in 환불_block.get("detail", [])
                 if isinstance(r, dict) and abs(float(str(r.get("오늘자 환불액", 0)).replace(",", "") or 0)) > 0),
                None
            )
            # 오늘 인플루언서 기여 최대
            top_inf = next(
                (r for r in 매출_block.get("detail", [])
                 if isinstance(r, dict) and str(r.get("인플루언서 id", "")).strip() not in ["", "None", "nan"]),
                None
            )

            if top_환불 and top_inf:
                환불금 = abs(float(str(top_환불.get("오늘자 환불액", 0)).replace(",", "")))
                매출금 = abs(float(str(top_inf.get("오늘자 매출", 0)).replace(",", "")))
                pid = top_환불.get("환불상품 id", "")
                inf_id = str(top_inf.get("인플루언서 id", "")).strip()

                lines.append(f"- 환불 핵심 상품: {pid} → 오늘 -{환불금:,.0f}원 (기준일 대비 신규 발생)")
                lines.append(f"- 매출 핵심 기여: {inf_id} → 오늘 +{매출금:,.0f}원 (기준일 0원에서 신규 발생)")

                if 환불금 > 0 and 매출금 > 0 and min(환불금, 매출금) / max(환불금, 매출금) > 0.7:
                    차이 = abs(환불금 - 매출금)
                    lines.append(
                        f"⚠️ 상쇄 구조 감지: {pid} 환불 -{환불금:,.0f}원 과 {inf_id} 기여매출 +{매출금:,.0f}원 의 차이가 {차이:,.0f}원으로 거의 동일."
                    )
                    lines.append(
                        f"   → 표면 순이익은 안정적으로 보이지만, 실제로는 {pid} 환불 문제가 {inf_id} 매출로 가려진 구조."
                    )
                    lines.append(
                        f"   → 리포트에 반드시: '표면상 순이익 변화 없어 보이지만, {pid} 환불(-{환불금:,.0f}원)을 {inf_id}(+{매출금:,.0f}원)가 상쇄하고 있는 구조. {inf_id} 효과가 사라지면 즉시 -{환불금:,.0f}원 손실 노출' 명시."
                    )
    except Exception:
        pass

    # 상품(상품명) IV 상세가 있으면 반드시 리포트에 포함
    상품_block = next((t for t in components.get("IV_20_이상_상세_테이블", [])
                       if "상품" in t.get("factor", "")), None)
    if 상품_block and 상품_block.get("detail"):
        lines.append("⚠️ 상품(상품명) IV 상세가 데이터에 있음 → KPI 변화 핵심원인 분석 본문에 반드시 상품별 기여 내용(상품명, 오늘자/기준일 매출)을 수치와 함께 포함할 것.")

    lines.append("---")
    lines.append("위 KEY NUMBERS와 상세 표의 수치를 본문에 반드시 인용하여 리포트를 작성하라. 숫자 없이 일반론만 쓰면 실격.")

    return "\n".join(lines)

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


def compute_sales_strength_factors(
    items: pd.DataFrame,
    today: date,
    compare_date: date,
    min_count: int = 50,
    iv_threshold: float = 20,
) -> List[Dict[str, Any]]:
    """
    order_items 기준: 기준일 vs 비교기준일로 (1) product_id, (2) channel, (3) influencer_id 별
    order_items 건수(행 수) 집계 후 woe_iv로 구성비 차이를 IV로 계산. 건수 min_count 이상·IV iv_threshold 초과만 반환.
    """
    items = items.copy()
    items["d"] = _to_day(items["order_ts"])
    sub = items[items["d"].isin([today, compare_date])].copy()
    if sub.empty:
        return []
    dimension_config = [
        ("product_id", "상품"),
        ("channel", "채널"),
        ("influencer_id", "인플루언서"),
    ]
    out = []
    for dim_col, dim_label in dimension_config:
        if dim_col not in sub.columns:
            continue
        sub_dim = sub[[dim_col, "d"]].copy()
        sub_dim[dim_col] = sub_dim[dim_col].astype(str).replace("nan", "__NA__").replace("", "__NA__")
        cnt = sub_dim.groupby(dim_col, dropna=False).size()
        valid_vals = cnt[cnt >= min_count].index.tolist()
        if not valid_vals:
            continue
        sub_dim = sub_dim[sub_dim[dim_col].isin(valid_vals)]
        if len(sub_dim) < min_count:
            continue
        data = sub_dim[[dim_col]].copy()
        target = (sub_dim["d"] == today).astype(int)
        try:
            WoE, IV_df = woe_iv(data, target, bins=min(50, max(2, len(valid_vals))))
            if IV_df is None or IV_df.empty:
                continue
            total_iv = float(IV_df["IV"].iloc[0])
            if total_iv <= iv_threshold:
                continue
            woe_var = WoE[WoE["Var_name"] == dim_col].copy()
            woe_var = woe_var.sort_values("IV", ascending=False)
            detail = [
                {"value": str(row["Cut_off"]), "iv_contribution": round(float(row["IV"]), 2)}
                for _, row in woe_var.head(10).iterrows()
            ]
            out.append({
                "dimension": dim_col,
                "dimension_label": dim_label,
                "iv": round(total_iv, 2),
                "detail": detail,
            })
        except Exception:
            continue
    out.sort(key=lambda x: -x["iv"])
    return out


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


def generate_iv_report(components: Dict[str, Any], model: str = "gpt-4o", today=None, compare_date=None) -> dict:
    """
    IV 기반 차이 분석 구성요소를 LLM에 보내 리포트 형식으로 생성.
    components: report_tables.build_components_for_llm() 반환값.
    총매출·총비용·순이익 변화를 함께 분석하고, 매출/비용 방향에 따라 보완·강화 액션 플랜을 요청한다.
    """
    context = build_llm_context(components, today=today, compare_date=compare_date)

    prompt = f"""
아래 데이터만 사용해서 CEO용 IV 기반 리포트를 작성하라. 제공된 숫자 외의 수치는 사용 금지.

---
{context}
---

### 작성 형식

**KPI 변화 핵심원인 분석** — 각 문장에 반드시 위 KEY NUMBERS 또는 상세표의 수치가 들어가야 함.
- 좋은 예시(이렇게 써라): "환불액이 기준일 405,000원에서 오늘 1,444,000원으로 약 3.6배 증가(IV 309.5)했다. 코튼 브라렛 세트 환불 -1,444,000원이 신규 발생했고, INF_A 매출 +1,449,000원이 이를 상쇄하고 있다."
- 나쁜 예시(절대 금지): "방치 시 월 ~원 추가 손실 예상" 같은 데이터에 없는 수치 추정, "분석이 필요합니다", "검토하세요"

**액션 플랜** — 구체적 행동(WHAT)만. 시간(언제까지)·누구(팀/담당자) 명시 금지. 예: "코튼 브라렛 세트 환불 사유 확인"

**주목 패턴**(매출↑ 순이익↓ 또는 매출↓ 순이익↑)이 있으면 "표면상 안정적으로 보이지만 실제로는…"으로 인과를 명시할 것.

### 제출 전 체크
- [ ] 모든 문장에 데이터에 있는 수치만 사용했는가? (추정·외삽 금지)
- [ ] 상품(상품명) IV 상세가 데이터에 있으면 KPI 변화 핵심원인 분석에 반드시 상품별 기여(상품명, 수치) 포함?
- [ ] 모든 액션은 구체적 행동(WHAT)만. 시간·누구/팀 명시 금지
- [ ] "방치 시 월 ~원" 같은 데이터 외 추정 수치 없음?
- [ ] "분석이 필요합니다", "검토하세요" 같은 모호 표현 없음?

아래 JSON만 출력. 마크다운·설명 없이. sections는 4개: 종합, 비용 관점, 매출 관점, 우선순위별 액션 플랜.

각 섹션 정의:
- 종합: 총매출·총비용·순이익 전체 흐름 + 핵심 인과관계 요약 (비용/매출 양쪽 다 포함)
- 비용 관점: 환불·비용 증가 요인만 집중 분석 (IV 비용 계열 요인 상세)
- 매출 관점: 매출 증가·감소 요인만 집중 분석 (상품별·채널별·인플루언서 기여 상세)
- 우선순위별 액션 플랜: actions 배열에 1순위, 2순위 2개만

{{
  "headline": "한줄 요약 30자 이내 (수치 포함)",
  "sections": [
    {{ "title": "종합", "body": "총매출·순이익 전체 흐름 + 핵심 인과관계. 수치 반드시 포함." }},
    {{ "title": "비용 관점", "body": "환불·비용 관련 요인만. 어떤 상품/항목이 비용을 올렸는지 수치 포함." }},
    {{ "title": "매출 관점", "body": "매출 기여 요인만. 상품별·인플루언서·채널 기여를 수치 포함." }},
    {{
      "title": "우선순위별 액션 플랜",
      "body": "전체 액션 요약 문단",
      "actions": [
        {{ "label": "1순위", "action": "구체 액션 (WHAT만, 시간·누구 명시 금지)" }},
        {{ "label": "2순위", "action": "구체 액션" }}
      ]
    }}
  ]
}}
"""
    print("=== LLM에 들어가는 컨텍스트 ===")
    print(context)
    print("=== 끝 ===")

    client = _client()
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": """You are a senior Korean fashion e-commerce analyst writing for the CEO.

MANDATORY rules - violating any = bad report:
1. Every sentence must contain AT LEAST ONE specific number from the provided data only
2. Always state: what happened → why it matters → what to do
3. Actions must state WHAT only. Do NOT specify WHEN (time/deadline) or WHO (team/person)
4. Never use vague words like "분석이 필요합니다", "검토하세요" - give the actual answer
5. If two factors cancel each other out, explicitly say "표면상 안정적으로 보이지만 실제로는..."
6. STRICTLY FORBIDDEN: Do NOT extrapolate or estimate monthly/annual figures. Only use numbers from the provided data. No "방치 시 월 ~원" style projections.

Respond ONLY in valid JSON. Korean language.
""",
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.1,
    )
    text = resp.choices[0].message.content.strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end <= start:
        return {"headline": text[:500], "sections": []}
    report = json.loads(text[start : end + 1])
    return report


def build_db_context_for_qa(
    orders: Optional[pd.DataFrame] = None,
    items: Optional[pd.DataFrame] = None,
    adj: Optional[pd.DataFrame] = None,
    products: Optional[pd.DataFrame] = None,
    max_rows: int = 150,
) -> str:
    """
    질의응답 시 전체 DB를 훑어서 답할 수 있도록 테이블별 스키마 + 샘플 문자열 생성.
    예: 상품 P010을 파는 셀러는 products.csv의 seller_id에서 확인 가능하도록 포함.
    """
    lines = ["## 전체 DB 개요 (질의응답 시 이 데이터를 훑어서 답변할 것)"]

    def _sample(df: pd.DataFrame, name: str, cols: Optional[List[str]] = None, n: int = max_rows) -> None:
        if df is None or df.empty:
            return
        lines.append(f"### {name}")
        lines.append("컬럼: " + ", ".join(df.columns.tolist()))
        use = df[cols] if cols and all(c in df.columns for c in cols) else df
        lines.append(use.head(n).to_string(index=False))
        lines.append("")

    if products is not None and not products.empty:
        lines.append("### products (상품 정보). 예: 상품 P010을 파는 셀러 → product_id로 행을 찾고, seller_id 컬럼이 셀러 정보.")
        lines.append("컬럼: " + ", ".join(products.columns.tolist()))
        cols = [c for c in ["product_id", "seller_id", "product_name"] if c in products.columns]
        sub = products[cols] if cols else products.iloc[:, :6]
        lines.append(sub.head(max_rows).to_string(index=False))
        lines.append("")

    if orders is not None and not orders.empty:
        _sample(orders, "orders (주문)", n=max_rows)

    if items is not None and not items.empty:
        _sample(items, "order_items (주문별 상품/매출)", n=max_rows)

    if adj is not None and not adj.empty:
        _sample(adj, "adjustments (환불 등)", n=max_rows)

    return "\n".join(lines)

def answer_report_question(
    report, context, messages,
    orders=None, items=None, adj=None, products=None,
    db_context: str = "",
    model: str = "gpt-4o",  # ← mini → gpt-4o
) -> tuple:
    client = _client()
    df_result = None
    sql_result = ""
    if items is not None:
        try:
            conn = _load_sqlite(orders, items, adj, products)
            schema = _get_schema(conn)
            
            # products 전체를 schema에 추가 (셀러 정보 오답 방지)
            if products is not None:
                schema += f"\n\n## products 전체 데이터 (seller_id 확인용)\n{products.to_string(index=False)}"
            
            question = messages[-1]["content"]
            sql = _text_to_sql(question, schema)
            
            # SQL 실행
            df_result = pd.read_sql_query(sql, conn)
            
            if not df_result.empty:
                sql_result = (
                    f"\n## 실시간 DB 조회 결과"
                    f"\n실행 SQL: {sql}"
                    f"\n결과:\n{df_result.to_string(index=False)}"
                    f"\n※ 위 조회 결과가 사실이며, 이 숫자만 사용할 것. 다른 숫자 사용 금지."
                )
        except Exception as e:
            sql_result = f"\n## DB 조회 실패: {e}"

    system = (
        "You are a senior analyst for a Korean fashion e-commerce company.\n"
        "RULES:\n"
        "1. '실시간 DB 조회 결과'가 있으면 그 숫자만 사용해라. 절대 추측하지 마라.\n"
        "2. DB 조회 결과가 없으면 리포트와 분석 컨텍스트만 사용해라.\n"
        "3. 모르면 '데이터에서 확인되지 않습니다'라고 해라. 절대 만들어내지 마라.\n"
        "4. 한국어로 답변. 간결하고 액션 중심으로."
    )
    
    report_text = "## 리포트\n" + report.get("headline", "") + "\n\n"
    for s in report.get("sections", []):
        report_text += f"### {s.get('title','')}\n{s.get('body','')}\n\n"
    report_text += "\n## 분석 컨텍스트\n" + context
    report_text += sql_result

    api_messages = [{"role": "system", "content": system + "\n\n" + report_text}]
    for m in messages[-10:]:
        api_messages.append({"role": m["role"], "content": m["content"]})

    resp = client.chat.completions.create(model=model, messages=api_messages, temperature=0)
    reply = resp.choices[0].message.content.strip()
    return (reply, df_result)