import streamlit as st
import pandas as pd
from datetime import date, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import core
from metrics import build_default_registry, Context

st.set_page_config(layout="wide")
st.title("Causely — Upload data")

REQUIRED = {
    "orders.csv": "orders",
    "order_items.csv": "order_items",
    "adjustments.csv": "adjustments",
    "products.csv": "products",
    # 필요하면 추가:
    "users.csv": "users",
    "coupons.csv": "coupons",
    "ad_costs.csv": "ad_costs",
    "influencer_costs.csv": "influencer_costs"
}

def read_csv(uploaded_file):
    # 인코딩 문제 있으면 encoding="utf-8-sig" 또는 "cp949"로 바꿔
    return pd.read_csv(uploaded_file)

st.subheader("1) CSV 업로드 (여러 개 파일을 한 번에 드래그앤드롭)")
uploaded_files = st.file_uploader(
    "orders.csv, order_items.csv, adjustments.csv, products.csv 등을 한 번에 올리세요",
    type=["csv"],
    accept_multiple_files=True
)

if not uploaded_files:
    st.stop()

# 업로드 파일을 파일명으로 매칭
file_map = {f.name: f for f in uploaded_files}

missing = [fn for fn in REQUIRED.keys() if fn not in file_map]
if missing:
    st.error("필수 파일이 부족합니다: " + ", ".join(missing))
    st.stop()

# 로드
orders = read_csv(file_map["orders.csv"])
items = read_csv(file_map["order_items.csv"])
adj = read_csv(file_map["adjustments.csv"])
products = read_csv(file_map["products.csv"])

st.success("CSV loaded ✅")

# 기준일 커스터마이징 (프리셋 + 날짜 선택)
if "benchmark_date_input" not in st.session_state:
    st.session_state["benchmark_date_input"] = date(2026, 1, 31)
st.caption("기준일")
preset1, preset2, preset3, date_col, _ = st.columns([1, 1, 1, 1, 4])
with preset1:
    if st.button("오늘", use_container_width=True):
        st.session_state["benchmark_date_input"] = date.today()
with preset2:
    if st.button("어제", use_container_width=True):
        st.session_state["benchmark_date_input"] = date.today() - timedelta(days=1)
with preset3:
    if st.button("데모 (2026-01-31)", use_container_width=True):
        st.session_state["benchmark_date_input"] = date(2026, 1, 31)
with date_col:
    today = st.date_input(
        "기준일",
        value=st.session_state["benchmark_date_input"],
        key="benchmark_date_input",
        label_visibility="collapsed",
    )

# --- 일별·누적 매출 시각화 (Plotly) ---
st.subheader("월별 매출 시각화")
try:
    series = core.get_monthly_sales_series(today, items, adj)
    tm = series["this_month"]
    lm = series["last_month"]

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    # 이번 달 일별 매출: 진한 빨간색 막대 (투명도 적용해 뒤 누적선이 보이도록)
    fig.add_trace(
        go.Bar(
            x=tm["days"],
            y=tm["daily"],
            name="이번 달 일별 매출",
            marker=dict(color="darkred", opacity=0.45),
        ),
        secondary_y=True,
    )
    # 이번 달 누적 매출: 파란색 꺾은선
    fig.add_trace(
        go.Scatter(x=tm["days"], y=tm["cumulative"], name="이번 달 누적 매출", mode="lines+markers", line=dict(color="blue", width=2)),
        secondary_y=False,
    )
    # 지난달 누적 매출: 회색 꺾은선 (X축 일자 맞춤)
    fig.add_trace(
        go.Scatter(x=lm["days"], y=lm["cumulative"], name="지난달 누적 매출", mode="lines+markers", line=dict(color="gray", width=2)),
        secondary_y=False,
    )
    fig.update_layout(
        title="이번 달 vs 지난달 매출 (일자 기준 비교)",
        xaxis_title="일자",
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_yaxes(title_text="누적 매출", secondary_y=False)
    fig.update_yaxes(title_text="일별 매출", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)
except Exception as e:
    st.warning(f"매출 시각화를 그리지 못했습니다: {e}")

# Causely 분석 버튼 → 시나리오 1(상쇄 효과) 연동
if st.button("Causely 분석", type="primary"):
    st.session_state["run_causely_analysis"] = True

st.subheader("2) 리포트 생성")
if st.button("오늘 리포트 생성") or st.session_state.get("run_causely_analysis"):
    if st.session_state.get("run_causely_analysis"):
        st.session_state["run_causely_analysis"] = False
    with st.spinner("리포트 생성 중… (Evidence 수집 및 브리핑 작성)"):
        evidence = core.build_evidence(today, orders, items, adj, products)
        try:
            briefing = core.generate_briefing(evidence)
        except RuntimeError as e:
            if "OPENAI_API_KEY" in str(e):
                st.error(
                    "OPENAI API 키가 설정되지 않았습니다. "
                    "터미널에서 `export OPENAI_API_KEY='...'` 실행 후 앱을 다시 띄워 주세요."
                )
                st.info("Evidence만 먼저 확인하려면 아래 expander를 펼쳐 보세요.")
            else:
                raise
            briefing = None
    st.session_state["report_shown"] = True
    st.session_state["report_briefing"] = briefing
    st.session_state["report_today"] = today

if st.session_state.get("report_shown"):
    report_today = st.session_state.get("report_today", today)
    briefing = st.session_state.get("report_briefing")
    period_options = {"D-1 (전일)": 1, "D-7 (7일 전)": 7, "D-28 (28일 전)": 28}
    period_label = st.selectbox("비교 기간", list(period_options.keys()), key="kpi_period")
    n_days = period_options[period_label]
    compare_date = report_today - timedelta(days=n_days)
    # 비교기간에 맞춰 evidence 재계산 (상세 인사이트·표 싱크)
    evidence = core.build_evidence(report_today, orders, items, adj, products, compare_date=compare_date)

    if briefing:
        st.subheader("브리핑")
        st.write(briefing["headline"])

    # --- 범인 검거: 기여도 분석에서 가장 영향 큰 지표 ---
    decomp = core.get_sales_decomposition(report_today, n_days, items, orders)
    main_driver = decomp["main_driver"]
    main_pct = decomp["main_driver_contrib_pct"]
    st.markdown("### 🎯 범인 검거")
    if main_driver == "동일":
        st.info("매출 변동이 없어 기여도가 동일합니다.")
    else:
        st.success(
            f"**전체 매출 변동에 가장 큰 영향을 준 지표는 '{main_driver}'입니다.** "
            f"(기여도 약 {abs(main_pct):.1f}%)"
        )
        with st.expander("유입량×전환율×객단가 기여도 요약"):
            r, n, a = decomp["revenue"], decomp["order_count"], decomp["aov"]
            st.caption(f"매출: {r['current']:,.0f} (기준일 {r['compare']:,.0f}, Δ {r['delta']:+,.0f})")
            st.caption(f"주문수: {n['current']:,.0f} (기준일 {n['compare']:,.0f}, Δ {n['delta']:+,.0f})")
            st.caption(f"객단가: {a['current']:,.1f} (기준일 {a['compare']:,.1f}, Δ {a['delta']:+,.1f})")
            st.caption(f"주문수 기여분: {decomp['contrib_orders']:+,.0f} / 객단가 기여분: {decomp['contrib_aov']:+,.0f}")

    # --- 사장님, 여기만 보세요: 변동 큰 상위 3 상품, 상위 2 채널 ---
    focus = core.get_focus_summary(report_today, n_days, items, adj, products, orders)
    st.markdown("### 👀 사장님, 여기만 보세요")
    fc1, fc2 = st.columns(2)
    with fc1:
        st.caption("**변동 폭 큰 상위 3개 상품**")
        for p in focus["top_3_products"]:
            st.write(f"- **{p['name']}**: {p['current']:,.0f} (기준일 {p['compare']:,.0f}) → **Δ {p['delta']:+,.0f} ({p['pct']:+.1f}%)**")
    with fc2:
        st.caption("**변동 폭 큰 상위 2개 채널**")
        for c in focus["top_2_channels"]:
            st.write(f"- **{c['channel']}**: {c['current']:,.0f} (기준일 {c['compare']:,.0f}) → **Δ {c['delta']:+,.0f} ({c['pct']:+.1f}%)**")
    if not focus["top_3_products"] and not focus["top_2_channels"]:
        st.caption("비교할 상품/채널 데이터가 없습니다.")

    # --- 핵심 요약: 매출·비용·손익비율 3지표 (전일 대비 %·절대값 크게) ---
    st.markdown("### 핵심 요약")
    top3 = core.get_top_three_metrics(report_today, n_days, items, adj)
    col1, col2, col3 = st.columns(3)
    for col, (label, key) in zip([col1, col2, col3], [("매출", "매출"), ("비용", "비용"), ("손익비율", "손익비율")]):
        row = top3[key]
        c, p, d, pct = row["current"], row["compare"], row["delta"], row["pct"]
        with col:
            fig = go.Figure(
                go.Indicator(
                    mode="number+delta",
                    value=c,
                    number={"font": {"size": 42}, "valueformat": ",.0f"},
                    delta={
                        "reference": p,
                        "valueformat": ",.0f",
                        "relative": True,
                        "suffix": "%",
                        "increasing": {"color": "#2e7d32"},
                        "decreasing": {"color": "#c62828"},
                        "font": {"size": 24},
                    },
                    title={"text": f"<b>{label}</b>", "font": {"size": 18}},
                )
            )
            fig.update_layout(height=160, margin=dict(l=30, r=30, t=50, b=20), paper_bgcolor="white", plot_bgcolor="white")
            st.plotly_chart(fig, use_container_width=True)
            st.markdown(f"**기준일 {p:,.0f}** → 증감 **{d:+,.0f}** ({pct:+.1f}%)")

    # 매출 분해: [유입량 | 전환율 | 객단가] + 범인 문구
    decomp = core.get_sales_decomposition(report_today, n_days, items, orders)
    narrative = core.get_sales_narrative(decomp)
    st.markdown("#### 매출 분해 (유입량 × 전환율 × 객단가)")
    st.info(narrative)
    tab_유입, tab_전환, tab_객단가 = st.tabs(["유입량(주문수)", "전환율(주문당 상품수)", "객단가"])
    with tab_유입:
        u = decomp["유입량"]
        st.metric("유입량", f"{u['current']:,.0f}", f"{u['delta']:+,.0f} ({u['pct']:+.1f}%)", help=f"기준일 {u['compare']:,.0f}")
    with tab_전환:
        t = decomp["전환율"]
        st.metric("전환율", f"{t['current']:.2f}", f"{t['delta']:+.2f} ({t['pct']:+.1f}%)", help=f"기준일 {t['compare']:.2f}")
    with tab_객단가:
        a = decomp["객단가"]
        st.metric("객단가", f"{a['current']:,.0f}", f"{a['delta']:+,.0f} ({a['pct']:+.1f}%)", help=f"기준일 {a['compare']:,.0f}")

    # The List: 전환율/매출 기여도가 가장 크게 떨어진 속성의 최근 14일 꺾은선
    st.markdown("#### 📉 The List — 가장 크게 떨어진 지표 (최근 14일)")
    worst_metric = core.get_worst_dropped_metric(decomp)
    metric_label = {"order_count": "유입량(주문수)", "conversion": "전환율(주문당 상품수)", "aov": "객단가"}
    series_14 = core.get_14day_series(report_today, items, orders, worst_metric)
    if series_14:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=[x["date"] for x in series_14],
                y=[x["value"] for x in series_14],
                mode="lines+markers",
                line=dict(width=3),
                marker=dict(size=8),
                name=metric_label.get(worst_metric, worst_metric),
            )
        )
        fig.update_layout(
            title=f"{metric_label.get(worst_metric, worst_metric)} — 어느 지점에서 급격히 꺾였는지 확인하세요",
            xaxis_title="일자",
            yaxis_title="값",
            height=320,
            margin=dict(l=50, r=30, t=50, b=50),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("14일 데이터가 없습니다.")

    if briefing:
        st.markdown("### 상세 인사이트")
        st.caption(f"비교 기준: **{period_label}** (오늘 {str(report_today)} vs 기준일 {str(compare_date)}) — 아래 수치는 선택한 비교기간에 맞춰 동기화됩니다.")
        for item in briefing["key_findings"]:
            if isinstance(item, dict):
                finding_text = item.get("finding", "")
                supporting_data = item.get("supporting_data")
            else:
                finding_text = item
                supporting_data = None
            st.write("-", finding_text)
            if supporting_data is not None:
                try:
                    if isinstance(supporting_data, list) and supporting_data and isinstance(supporting_data[0], dict):
                        df = pd.DataFrame(supporting_data)
                    elif isinstance(supporting_data, dict):
                        df = pd.DataFrame([supporting_data])
                    else:
                        df = None
                    if df is not None and not df.empty:
                        if "어제" in df.columns:
                            df = df.rename(columns={"어제": "기준일"})
                        st.dataframe(df, use_container_width=True, hide_index=True)
                except Exception:
                    st.json(supporting_data)

        st.markdown("### 권장 액션")
        for a in briefing["actions"]:
            st.write(f"**{a['title']}**")
            st.write("-", a["why"])
            if a.get("expected_impact"):
                st.write("-", a["expected_impact"])

    with st.expander("Evidence (debug)"):
        st.caption(f"기준일: {evidence.get('compare_to', '')} (선택한 비교기간과 동일)")
        st.json(evidence)

# --- 3) 핵심 지표 (카테고리별) - metrics.py 연동 ---
st.subheader("3) 핵심 지표 (카테고리별)")
start_date = today.replace(day=1)
tables = {
    "order_items": items,
    "adjustments": adj,
    "orders": orders,
}
ctx = Context(tables=tables, start_date=start_date, end_date=today)
registry = build_default_registry()

for category in registry.categories():
    st.markdown(f"#### {category}")
    metrics_in_cat = registry.list_by_category(category)
    # 카테고리 내 지표별로 계산 후 성공한 것만 수집
    computed = []
    for m in metrics_in_cat:
        try:
            df = registry.compute_metric(m.key, ctx)
            if df is not None and not df.empty:
                computed.append((m, df))
        except Exception as e:
            st.caption(f"**{m.title}** — 계산 생략: {e}")
    if not computed:
        continue
    # 카테고리별 지표들을 한 줄에 최대 4개씩 배치
    n_per_row = 4
    for start in range(0, len(computed), n_per_row):
        chunk = computed[start : start + n_per_row]
        cols = st.columns(len(chunk))
        for i, (m, df) in enumerate(chunk):
            with cols[i]:
                latest = df["value"].iloc[-1] if len(df) else 0
                st.metric(m.title, f"{latest:,.0f}", help=m.description)
                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=df["date"].astype(str),
                        y=df["value"],
                        mode="lines+markers",
                        line=dict(width=2),
                        marker=dict(size=4),
                    )
                )
                fig.update_layout(
                    height=200,
                    margin=dict(l=30, r=10, t=20, b=30),
                    xaxis_title="일자",
                    yaxis_title="값",
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)
    st.divider()
