import os
import streamlit as st
import pandas as pd
from datetime import date, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import core
from report_tables import (
    build_key_metric_table,
    build_cost_detail_table,
    get_iv_ranking,
    get_high_iv_detail_tables,
    build_components_for_llm,
)

st.set_page_config(layout="wide")
# 웹 크기에 맞게 표가 줄어들고 가로 스크롤 없이 보이도록
st.markdown(
    """
    <style>
    .main .block-container { max-width: 100%; padding-left: 1rem; padding-right: 1rem; }
    [data-testid="stDataFrame"] { max-width: 100% !important; overflow-x: auto !important; }
    [data-testid="stDataFrame"] table { table-layout: fixed; width: 100% !important; font-size: clamp(0.75rem, 1.8vw, 0.95rem); word-break: break-word; }
    [data-testid="stDataFrame"] th, [data-testid="stDataFrame"] td { word-break: break-word; }
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("Causely — 데이터 분석")

# 기본 DB: files/ 폴더의 CSV 사용
FILES_DIR = os.path.join(os.path.dirname(__file__), "files")
REQUIRED = ["orders.csv", "order_items.csv", "adjustments.csv", "products.csv"]
OPTIONAL = ["users.csv", "coupons.csv", "ad_costs.csv", "influencer_costs.csv"]


def load_csv(name: str):
    path = os.path.join(FILES_DIR, name)
    if not os.path.isfile(path):
        return None
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.read_csv(path, encoding="cp949")


missing = [fn for fn in REQUIRED if not os.path.isfile(os.path.join(FILES_DIR, fn))]
if missing:
    st.error(f"필수 파일이 없습니다. `files/` 폴더에 다음을 넣어 주세요: {', '.join(missing)}")
    st.stop()

orders = load_csv("orders.csv")
items = load_csv("order_items.csv")
adj = load_csv("adjustments.csv")
products = load_csv("products.csv")
ad_costs = load_csv("ad_costs.csv")
influencer_costs = load_csv("influencer_costs.csv")

st.caption(f"기본 DB: `{FILES_DIR}`")

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

# --- 주요 테이블 + IV 분석 + 리포트 (기준일 D-n 선택) ---
st.subheader("주요 지표·매출/비용 상세 및 IV 리포트")
period_days = {"D-1": 1, "D-7": 7, "D-14": 14, "D-28": 28}
period_choice = st.selectbox("비교 기준일", list(period_days.keys()), key="report_period")
n_days = period_days[period_choice]
compare_date = today - timedelta(days=n_days)

try:
    key_metric_df = build_key_metric_table(today, compare_date, items, adj, ad_costs=ad_costs, influencer_costs=influencer_costs)
    if not key_metric_df.empty:
        row_today = key_metric_df[key_metric_df["구분"] == "오늘"].iloc[0]
        row_base = key_metric_df[key_metric_df["구분"] == "기준일"].iloc[0]
        st.markdown("#### 1) Key metric — 오늘 vs 기준일 증감%")
        c1, c2, c3 = st.columns(3)
        for col, label, key in [(c1, "총매출", "총매출"), (c2, "총비용", "총비용"), (c3, "순이익", "순이익")]:
            with col:
                a, b = row_today[key], row_base[key]
                pct = ((a - b) / b * 100) if b != 0 else 0
                st.metric(label, f"{a:,.0f}", f"{pct:+.1f}%")
        st.dataframe(key_metric_df, use_container_width=True, hide_index=True)

    cost_detail_df = build_cost_detail_table(today, compare_date, items, adj, ad_costs=ad_costs, influencer_costs=influencer_costs)
    st.markdown("#### 2) 차이 기여도 (Information Value)")
    iv_result = get_iv_ranking(items, adj, today, compare_date)
    rank_df = pd.DataFrame(iv_result["ranking"], columns=["요인", "IV"])
    st.dataframe(rank_df, use_container_width=True, hide_index=True)
    st.caption(
        "IV가 클수록 오늘 vs 기준일 차이를 그 요인이 더 잘 설명합니다. "
        "**(매출)** = 매출 구성(채널/광고/인플 유무), **(비용)** = 비용 금액을 기준일 10% 구간화 후 구간별 건수 구성비 차이. "
        "**IV 20 초과**인 요인만 아래 상세 테이블에 표시됩니다."
    )

    iv_threshold = 20
    high_iv_tables = get_high_iv_detail_tables(items, adj, today, compare_date, iv_result, cost_detail_df, threshold=iv_threshold, top_n=5)
    if high_iv_tables:
        st.markdown("#### 3) IV 20 초과 요인 상세 (표 2벌: 요약 + 오늘자 기준 Top 5)")
        for block in high_iv_tables:
            st.markdown(f"**{block['factor']}** (IV: {block['iv']:.2f})")
            summary_df = block.get("summary_table")
            detail_df = block.get("detail_table")
            col1, col2 = st.columns(2)
            with col1:
                if summary_df is not None and not summary_df.empty:
                    st.caption("요약 (날짜 | 지표)")
                    st.dataframe(summary_df, use_container_width=True, hide_index=True)
            with col2:
                if detail_df is not None and not detail_df.empty:
                    st.caption("상세 (ID | 오늘자 | 기준일, Top 5)")
                    st.dataframe(detail_df, use_container_width=True, hide_index=True)
            st.divider()

    if st.button("IV 기반 LLM 리포트 생성"):
        with st.spinner("리포트 생성 중…"):
            components = build_components_for_llm(key_metric_df, iv_result, high_iv_tables, threshold=iv_threshold)
            try:
                report = core.generate_iv_report(components)
                context = core.build_llm_context(components)
                st.session_state["iv_report"] = report
                st.session_state["iv_report_context"] = context
                if "iv_chat_messages" not in st.session_state:
                    st.session_state["iv_chat_messages"] = []
            except RuntimeError as e:
                if "OPENAI_API_KEY" in str(e):
                    st.error("OPENAI_API_KEY를 설정한 뒤 다시 시도해 주세요.")
                else:
                    raise

    # 이미 생성된 리포트가 있으면 표시 (버튼 없이 재진입 시에도)
    if st.session_state.get("iv_report"):
        report = st.session_state["iv_report"]
        st.markdown("---")
        st.subheader("IV 기반 리포트")
        st.write(report.get("headline", ""))
        for sec in report.get("sections", []):
            st.markdown(f"**{sec.get('title', '')}**")
            st.write(sec.get("body", ""))

    # 분석 결과 질의응답 — 항상 표시 (리포트 없으면 안내만)
    st.markdown("---")
    st.subheader("💬 분석 결과 질의응답")
    if not st.session_state.get("iv_report"):
        st.info("👆 위에서 **IV 기반 LLM 리포트 생성** 버튼을 누르면, 여기서 분석 결과에 대해 질문할 수 있습니다.")
    else:
        st.caption("리포트뿐 아니라 orders, order_items, adjustments, products 등 전체 DB를 참고해 답변합니다. 예: 상품 P010의 셀러는 products의 seller_id에서 확인.")
        if "iv_chat_messages" not in st.session_state:
            st.session_state["iv_chat_messages"] = []

        # 환영 메시지: 대화가 비어 있을 때 한 줄 안내
        if not st.session_state["iv_chat_messages"]:
            with st.chat_message("assistant"):
                st.write("리포트와 전체 DB를 바탕으로 질문해 주세요. 예: \"환불액이 높은 이유가 뭐야?\", \"상품 P010 파는 셀러가 누구야?\", \"채널별로 어떤 액션을 취해야 해?\"")

        for msg in st.session_state["iv_chat_messages"]:
            with st.chat_message(msg["role"]):
                st.write(msg["content"])

        if prompt := st.chat_input("분석 결과에 대해 질문하세요..."):
            st.session_state["iv_chat_messages"].append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.write(prompt)

            with st.chat_message("assistant"):
                with st.spinner("답변 생성 중…"):
                    try:
                        db_context = core.build_db_context_for_qa(orders, items, adj, products)
                        reply = core.answer_report_question(
                            st.session_state["iv_report"],
                            st.session_state["iv_report_context"],
                            st.session_state["iv_chat_messages"],
                            db_context=db_context,
                        )
                        st.write(reply)
                        st.session_state["iv_chat_messages"].append({"role": "assistant", "content": reply})
                    except RuntimeError as e:
                        if "OPENAI_API_KEY" in str(e):
                            st.error("OPENAI_API_KEY를 설정한 뒤 다시 시도해 주세요.")
                        else:
                            st.error(str(e))
                        st.session_state["iv_chat_messages"].pop()  # user 메시지만 남기고 재시도 가능하게
except Exception as e:
    st.warning(f"테이블/IV 계산 중 오류: {e}")

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
