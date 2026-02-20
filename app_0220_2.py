import os
import glob
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

# 기본 DB: files/ 폴더 아래 모든 CSV 자동 로드
FILES_DIR = os.path.join(os.path.dirname(__file__), "files")
REQUIRED = ["orders", "order_items", "adjustments", "products"]


def load_csv(path: str):
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.read_csv(path, encoding="cp949")


_csv_paths = glob.glob(os.path.join(FILES_DIR, "*.csv"))
_loaded = {}
for p in _csv_paths:
    name = os.path.splitext(os.path.basename(p))[0]
    _loaded[name] = load_csv(p)

missing = [fn for fn in REQUIRED if fn not in _loaded]
if missing:
    st.error(f"필수 파일이 없습니다. `files/` 폴더에 다음을 넣어 주세요: {', '.join(f'{x}.csv' for x in missing)}")
    st.stop()

orders = _loaded.get("orders")
items = _loaded.get("order_items")
adj = _loaded.get("adjustments")
products = _loaded.get("products")
ad_costs = _loaded.get("ad_costs")
influencer_costs = _loaded.get("influencer_costs")

st.caption(f"기본 DB: `{FILES_DIR}`")

# 기준일: 라벨 + 날짜 선택만 (드롭다운 하나 = 비교기준일은 그래프 아래)
period_days = {"D-1": 1, "D-7": 7, "D-14": 14, "D-28": 28}
if "benchmark_date_input" not in st.session_state:
    st.session_state["benchmark_date_input"] = date(2026, 1, 31)
if "report_period" not in st.session_state:
    st.session_state["report_period"] = "D-1"
today = st.session_state["benchmark_date_input"]
n_days = period_days[st.session_state["report_period"]]
compare_date = today - timedelta(days=n_days)

col_label, col_date = st.columns([1, 5])
with col_label:
    st.caption("기준일")
with col_date:
    st.date_input("기준일", key="benchmark_date_input", label_visibility="collapsed")
today = st.session_state["benchmark_date_input"]
compare_date = today - timedelta(days=period_days[st.session_state["report_period"]])

_report_key = (today, compare_date)
if st.session_state.get("iv_report_key") != _report_key:
    for key in ("iv_report", "iv_report_context", "iv_chat_messages", "iv_report_key"):
        st.session_state.pop(key, None)
st.session_state["iv_report_key"] = _report_key

# --- 맨 상단: 일별 매출 현황 (막대 + 이번달/지난달 누적 꺾은선, x축 1,5,10,15...) ---
st.subheader("주요 지표·매출/비용 상세 및 IV 리포트")
items_chart = items.copy()
if "net_sales_amount" not in items_chart.columns and "gross_amount" in items_chart.columns:
    items_chart["net_sales_amount"] = items_chart["gross_amount"]
try:
    series = core.get_monthly_sales_series(today, items_chart, adj)
    this_month = series["this_month"]
    last_month = series["last_month"]
    if this_month["days"]:
        days_this = this_month["days"]
        days_last = last_month["days"]
        tick_vals = [d for d in [1, 5, 10, 15, 20, 25, 30, 31] if d <= max(days_this)]
        fig_sales = make_subplots(specs=[[{"secondary_y": True}]])
        fig_sales.add_trace(
            go.Bar(x=days_this, y=this_month["daily"], name="이번 달 일별", marker_color="rgba(220, 80, 80, 0.6)"),
            secondary_y=False,
        )
        fig_sales.add_trace(
            go.Scatter(x=days_this, y=this_month["cumulative"], name="이번 달 누적", mode="lines+markers", line=dict(color="rgb(50, 120, 200)", width=2)),
            secondary_y=True,
        )
        fig_sales.add_trace(
            go.Scatter(x=days_last, y=last_month["cumulative"], name="지난달 누적", mode="lines+markers", line=dict(color="rgb(180, 180, 180)", width=2, dash="dot")),
            secondary_y=True,
        )
        fig_sales.update_xaxes(tickvals=tick_vals, title_text="일")
        fig_sales.update_yaxes(title_text="일별 매출 (원)", secondary_y=False)
        fig_sales.update_yaxes(title_text="누적 매출 (원)", secondary_y=True)
        fig_sales.update_layout(height=500, showlegend=True, margin=dict(t=40, b=40))
        st.markdown("#### 일별 매출 현황 (이번달)")
        st.plotly_chart(fig_sales, use_container_width=True)
except Exception:
    pass

# 그래프 아래: 비교기준일 드롭다운만
st.markdown("---")
st.caption("비교 기준일")
st.selectbox("비교 기준일", list(period_days.keys()), key="report_period", label_visibility="collapsed")
today = st.session_state["benchmark_date_input"]
compare_date = today - timedelta(days=period_days[st.session_state["report_period"]])
st.markdown("---")

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
    st.markdown("#### 2) 순이익 변화 핵심 요인 분석")
    iv_result = get_iv_ranking(items, adj, today, compare_date)
    ranking = iv_result["ranking"]
    total_iv = sum(iv for _, iv in ranking)
    if total_iv > 0:
        labels = [name for name, _ in ranking]
        values = [iv / total_iv * 100 for _, iv in ranking]
        fig_pie = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.4, textinfo="label+percent")])
        fig_pie.update_layout(height=400, margin=dict(t=80, b=40, l=20, r=20), showlegend=True, legend=dict(orientation="h", yanchor="top", y=-0.05))
        st.plotly_chart(fig_pie, use_container_width=True)
    st.caption("순이익 변화에 영향을 가장 끼친 요소들과 그 비중을 나타냅니다.")

    iv_threshold = 20
    high_iv_tables = get_high_iv_detail_tables(items, adj, today, compare_date, iv_result, cost_detail_df, threshold=iv_threshold, top_n=5, products=products)
    if high_iv_tables:
        st.markdown("#### 3) 순이익 변화 핵심 요인 상세 TOP5")
        st.caption("변화가 크지 않은 경우 5개 이하로 나타납니다.")
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
                st.session_state["iv_report_key"] = (today, compare_date)
                if "iv_chat_messages" not in st.session_state:
                    st.session_state["iv_chat_messages"] = []
            except RuntimeError as e:
                if "OPENAI_API_KEY" in str(e):
                    st.error("OPENAI_API_KEY를 설정한 뒤 다시 시도해 주세요.")
                else:
                    raise

    # 이미 생성된 리포트가 있으면 표시 (버튼 없이 재진입 시에도) + 그 안에 질의응답 채팅
    if st.session_state.get("iv_report"):
        report = st.session_state["iv_report"]
        st.markdown("---")
        st.subheader("IV 기반 리포트")
        st.write(report.get("headline", ""))
        for sec in report.get("sections", []):
            st.markdown(f"**{sec.get('title', '')}**")
            if sec.get("body"):
                st.write(sec.get("body", ""))
            for i, action in enumerate(sec.get("actions", []), 1):
                label = action.get("label", f"{i}순위")
                st.markdown(f"**{label}** {action.get('action','')}")
        st.markdown("---")
        # 채팅은 '분석 결과에 대해 질문하세요' 하이라키 바로 아래에 묶음
        with st.container():
            st.markdown("#### 💬 분석 결과에 대해 질문하세요")
            st.caption("리포트와 전체 DB(orders, order_items, adjustments, products)를 참고해 답변합니다.")
            if "iv_chat_messages" not in st.session_state:
                st.session_state["iv_chat_messages"] = []

            if not st.session_state["iv_chat_messages"]:
                with st.chat_message("assistant"):
                    st.write("리포트와 전체 DB를 바탕으로 질문해 주세요. 예: \"환불액이 높은 이유가 뭐야?\", \"상품 P010 파는 셀러가 누구야?\", \"채널별로 어떤 액션을 취해야 해?\"")

            for msg in st.session_state["iv_chat_messages"]:
                with st.chat_message(msg["role"]):
                    st.write(msg["content"])
                    if msg.get("table") is not None and not msg["table"].empty:
                        st.caption("조회 결과")
                        st.dataframe(msg["table"], use_container_width=True, hide_index=True)

            # 입력창은 항상 대화 목록 아래(맨 아래)에 두기 위해, 전송 시 메시지만 추가하고 rerun
            if prompt := st.chat_input("분석 결과에 대해 질문하세요..."):
                st.session_state["iv_chat_messages"].append({"role": "user", "content": prompt})
                with st.spinner("답변 생성 중…"):
                    try:
                        reply, table = core.answer_report_question(
                            st.session_state["iv_report"],
                            st.session_state["iv_report_context"],
                            st.session_state["iv_chat_messages"],
                            orders=orders,
                            items=items,
                            adj=adj,
                            products=products,
                        )
                        m = {"role": "assistant", "content": reply}
                        if table is not None and not table.empty:
                            m["table"] = table
                        st.session_state["iv_chat_messages"].append(m)
                    except RuntimeError as e:
                        if "OPENAI_API_KEY" in str(e):
                            st.error("OPENAI_API_KEY를 설정한 뒤 다시 시도해 주세요.")
                        else:
                            st.error(str(e))
                        st.session_state["iv_chat_messages"].pop()
                st.rerun()
    else:
        # 리포트 없을 때만 별도로 질의응답 섹션 표시 (안내)
        st.markdown("---")
        st.subheader("💬 분석 결과 질의응답")
        st.info("👆 위에서 **IV 기반 LLM 리포트 생성** 버튼을 누르면, 리포트와 함께 분석 결과에 대해 질문할 수 있는 채팅이 나타납니다.")
except Exception as e:
    st.warning(f"테이블/IV 계산 중 오류: {e}")
