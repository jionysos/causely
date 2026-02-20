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

st.set_page_config(layout="wide", page_title="Causely", page_icon="📊")

st.markdown(
    """
    <style>
    .main .block-container { max-width: 100%; padding-left: 1rem; padding-right: 1rem; }
    [data-testid="stDataFrame"] { max-width: 100% !important; overflow-x: auto !important; }
    [data-testid="stDataFrame"] table { table-layout: fixed; width: 100% !important; font-size: clamp(0.75rem, 1.8vw, 0.95rem); word-break: break-word; }
    [data-testid="stDataFrame"] th, [data-testid="stDataFrame"] td { word-break: break-word; }

    .stTabs [data-baseweb="tab-list"] { gap: 8px; border-bottom: 2px solid #f0f0f0; }
    .stTabs [data-baseweb="tab"] { font-size: 1rem; font-weight: 600; padding: 0.6rem 1.5rem; }

    .hero-section {
        background: linear-gradient(135deg, #0f0f23 0%, #1a1a3e 50%, #0d1b2a 100%);
        border-radius: 20px;
        padding: 80px 60px;
        text-align: center;
        margin-bottom: 40px;
        color: white;
    }
    .hero-badge {
        display: inline-block;
        background: rgba(99, 102, 241, 0.2);
        border: 1px solid rgba(99, 102, 241, 0.5);
        color: #a5b4fc;
        padding: 6px 16px;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
        margin-bottom: 24px;
        letter-spacing: 0.05em;
    }
    .hero-title {
        font-size: clamp(2rem, 5vw, 3.5rem);
        font-weight: 800;
        line-height: 1.15;
        margin-bottom: 20px;
        background: linear-gradient(135deg, #ffffff 0%, #a5b4fc 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .hero-subtitle {
        font-size: clamp(1rem, 2vw, 1.25rem);
        color: #94a3b8;
        max-width: 600px;
        margin: 0 auto 36px;
        line-height: 1.7;
    }
    .feature-card {
        background: #f8faff;
        border: 1px solid #e8ecf8;
        border-radius: 16px;
        padding: 32px 28px;
        height: 100%;
    }
    .feature-icon { font-size: 2rem; margin-bottom: 16px; }
    .feature-title { font-size: 1.1rem; font-weight: 700; color: #1e1b4b; margin-bottom: 10px; }
    .feature-desc { font-size: 0.92rem; color: #64748b; line-height: 1.65; }
    .compare-card { border-radius: 16px; padding: 28px; text-align: center; }
    .compare-bad { background: #fff5f5; border: 1px solid #fecaca; }
    .compare-good { background: #f0fdf4; border: 1px solid #86efac; }
    .compare-title { font-size: 1rem; font-weight: 700; margin-bottom: 16px; }
    .compare-item { font-size: 0.9rem; padding: 6px 0; border-bottom: 1px solid rgba(0,0,0,0.05); }
    .stat-card {
        background: white;
        border: 1px solid #e8ecf8;
        border-radius: 16px;
        padding: 28px;
        text-align: center;
        box-shadow: 0 2px 12px rgba(0,0,0,0.04);
    }
    .stat-number { font-size: 2.5rem; font-weight: 800; color: #6366f1; }
    .stat-label { font-size: 0.9rem; color: #64748b; margin-top: 6px; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── 데이터 로딩 (공통) ──────────────────────────────────────────────
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

orders = _loaded.get("orders")
items = _loaded.get("order_items")
adj = _loaded.get("adjustments")
products = _loaded.get("products")
ad_costs = _loaded.get("ad_costs")
influencer_costs = _loaded.get("influencer_costs")

# ── 탭 구성 ─────────────────────────────────────────────────────────
tab_home, tab_analysis = st.tabs(["🏠  홈", "📊  분석"])

# ════════════════════════════════════════════════════════════════════
# TAB 1: 홈
# ════════════════════════════════════════════════════════════════════
with tab_home:

    st.markdown("""
    <div class="hero-section">
        <div class="hero-badge">✦ AI 원인 분석 플랫폼</div>
        <div class="hero-title">"이번 주 매출,<br>왜 떨어졌지?"</div>
        <div class="hero-subtitle">
            핵심 지표의 변화를 감지하고,<br>
            왜 변했는지 원인 힌트까지 자동으로.<br>
            의사결정을 빠르고 효율적으로 만듭니다.
        </div>
    </div>
    """, unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("""
        <div class="stat-card">
            <div class="stat-number">즉시</div>
            <div class="stat-label">KPI 변화 원인 파악<br><small style="color:#94a3b8">(기존 분석가 하루~이틀 소요)</small></div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown("""
        <div class="stat-card">
            <div class="stat-number">매일</div>
            <div class="stat-label">Slack 자동 알림<br><small style="color:#94a3b8">팀 전체가 같은 맥락 공유</small></div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown("""
        <div class="stat-card">
            <div class="stat-number">Why</div>
            <div class="stat-label">숫자가 아닌 원인 힌트<br><small style="color:#94a3b8">빠른 의사결정을 위한 도구</small></div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("### 🔄 기존 방식 vs Causely")
    col_bad, col_mid, col_good = st.columns([5, 1, 5])
    with col_bad:
        st.markdown("""
        <div class="compare-card compare-bad">
            <div class="compare-title">😫 기존 방식</div>
            <div class="compare-item">GA4, 광고, 매출 대시보드 따로 확인</div>
            <div class="compare-item">분석가가 그래프 수십 개 수동 정리</div>
            <div class="compare-item">Slack에 분석 올리는 데 하루 소요</div>
            <div class="compare-item">원인 추정은 여전히 사람이 직접</div>
            <div class="compare-item">의사결정까지 2~3일 소요</div>
        </div>""", unsafe_allow_html=True)
    with col_mid:
        st.markdown("<div style='text-align:center; font-size:2rem; padding-top:80px'>→</div>", unsafe_allow_html=True)
    with col_good:
        st.markdown("""
        <div class="compare-card compare-good">
            <div class="compare-title">✅ Causely</div>
            <div class="compare-item">핵심 지표 변화 자동 감지</div>
            <div class="compare-item">KPI 변화 원인 힌트 자동 제공</div>
            <div class="compare-item">매일 아침 Slack으로 자동 전송</div>
            <div class="compare-item">대시보드에서 즉시 원인 파악</div>
            <div class="compare-item" style="font-weight:700; color:#16a34a">의사결정까지 당일 완료 ⚡</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("### ⚡ 핵심 기능")
    f1, f2, f3 = st.columns(3)
    with f1:
        st.markdown("""
        <div class="feature-card">
            <div class="feature-icon">🔍</div>
            <div class="feature-title">KPI 변화 원인 파악</div>
            <div class="feature-desc">
                매출·유저·전환율이 변했을 때,
                어떤 캠페인·채널·상품이 원인인지
                힌트를 자동으로 찾아줍니다.
            </div>
        </div>""", unsafe_allow_html=True)
    with f2:
        st.markdown("""
        <div class="feature-card">
            <div class="feature-icon">📊</div>
            <div class="feature-title">핵심 지표 대시보드</div>
            <div class="feature-desc">
                업종에 맞는 핵심 지표를 구성하고,
                변화를 한눈에 파악할 수 있는
                대시보드를 자동으로 만들어줍니다.
            </div>
        </div>""", unsafe_allow_html=True)
    with f3:
        st.markdown("""
        <div class="feature-card">
            <div class="feature-icon">💬</div>
            <div class="feature-title">매일 Slack 자동 알림</div>
            <div class="feature-desc">
                매일 아침 핵심 지표 요약과
                원인 힌트가 Slack으로 자동 전송.
                팀 전체가 같은 맥락을 공유합니다.
            </div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("### 👥 이런 분들을 위해 만들었어요")
    t1, t2, t3 = st.columns(3)
    for col, icon, title, desc in [
        (t1, "🏃", "데이터 분석가 없는 스타트업 대표",
         'KPI가 왜 변했는지 원인 힌트를 즉시 파악. 분석가 없어도 빠른 의사결정이 가능합니다.'),
        (t2, "📣", "마케터 · PM",
         "캠페인·채널·상품 중 무엇이 지표를 움직였는지 자동으로 찾아줍니다. 수동 분석은 그만."),
        (t3, "📋", "데이터 분석가",
         "매번 반복되는 KPI 리포트 자동화. 원인 분석에 집중할 수 있는 시간을 돌려드립니다."),
    ]:
        with col:
            st.markdown(f"""
            <div class="feature-card">
                <div class="feature-icon">{icon}</div>
                <div class="feature-title">{title}</div>
                <div class="feature-desc">{desc}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br><br>", unsafe_allow_html=True)

    st.markdown("""
    <div style="background: linear-gradient(135deg, #6366f1, #8b5cf6); border-radius: 20px; padding: 60px; text-align: center; color: white;">
        <div style="font-size: 1.8rem; font-weight: 800; margin-bottom: 12px;">지금 바로 분석해보세요</div>
        <div style="color: rgba(255,255,255,0.8); margin-bottom: 12px; font-size: 1rem;">
            위 <b>📊 분석</b> 탭에서 바로 체험할 수 있습니다
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════
# TAB 2: 분석
# ════════════════════════════════════════════════════════════════════
with tab_analysis:

    st.title("Causely — 데이터 분석")

    if missing:
        st.error(f"필수 파일이 없습니다. `files/` 폴더에 다음을 넣어 주세요: {', '.join(f'{x}.csv' for x in missing)}")
        st.stop()

    st.caption(f"기본 DB: `{FILES_DIR}`")

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
            with st.container():
                st.markdown("#### 💬 분석 결과에 대해 질문하세요")
                st.caption("리포트와 전체 DB(orders, order_items, adjustments, products)를 참고해 답변합니다.")
                if "iv_chat_messages" not in st.session_state:
                    st.session_state["iv_chat_messages"] = []

                if not st.session_state["iv_chat_messages"]:
                    with st.chat_message("assistant"):
                        st.write('리포트와 전체 DB를 바탕으로 질문해 주세요. 예: "환불액이 높은 이유가 뭐야?", "상품 P010 파는 셀러가 누구야?", "채널별로 어떤 액션을 취해야 해?"')

                for msg in st.session_state["iv_chat_messages"]:
                    with st.chat_message(msg["role"]):
                        st.write(msg["content"])
                        if msg.get("table") is not None and not msg["table"].empty:
                            st.caption("조회 결과")
                            st.dataframe(msg["table"], use_container_width=True, hide_index=True)

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
            st.markdown("---")
            st.subheader("💬 분석 결과 질의응답")
            st.info("👆 위에서 **IV 기반 LLM 리포트 생성** 버튼을 누르면, 리포트와 함께 분석 결과에 대해 질문할 수 있는 채팅이 나타납니다.")
    except Exception as e:
        st.warning(f"테이블/IV 계산 중 오류: {e}")
