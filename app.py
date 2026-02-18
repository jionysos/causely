import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from openai import OpenAI
import os
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

st.set_page_config(page_title="Causely", page_icon="🔍", layout="wide")

if "page" not in st.session_state:
    st.session_state.page = "onboarding"
if "data" not in st.session_state:
    st.session_state.data = {}

# =============================================
# DAG 그리기 공통 함수
# =============================================
def draw_dag(nodes, edges, pos, highlights=None):
    """
    nodes: [{"id": str, "formula": str, "color": str}]
    edges: [(from, to)]
    pos: {id: (x, y)}
    highlights: {id: "red"/"green"/"orange"} 이상 노드 강조
    """
    edge_x, edge_y = [], []
    for u, v in edges:
        if u in pos and v in pos:
            x0, y0 = pos[u]; x1, y1 = pos[v]
            edge_x += [x0, x1, None]; edge_y += [y0, y1, None]

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=edge_x, y=edge_y, mode='lines',
        line=dict(color='#cbd5e1', width=1.5), hoverinfo='none'))

    for node in nodes:
        if node["id"] not in pos:
            continue
        x, y = pos[node["id"]]
        color = node.get("color", "#6366f1")
        if highlights and node["id"] in highlights:
            color = highlights[node["id"]]
        fig.add_trace(go.Scatter(
            x=[x], y=[y], mode='markers+text',
            text=[node["id"]], textposition="top center",
            marker=dict(size=38, color=color, line=dict(width=2, color='white')),
            hovertext=f"{node['id']}<br>{node.get('formula','')}",
            hoverinfo='text', textfont=dict(size=11), showlegend=False
        ))

    fig.update_layout(height=380, plot_bgcolor='white',
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis=dict(showgrid=False, zeroline=False, visible=False),
        yaxis=dict(showgrid=False, zeroline=False, visible=False))
    return fig

# =============================================
# 페이지 1: 온보딩
# =============================================
def page_onboarding():
    st.title("🔍 Causely")
    st.subheader("비즈니스 데이터의 'Why'를 자동으로 찾아주는 AI 분석 어시스턴트")
    st.markdown("---")
    st.markdown("### 어떤 비즈니스를 운영하고 계신가요?")
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("🛍️ 패션/커머스", use_container_width=True, type="primary"):
            st.session_state.page = "upload"
            st.rerun()
    with col2:
        st.button("💻 SaaS (준비중)", use_container_width=True, disabled=True)
    with col3:
        st.button("📱 앱 서비스 (준비중)", use_container_width=True, disabled=True)
    st.markdown("---")
    st.caption("💡 Causely는 단순한 대시보드가 아닙니다. 지표 간 인과관계를 자동으로 분석해 Why를 알려드려요.")

# =============================================
# 페이지 2: 데이터 업로드
# =============================================
def page_upload():
    st.title("📂 데이터 업로드")
    st.markdown("---")

    required_keys = ["orders", "order_items", "adjustments", "products", "coupons", "users", "influencer_costs", "ad_costs"]

    st.markdown("### CSV 파일을 한번에 선택해주세요")
    st.caption("파일명이 orders.csv, order_items.csv 등으로 되어 있으면 자동으로 매핑돼요.")

    files = st.file_uploader("CSV 파일 전체 선택 (다중 선택 가능)", type="csv",
                              accept_multiple_files=True, key="bulk_upload")

    uploaded = {}
    if files:
        for f in files:
            name = f.name.replace(".csv", "")
            if name in required_keys:
                uploaded[name] = pd.read_csv(f)

        # 업로드 현황 표시
        cols = st.columns(4)
        for i, key in enumerate(required_keys):
            with cols[i % 4]:
                if key in uploaded:
                    st.success(f"✅ {key}\n{len(uploaded[key])}행")
                else:
                    st.error(f"❌ {key}")

    st.markdown("---")
    n_required = len(required_keys)
    n_uploaded = len(uploaded)
    st.progress(n_uploaded / n_required, text=f"{n_uploaded}/{n_required} 파일 업로드됨")

    if n_uploaded == n_required:
        st.session_state.data = uploaded
        if st.button("🚀 지표 관계 자동 생성하기", use_container_width=True, type="primary"):
            st.session_state.page = "dag"
            st.rerun()
    else:
        st.info(f"📎 {n_required - n_uploaded}개 파일이 더 필요해요. 파일명을 확인해주세요.")

    if st.button("← 처음으로"):
        st.session_state.page = "onboarding"
        st.rerun()

# =============================================
# 페이지 3: DAG 확인
# =============================================
def page_dag():
    st.title("🕸️ 지표 인과관계 구조 확인")
    st.caption("업로드하신 데이터를 기반으로 3가지 지표 구조를 자동으로 구성했어요.")
    st.markdown("---")

    tab1, tab2, tab3 = st.tabs(["📊 그래프 1: 매출 분해", "💰 그래프 2: 손익 분해", "📣 그래프 3: 마케팅 (MER)"])

    # --- 그래프 1: 매출 분해 ---
    with tab1:
        st.subheader("매출 = 주문수 × 객단가")
        nodes1 = [
            {"id": "매출", "formula": "주문수 × 객단가", "color": "#6366f1"},
            {"id": "주문수", "formula": "방문자수 × 전환율", "color": "#8b5cf6"},
            {"id": "객단가", "formula": "총매출 / 주문수", "color": "#8b5cf6"},
            {"id": "방문자수", "formula": "채널별 유입 합산", "color": "#06b6d4"},
            {"id": "전환율", "formula": "주문수 / 방문자수", "color": "#06b6d4"},
            {"id": "채널별유입", "formula": "자사몰/쿠팡/네이버/지그재그/에이블리", "color": "#94a3b8"},
            {"id": "인플루언서유입", "formula": "influencer_id별 주문수", "color": "#94a3b8"},
            {"id": "상품구성", "formula": "카테고리별 판매 비중", "color": "#94a3b8"},
        ]
        edges1 = [
            ("주문수", "매출"), ("객단가", "매출"),
            ("방문자수", "주문수"), ("전환율", "주문수"),
            ("채널별유입", "방문자수"), ("인플루언서유입", "방문자수"),
            ("상품구성", "객단가"),
        ]
        pos1 = {
            "매출": (3, 4),
            "주문수": (2, 3), "객단가": (4, 3),
            "방문자수": (1, 2), "전환율": (3, 2),
            "채널별유입": (0.5, 1), "인플루언서유입": (1.5, 1),
            "상품구성": (4, 1),
        }
        st.plotly_chart(draw_dag(nodes1, edges1, pos1), use_container_width=True)

    # --- 그래프 2: 손익 분해 ---
    with tab2:
        st.subheader("영업이익 = 순매출 - 변동비 - 마케팅비 - 고정비")
        nodes2 = [
            {"id": "영업이익", "formula": "순매출 - 변동비 - 마케팅비 - 고정비", "color": "#6366f1"},
            {"id": "순매출", "formula": "총매출 - 환불 - 할인 - 수수료", "color": "#8b5cf6"},
            {"id": "변동비", "formula": "배송비 + 포장비", "color": "#8b5cf6"},
            {"id": "마케팅비", "formula": "인플루언서비용 + 쿠폰비용", "color": "#8b5cf6"},
            {"id": "고정비", "formula": "인건비 + 임차료 등", "color": "#8b5cf6"},
            {"id": "총매출", "formula": "Σ gross_amount", "color": "#06b6d4"},
            {"id": "환불금액", "formula": "Σ adjustments.amount", "color": "#06b6d4"},
            {"id": "할인금액", "formula": "Σ discount_amount", "color": "#06b6d4"},
            {"id": "플랫폼수수료", "formula": "채널별 수수료율 × 매출", "color": "#06b6d4"},
            {"id": "상품별환불율", "formula": "환불건수 / 주문건수 (상품별)", "color": "#94a3b8"},
            {"id": "셀러별불량율", "formula": "DEFECT 환불 / 전체환불 (셀러별)", "color": "#94a3b8"},
        ]
        edges2 = [
            ("순매출", "영업이익"), ("변동비", "영업이익"),
            ("마케팅비", "영업이익"), ("고정비", "영업이익"),
            ("총매출", "순매출"), ("환불금액", "순매출"),
            ("할인금액", "순매출"), ("플랫폼수수료", "순매출"),
            ("상품별환불율", "환불금액"), ("셀러별불량율", "환불금액"),
        ]
        pos2 = {
            "영업이익": (4, 5),
            "순매출": (2, 4), "변동비": (4, 4), "마케팅비": (5.5, 4), "고정비": (7, 4),
            "총매출": (1, 3), "환불금액": (2.5, 3), "할인금액": (4, 3), "플랫폼수수료": (5.5, 3),
            "상품별환불율": (2, 2), "셀러별불량율": (3.5, 2),
        }
        st.plotly_chart(draw_dag(nodes2, edges2, pos2), use_container_width=True)

    # --- 그래프 3: 마케팅 MER ---
    with tab3:
        st.subheader("MER = 전체매출 / 전체마케팅비용")
        nodes3 = [
            {"id": "MER", "formula": "전체매출 / 전체마케팅비용", "color": "#6366f1"},
            {"id": "마케팅매출", "formula": "광고기여 + 인플루언서기여 + 쿠폰기여", "color": "#8b5cf6"},
            {"id": "마케팅비용", "formula": "광고비 + 인플루언서비용 + 쿠폰비용", "color": "#8b5cf6"},
            {"id": "광고기여매출", "formula": "채널별 광고 유입 매출", "color": "#06b6d4"},
            {"id": "인플루언서기여매출", "formula": "influencer_id별 net_sales 합산", "color": "#06b6d4"},
            {"id": "쿠폰기여매출", "formula": "coupon_id별 주문 매출 합산", "color": "#06b6d4"},
            {"id": "광고비", "formula": "Σ ad_costs.cost", "color": "#06b6d4"},
            {"id": "인플루언서비용", "formula": "Σ influencer_costs.cost", "color": "#06b6d4"},
            {"id": "쿠폰비용", "formula": "Σ discount_amount", "color": "#06b6d4"},
            {"id": "인스타그램광고", "formula": "비용/CTR/기여매출", "color": "#94a3b8"},
            {"id": "네이버광고", "formula": "비용/CTR/기여매출", "color": "#94a3b8"},
            {"id": "카카오광고", "formula": "비용/CTR/기여매출", "color": "#94a3b8"},
            {"id": "INF_A", "formula": "ROAS = 기여매출/비용", "color": "#94a3b8"},
            {"id": "INF_B", "formula": "ROAS = 기여매출/비용", "color": "#94a3b8"},
            {"id": "배송비쿠폰(C001)", "formula": "비용 vs 재구매율 30%", "color": "#94a3b8"},
            {"id": "금액쿠폰(C002)", "formula": "비용 vs 재구매율 10%", "color": "#94a3b8"},
        ]
        edges3 = [
            ("마케팅매출", "MER"), ("마케팅비용", "MER"),
            ("광고기여매출", "마케팅매출"),
            ("인플루언서기여매출", "마케팅매출"),
            ("쿠폰기여매출", "마케팅매출"),
            ("광고비", "마케팅비용"),
            ("인플루언서비용", "마케팅비용"),
            ("쿠폰비용", "마케팅비용"),
            ("인스타그램광고", "광고기여매출"), ("인스타그램광고", "광고비"),
            ("네이버광고", "광고기여매출"), ("네이버광고", "광고비"),
            ("카카오광고", "광고기여매출"), ("카카오광고", "광고비"),
            ("INF_A", "인플루언서기여매출"), ("INF_A", "인플루언서비용"),
            ("INF_B", "인플루언서기여매출"), ("INF_B", "인플루언서비용"),
            ("배송비쿠폰(C001)", "쿠폰기여매출"), ("배송비쿠폰(C001)", "쿠폰비용"),
            ("금액쿠폰(C002)", "쿠폰기여매출"), ("금액쿠폰(C002)", "쿠폰비용"),
        ]
        pos3 = {
            "MER": (5, 6),
            "마케팅매출": (3, 5), "마케팅비용": (7, 5),
            "광고기여매출": (1.5, 4), "인플루언서기여매출": (3.5, 4), "쿠폰기여매출": (5, 4),
            "광고비": (6.5, 4), "인플루언서비용": (8, 4), "쿠폰비용": (9, 4),
            "인스타그램광고": (0.5, 3), "네이버광고": (1.5, 3), "카카오광고": (2.5, 3),
            "INF_A": (3.5, 3), "INF_B": (4.5, 3),
            "배송비쿠폰(C001)": (5.5, 3), "금액쿠폰(C002)": (7, 3),
        }
        st.plotly_chart(draw_dag(nodes3, edges3, pos3), use_container_width=True)

    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("← 데이터 업로드로", use_container_width=True):
            st.session_state.page = "upload"
            st.rerun()
    with col2:
        if st.button("✅ 이 구조로 분석 시작", use_container_width=True, type="primary"):
            st.session_state.page = "dashboard"
            st.rerun()

# =============================================
# 페이지 4: 대시보드
# =============================================
def page_dashboard():
    data = st.session_state.data
    items = data["order_items"].copy()
    adjs = data["adjustments"].copy()
    products = data["products"].copy()
    inf_costs = data["influencer_costs"].copy()
    ad_costs = data["ad_costs"].copy()

    items["order_ts"] = pd.to_datetime(items["order_ts"])
    adjs["event_ts"] = pd.to_datetime(adjs["event_ts"])

    st.title("🔍 Causely — 루미에르 분석 대시보드")
    st.markdown("---")

    tab1, tab2, tab3 = st.tabs(["🔴 시나리오 1: 환불 이슈", "🎟️ 시나리오 2: 쿠폰 & 손익", "📊 전체 현황"])

    # ==========================
    # 시나리오 1
    # ==========================
    with tab1:
        st.subheader("🔴 1월 31일 — 대시보드엔 안정, Causely로 보면?")

        jan31_items = items[items["order_ts"].dt.date == pd.Timestamp("2026-01-31").date()]
        jan31_adjs = adjs[adjs["event_ts"].dt.date == pd.Timestamp("2026-01-31").date()]
        jan30_items = items[items["order_ts"].dt.date == pd.Timestamp("2026-01-30").date()]
        jan30_adjs = adjs[adjs["event_ts"].dt.date == pd.Timestamp("2026-01-30").date()]

        net_30 = jan30_items["net_sales_amount"].sum() + jan30_adjs["amount"].sum()
        net_31 = jan31_items["net_sales_amount"].sum() + jan31_adjs["amount"].sum()

        # 표면 지표
        st.markdown("#### 📊 표면 지표 (일반 대시보드에서 보이는 것)")
        col1, col2, col3 = st.columns(3)
        col1.metric("1월 30일 Net 매출", f"{net_30:,.0f}원")
        col2.metric("1월 31일 Net 매출", f"{net_31:,.0f}원", delta=f"{net_31-net_30:,.0f}원")
        col3.metric("전일 대비 변화", f"{(net_31-net_30)/net_30*100:.1f}%")
        st.success("✅ 일반 대시보드: 매출 안정적, 이상 없음")

        st.markdown("---")
        st.markdown("#### 🔍 Causely 드릴다운")

        c1, c2, c3 = st.columns(3)
        p010_ref = jan31_adjs[jan31_adjs["product_id"] == "P010"]["amount"].sum()
        inf_a_sales = jan31_items[jan31_items["influencer_id"] == "INF_A"]["net_sales_amount"].sum()
        net_impact = p010_ref + inf_a_sales

        c1.metric("🔴 그래프2: P010 환불 급증", f"{p010_ref:,.0f}원",
            delta=f"{jan31_adjs[jan31_adjs['product_id']=='P010'].shape[0]}건", delta_color="inverse")
        c2.metric("🟢 그래프3: INF_A 기여 매출", f"+{inf_a_sales:,.0f}원",
            delta=f"{jan31_items[jan31_items['influencer_id']=='INF_A'].shape[0]}건 주문")
        c3.metric("➡️ 상쇄 후 실제 영향", f"{net_impact:,.0f}원",
            delta="표면상 0에 수렴하지만 내부 이슈 존재")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**그래프 2 — 환불 드릴다운**")
            reason = jan31_adjs[jan31_adjs["product_id"] == "P010"]["reason_code"].value_counts()
            if len(reason):
                fig = px.pie(values=reason.values, names=reason.index,
                    title="P010 환불 사유", color_discrete_sequence=["#ef4444","#f97316","#fbbf24"])
                st.plotly_chart(fig, use_container_width=True)
            p010_info = products[products["product_id"] == "P010"].iloc[0]
            st.error(f"⚠️ 셀러 **{p010_info['seller_id']}** 품질 이슈 의심 (DEFECT 65%)")

        with col2:
            st.markdown("**그래프 3 — INF_A 마케팅 성과**")
            inf_a_cost = inf_costs[(inf_costs["influencer_id"] == "INF_A") &
                                   (inf_costs["month"] == "2026-01")]["cost"].sum()
            inf_a_roas = inf_a_sales / inf_a_cost if inf_a_cost > 0 else 0
            st.metric("INF_A 집행 비용", f"{inf_a_cost:,.0f}원")
            st.metric("INF_A 기여 매출", f"{inf_a_sales:,.0f}원")
            st.metric("INF_A ROAS", f"{inf_a_roas:.1f}x")
            st.info("💡 INF_A ROAS 양호 → 추가 협업 검토 가치 있음")

        st.markdown("---")
        if st.button("🤖 AI 권장 액션 보기", key="btn_s1"):
            with st.spinner("분석 중..."):
                prompt = f"""
패션 커머스 루미에르 1월 31일 분석:
- 표면 Net 매출: 30일 {net_30:,.0f}원 vs 31일 {net_31:,.0f}원 (안정적으로 보임)
- 실제: P010(코튼 브라렛 세트, S003 셀러) 환불 {p010_ref:,.0f}원, 환불사유 DEFECT 65%
- INF_A 인플루언서 기여 매출 +{inf_a_sales:,.0f}원 (비용 {inf_a_cost:,.0f}원, ROAS {inf_a_roas:.1f}x)으로 환불 상쇄

우선순위별 권장 액션 3가지를 실무적으로 작성해주세요.
1번은 S003 셀러 관련 액션
2번은 INF_A 관련 액션
3번은 모니터링 관련 액션"""
                resp = client.chat.completions.create(model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}])
                st.success(resp.choices[0].message.content)

    # ==========================
    # 시나리오 2
    # ==========================
    with tab2:
        st.subheader("🎟️ 매출은 올랐는데 영업이익은 그대로?")

        dec_items = items[items["order_ts"].dt.month == 12]
        jan_items2 = items[items["order_ts"].dt.month == 1]
        dec_adjs2 = adjs[adjs["event_ts"].dt.month == 12]
        jan_adjs2 = adjs[adjs["event_ts"].dt.month == 1]

        dec_gross = dec_items["net_sales_amount"].sum()
        jan_gross = jan_items2["net_sales_amount"].sum()
        dec_coupon = dec_items["discount_amount"].sum()
        jan_coupon = jan_items2["discount_amount"].sum()
        dec_net = dec_gross + dec_adjs2["amount"].sum() - dec_coupon
        jan_net = jan_gross + jan_adjs2["amount"].sum() - jan_coupon

        # 광고비 + 인플루언서 비용
        jan_ad = ad_costs[ad_costs["month"] == "2026-01"]["cost"].sum()
        dec_ad = ad_costs[ad_costs["month"] == "2025-12"]["cost"].sum()
        jan_inf = inf_costs[inf_costs["month"] == "2026-01"]["cost"].sum()
        dec_inf = inf_costs[inf_costs["month"] == "2025-12"]["cost"].sum()

        jan_total_mkt = jan_coupon + jan_ad + jan_inf
        dec_total_mkt = dec_coupon + dec_ad + dec_inf
        jan_MER = jan_gross / jan_total_mkt if jan_total_mkt > 0 else 0
        dec_MER = dec_gross / dec_total_mkt if dec_total_mkt > 0 else 0

        st.markdown("#### 📊 그래프 1: 매출 변화")
        col1, col2, col3 = st.columns(3)
        col1.metric("12월 총매출", f"{dec_gross:,.0f}원")
        col2.metric("1월 총매출", f"{jan_gross:,.0f}원",
            delta=f"+{(jan_gross/dec_gross-1)*100:.1f}%")
        col3.metric("매출 증가분", f"+{jan_gross-dec_gross:,.0f}원")
        st.success("✅ 일반 대시보드: 매출 5% 이상 증가, 좋은 신호!")

        st.markdown("---")
        st.markdown("#### 🔍 그래프 2: 손익 드릴다운")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("광고비 증가", f"+{jan_ad-dec_ad:,.0f}원",
            delta=f"+{(jan_ad/dec_ad-1)*100:.0f}%", delta_color="inverse")
        col2.metric("인플루언서 비용 증가", f"+{jan_inf-dec_inf:,.0f}원",
            delta=f"+{(jan_inf/dec_inf-1)*100:.0f}%", delta_color="inverse")
        col3.metric("쿠폰 비용 증가", f"+{jan_coupon-dec_coupon:,.0f}원",
            delta=f"+{(jan_coupon/dec_coupon-1)*100:.0f}%", delta_color="inverse")
        col4.metric("순매출 변화", f"{jan_net-dec_net:,.0f}원")
        st.error("🚨 매출 증가분을 마케팅 비용이 대부분 상쇄!")

        st.markdown("---")
        st.markdown("#### 📣 그래프 3: MER 비교")
        col1, col2, col3 = st.columns(3)
        col1.metric("12월 MER", f"{dec_MER:.1f}x",
            help="전체매출/전체마케팅비용")
        col2.metric("1월 MER", f"{jan_MER:.1f}x",
            delta=f"{jan_MER-dec_MER:.1f}x", delta_color="inverse")
        col3.metric("총 마케팅비용 증가", f"+{jan_total_mkt-dec_total_mkt:,.0f}원")

        # 마케팅 비용 구성 파이차트
        col1, col2 = st.columns(2)
        with col1:
            mkt_breakdown = pd.DataFrame({
                "항목": ["광고비", "인플루언서비용", "쿠폰비용"],
                "12월": [dec_ad, dec_inf, dec_coupon],
                "1월": [jan_ad, jan_inf, jan_coupon],
            })
            fig_mkt = go.Figure()
            fig_mkt.add_trace(go.Bar(name="12월", x=mkt_breakdown["항목"], y=mkt_breakdown["12월"],
                marker_color="#94a3b8"))
            fig_mkt.add_trace(go.Bar(name="1월", x=mkt_breakdown["항목"], y=mkt_breakdown["1월"],
                marker_color="#6366f1"))
            fig_mkt.update_layout(barmode="group", title="마케팅 비용 구성 비교",
                height=300, legend=dict(orientation="h"))
            st.plotly_chart(fig_mkt, use_container_width=True)
        with col2:
            fig_pie = px.pie(
                values=[jan_ad, jan_inf, jan_coupon],
                names=["광고비", "인플루언서비용", "쿠폰비용"],
                title="1월 마케팅 비용 구성",
                color_discrete_sequence=["#6366f1", "#8b5cf6", "#06b6d4"]
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        st.markdown("---")
        st.markdown("#### 🔍 쿠폰별 재구매율")

        coupon_users = jan_items2[jan_items2["coupon_id"].notna()][["user_id","coupon_id"]].drop_duplicates("user_id")
        repurchase_data = []
        for label, cid, cost_per_user in [
            ("배송비 쿠폰(C001)", "C001", 3000),
            ("금액 쿠폰(C002)", "C002", 3000)
        ]:
            uids = coupon_users[coupon_users["coupon_id"] == cid]["user_id"].unique()
            repurchase = sum(1 for uid in uids if jan_items2[jan_items2["user_id"] == uid].shape[0] >= 2)
            rate = repurchase / len(uids) * 100 if len(uids) > 0 else 0
            total_cost = len(uids) * cost_per_user
            repurchase_data.append({
                "쿠폰 타입": label, "사용자": len(uids),
                "재구매자": repurchase, "재구매율(%)": round(rate, 1),
                "총 쿠폰비용": f"{total_cost:,}원"
            })
        no_coupon = jan_items2[jan_items2["coupon_id"].isna()]["user_id"].unique()
        repurchase_nc = sum(1 for uid in no_coupon if jan_items2[jan_items2["user_id"] == uid].shape[0] >= 2)
        repurchase_data.append({
            "쿠폰 타입": "미사용", "사용자": len(no_coupon),
            "재구매자": repurchase_nc,
            "재구매율(%)": round(repurchase_nc/len(no_coupon)*100 if len(no_coupon) > 0 else 0, 1),
            "총 쿠폰비용": "0원"
        })

        df_ret = pd.DataFrame(repurchase_data)
        st.dataframe(df_ret, use_container_width=True, hide_index=True)

        col1, col2 = st.columns(2)
        with col1:
            fig_ret = px.bar(df_ret, x="쿠폰 타입", y="재구매율(%)", color="쿠폰 타입",
                title="쿠폰 타입별 재구매율",
                color_discrete_sequence=["#6366f1","#f97316","#94a3b8"])
            fig_ret.update_layout(showlegend=False)
            st.plotly_chart(fig_ret, use_container_width=True)
        with col2:
            st.info("💡 배송비 쿠폰(C001) 재구매율 30% vs 금액쿠폰 10%\n\n같은 비용(3,000원)인데 배송비 쿠폰이 3배 효율!")

        st.markdown("---")
        if st.button("🤖 AI 권장 액션 보기", key="btn_s2"):
            with st.spinner("분석 중..."):
                prompt = f"""
패션 커머스 루미에르 12월→1월 손익 분석:
- 매출: +{(jan_gross/dec_gross-1)*100:.1f}% 증가 (좋아 보임)
- 쿠폰 비용: +{(jan_coupon/dec_coupon-1)*100:.0f}% 증가 → 순매출 실질 증가 미미
- 쿠폰 효율: 배송비쿠폰(C001) 재구매율 30% vs 금액쿠폰(C002) 10% (비용 동일 3,000원)

우선순위별 권장 액션 3가지:
1번은 쿠폰 전략 조정
2번은 배송비 쿠폰 실험 설계
3번은 손익 모니터링"""
                resp = client.chat.completions.create(model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}])
                st.success(resp.choices[0].message.content)

    # ==========================
    # 전체 현황
    # ==========================
    with tab3:
        st.subheader("📊 전체 현황")
        jan_items3 = items[items["order_ts"].dt.month == 1]
        jan_adjs3 = adjs[adjs["event_ts"].dt.month == 1]

        daily_gross = jan_items3.groupby(jan_items3["order_ts"].dt.date)["net_sales_amount"].sum().reset_index()
        daily_gross.columns = ["date", "gross"]
        daily_refund = jan_adjs3.groupby(jan_adjs3["event_ts"].dt.date)["amount"].sum().reset_index()
        daily_refund.columns = ["date", "refund"]
        daily = daily_gross.merge(daily_refund, on="date", how="left").fillna(0)
        daily["net"] = daily["gross"] + daily["refund"]

        fig = go.Figure()
        fig.add_trace(go.Bar(x=daily["date"], y=daily["gross"], name="Gross 매출", marker_color="#6366f1"))
        fig.add_trace(go.Bar(x=daily["date"], y=daily["refund"], name="환불", marker_color="#ef4444"))
        fig.add_trace(go.Scatter(x=daily["date"], y=daily["net"], name="Net 매출",
            line=dict(color="#f59e0b", width=2.5), mode="lines+markers"))
        fig.update_layout(barmode="relative", height=350, legend=dict(orientation="h"))
        st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            channel = jan_items3.groupby("channel")["net_sales_amount"].sum().reset_index()
            fig_ch = px.pie(channel, values="net_sales_amount", names="channel", title="채널별 매출 비중")
            st.plotly_chart(fig_ch, use_container_width=True)
        with col2:
            cat = jan_items3.merge(products[["product_id","category"]], on="product_id")
            cat_sales = cat.groupby("category")["net_sales_amount"].sum().sort_values(ascending=True).reset_index()
            fig_cat = px.bar(cat_sales, x="net_sales_amount", y="category",
                title="카테고리별 매출", orientation="h", color_discrete_sequence=["#6366f1"])
            st.plotly_chart(fig_cat, use_container_width=True)

    st.markdown("---")
    if st.button("← 처음으로"):
        st.session_state.page = "onboarding"
        st.session_state.data = {}
        st.rerun()

# =============================================
# 라우터
# =============================================
if st.session_state.page == "onboarding":
    page_onboarding()
elif st.session_state.page == "upload":
    page_upload()
elif st.session_state.page == "dag":
    page_dag()
elif st.session_state.page == "dashboard":
    page_dashboard()
