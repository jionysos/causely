import streamlit as st
import pandas as pd
import numpy as np
import networkx as nx
import plotly.graph_objects as go
from datetime import datetime, timedelta
from openai import OpenAI
import os
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ---- 데이터 생성 ----
def generate_weekly_data():
    np.random.seed(42)
    dates = [datetime.today() - timedelta(days=i) for i in range(14, 0, -1)]
    data = []
    for i, date in enumerate(dates):
        campaign_active = i < 6
        traffic = int(np.random.normal(1000, 50) * (1.3 if campaign_active else 1.0))
        conversion_rate = round(np.random.normal(0.05, 0.005) * (1.0 if campaign_active else 0.9), 4)
        avg_order_value = round(np.random.normal(50000, 2000), 0)
        returning_rate = round(np.random.normal(0.3, 0.02) * (1.0 if campaign_active else 0.85), 4)
        revenue = int(traffic * conversion_rate * avg_order_value * (1 + returning_rate))
        data.append({
            "date": date.strftime("%Y-%m-%d"),
            "revenue": revenue,
            "traffic": traffic,
            "conversion_rate": conversion_rate,
            "avg_order_value": avg_order_value,
            "returning_rate": returning_rate,
            "campaign_active": campaign_active
        })
    return pd.DataFrame(data)

# ---- Knowledge Graph ----
def build_knowledge_graph():
    G = nx.DiGraph()
    nodes = ["매출", "유입", "전환율", "객단가", "재구매율", "캠페인", "가격정책", "상품구성", "SEO"]
    G.add_nodes_from(nodes)
    edges = [
        ("유입", "매출"), ("전환율", "매출"), ("객단가", "매출"), ("재구매율", "매출"),
        ("캠페인", "유입"), ("캠페인", "전환율"),
        ("가격정책", "전환율"), ("가격정책", "객단가"),
        ("상품구성", "객단가"), ("상품구성", "재구매율"),
        ("SEO", "유입"),
    ]
    G.add_edges_from(edges)
    return G

# ---- 이상 감지 ----
def detect_anomalies(df):
    metrics = ["revenue", "traffic", "conversion_rate", "avg_order_value", "returning_rate"]
    this_week = df.tail(7)[metrics].mean()
    last_week = df.head(7)[metrics].mean()
    anomalies = {}
    for metric in metrics:
        change = (this_week[metric] - last_week[metric]) / last_week[metric] * 100
        anomalies[metric] = round(change, 1)
    return anomalies

# ---- LLM 분석 ----
def analyze_with_llm(anomalies, G):
    graph_structure = list(G.edges())
    prompt = f"""
당신은 비즈니스 데이터 분석 전문가입니다.
아래는 이번 주 vs 지난 주 지표 변화율(%)입니다:
{anomalies}

아래는 지표 간 인과관계 구조입니다:
{graph_structure}

위 데이터를 바탕으로:
1. 가장 심각한 문제가 무엇인지
2. 근본 원인이 무엇인지 (Knowledge Graph 기반으로 추론)
3. 권장 액션이 무엇인지

3-4문장으로 명확하게 설명해주세요.
"""
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content

# ---- Plotly 그래프 시각화 ----
def draw_graph(G, anomalies):
    pos = {
        "매출": (2, 3), "유입": (1, 2), "전환율": (2, 2),
        "객단가": (3, 2), "재구매율": (4, 2),
        "캠페인": (0.5, 1), "SEO": (1.5, 1),
        "가격정책": (2.5, 1), "상품구성": (3.5, 1)
    }
    metric_map = {
        "유입": "traffic", "전환율": "conversion_rate",
        "객단가": "avg_order_value", "재구매율": "returning_rate", "매출": "revenue"
    }
    def node_color(node):
        metric = metric_map.get(node)
        if metric and metric in anomalies:
            val = anomalies[metric]
            if val < -20: return "red"
            elif val < -5: return "orange"
        return "skyblue"

    edge_x, edge_y = [], []
    for u, v in G.edges():
        x0, y0 = pos[u]; x1, y1 = pos[v]
        edge_x += [x0, x1, None]; edge_y += [y0, y1, None]

    node_x = [pos[n][0] for n in G.nodes()]
    node_y = [pos[n][1] for n in G.nodes()]
    node_colors = [node_color(n) for n in G.nodes()]
    node_text = [f"{n}<br>{anomalies.get(metric_map.get(n,''), '')}%" if metric_map.get(n) else n for n in G.nodes()]

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=edge_x, y=edge_y, mode='lines', line=dict(color='gray', width=1.5), hoverinfo='none'))
    fig.add_trace(go.Scatter(x=node_x, y=node_y, mode='markers+text', text=list(G.nodes()),
        textposition="top center", marker=dict(size=40, color=node_colors, line=dict(width=2, color='white')),
        hovertext=node_text, hoverinfo='text'))
    fig.update_layout(showlegend=False, margin=dict(l=0,r=0,t=0,b=0),
        xaxis=dict(showgrid=False, zeroline=False, visible=False),
        yaxis=dict(showgrid=False, zeroline=False, visible=False),
        height=350, plot_bgcolor='white')
    return fig

# ---- Streamlit UI ----
st.set_page_config(page_title="Causely", page_icon="🔍", layout="wide")
st.title("🔍 Causely")
st.caption("비즈니스 데이터의 'Why'를 자동으로 찾아주는 AI 분석 어시스턴트")

df = generate_weekly_data()
G = build_knowledge_graph()
anomalies = detect_anomalies(df)

col1, col2, col3, col4, col5 = st.columns(5)
metrics_kr = {"revenue": "매출", "traffic": "유입", "conversion_rate": "전환율", "avg_order_value": "객단가", "returning_rate": "재구매율"}
for col, (metric, label) in zip([col1,col2,col3,col4,col5], metrics_kr.items()):
    val = anomalies[metric]
    col.metric(label, f"{val}%", delta=f"{val}%")

st.markdown("---")
col_left, col_right = st.columns([1.2, 1])

with col_left:
    st.subheader("📊 Knowledge Graph")
    fig = draw_graph(G, anomalies)
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("💬 AI에게 물어보기")
    question = st.text_input("질문을 입력하세요", placeholder="이번 주 매출 왜 떨어졌어?")
    if st.button("분석하기 🔍") or question:
        with st.spinner("분석 중..."):
            result = analyze_with_llm(anomalies, G)
        st.success(result)

st.markdown("---")
st.subheader("📈 매출 트렌드")
st.line_chart(df.set_index("date")["revenue"])