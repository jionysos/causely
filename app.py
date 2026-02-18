import streamlit as st
import pandas as pd
from datetime import date
import core

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

st.subheader("2) 리포트 생성")
today = st.date_input("기준일", value=pd.to_datetime("2026-01-31").date())

if st.button("오늘 리포트 생성"):
    evidence = core.build_evidence(today, orders, items, adj, products)
    briefing = core.generate_briefing(evidence)

    st.subheader("브리핑")
    st.write(briefing["headline"])

    st.markdown("### Key findings")
    for x in briefing["key_findings"]:
        st.write("-", x)

    st.markdown("### Actions")
    for a in briefing["actions"]:
        st.write(f"**{a['title']}**")
        st.write("-", a["why"])
        if a.get("expected_impact"):
            st.write("-", a["expected_impact"])

    with st.expander("Evidence (debug)"):
        st.json(evidence)
