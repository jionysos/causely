import sqlite3
import pandas as pd
import json
import os
from openai import OpenAI

def _client():
    return OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# =============================================
# 1. CSV → SQLite 인메모리 DB 로드
# =============================================
def load_db(data: dict) -> sqlite3.Connection:
    """
    data: {"orders": df, "order_items": df, ...} 형태의 딕셔너리
    SQLite 인메모리 DB에 로드하고 connection 반환
    """
    conn = sqlite3.connect(":memory:")
    for table_name, df in data.items():
        df.to_sql(table_name, conn, index=False, if_exists="replace")
    return conn


# =============================================
# 2. 스키마 정보 자동 추출
# =============================================
def get_schema(conn: sqlite3.Connection) -> str:
    """
    DB에 있는 테이블/컬럼 정보를 LLM이 읽기 좋은 문자열로 반환
    """
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]

    schema_lines = []
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()
        col_str = ", ".join(f"{c[1]}({c[2]})" for c in cols)

        # 샘플 데이터 1행
        cursor.execute(f"SELECT * FROM {table} LIMIT 1")
        sample = cursor.fetchone()

        schema_lines.append(f"TABLE: {table}")
        schema_lines.append(f"  COLUMNS: {col_str}")
        if sample:
            schema_lines.append(f"  SAMPLE: {dict(zip([c[1] for c in cols], sample))}")
        schema_lines.append("")

    return "\n".join(schema_lines)


# =============================================
# 3. 자연어 → SQL 생성
# =============================================
def generate_sql(question: str, schema: str) -> str:
    prompt = f"""
당신은 SQLite SQL 전문가입니다.
아래 스키마를 보고 질문에 맞는 SQL 쿼리를 작성하세요.

## 스키마
{schema}

## 비즈니스 컨텍스트
- order_items.net_sales_amount: 실제 판매금액 (할인 적용 후)
- order_items.discount_amount: 할인/쿠폰 금액
- adjustments.amount: 환불금액 (음수)
- adjustments.reason_code: DEFECT(불량), SIZE(사이즈), CHANGE_MIND(단순변심), DELIVERY(배송)
- products.seller_id: 셀러 ID (S001, S002, S003)
- order_items.influencer_id: 인플루언서 ID (INF_A, INF_B 등, NULL이면 일반 구매)
- order_items.coupon_id: 쿠폰 ID (C001=배송비, C002=금액할인, NULL이면 미사용)

## 질문
{question}

## 규칙
- SQLite 문법만 사용하세요
- 날짜 필터는 strftime 또는 LIKE '2026-01-%' 형식 사용
- 금액은 SUM, AVG 등 집계함수 사용
- ORDER BY, LIMIT으로 상위 결과만 반환
- SQL 쿼리만 반환하세요. 설명 없이.
"""
    client = _client()
    resp = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "Return ONLY the SQL query. No explanation. No markdown."},
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )
    sql = resp.choices[0].message.content.strip()
    # 마크다운 코드블록 제거
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql


# =============================================
# 4. SQL 실행
# =============================================
def execute_sql(sql: str, conn: sqlite3.Connection) -> tuple[pd.DataFrame, str]:
    """
    SQL 실행 후 (DataFrame, error_message) 반환
    """
    try:
        df = pd.read_sql_query(sql, conn)
        return df, None
    except Exception as e:
        return None, str(e)


# =============================================
# 5. 결과 → 자연어 해석
# =============================================
def interpret_result(question: str, sql: str, result_df: pd.DataFrame) -> str:
    """
    SQL 결과를 사람이 읽기 좋은 답변으로 변환
    """
    if result_df is None or result_df.empty:
        return "해당 조건에 맞는 데이터가 없어요."

    result_str = result_df.to_string(index=False)

    prompt = f"""
당신은 패션 커머스 데이터 분석가입니다.

## 사용자 질문
{question}

## SQL 실행 결과
{result_str}

## 답변 규칙
- 데이터에 있는 숫자만 사용하세요 (절대 만들어내지 마세요)
- 핵심 수치를 먼저 말하고, 그 의미를 설명하세요
- 비즈니스 임팩트나 액션 포인트가 있으면 한 줄 추가하세요
- 3~5문장으로 간결하게 작성하세요
- 한국어로 답변하세요
"""
    client = _client()
    resp = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a Korean fashion e-commerce analyst. Be concise and data-driven."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.2
    )
    return resp.choices[0].message.content.strip()


# =============================================
# 6. 메인 함수 - 질문 하나로 전체 파이프라인
# =============================================
def answer_question(question: str, conn: sqlite3.Connection, schema: str) -> dict:
    """
    질문 → SQL 생성 → 실행 → 해석 → 반환
    반환: {"question": ..., "sql": ..., "result": df, "answer": ..., "error": ...}
    """
    # SQL 생성
    sql = generate_sql(question, schema)

    # SQL 실행
    result_df, error = execute_sql(sql, conn)

    if error:
        # SQL 오류 시 자동 재시도 (오류 메시지 포함해서 재생성)
        retry_prompt = f"이전 SQL에서 오류가 발생했습니다: {error}\n질문: {question}\n다시 작성해주세요."
        sql = generate_sql(retry_prompt, schema)
        result_df, error = execute_sql(sql, conn)

    if error:
        return {
            "question": question,
            "sql": sql,
            "result": None,
            "answer": f"SQL 실행 오류: {error}",
            "error": error
        }

    # 결과 해석
    answer = interpret_result(question, sql, result_df)

    return {
        "question": question,
        "sql": sql,
        "result": result_df,
        "answer": answer,
        "error": None
    }
