import requests
import json

SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"


def _to_manwon(val):
    """원 단위를 만원으로 (소수점 1자리)."""
    return f"{val / 10000:.1f}만원"


def send_morning_briefing(
    sales_amount,
    sales_pct,
    order_count,
    order_delta,
    good_signals,
    warning_signals,
    actions,
):
    """
    오늘 아침 브리핑 형식으로 슬랙 전송.
    sales_amount: 어제 매출(원), sales_pct: 증감%, order_count: 주문건수, order_delta: 주문 증감건
    good_signals, warning_signals, actions: 문자열 리스트
    """
    pct_str = f"+{sales_pct:.1f}%" if sales_pct >= 0 else f"{sales_pct:.1f}%"
    delta_str = f"+{order_delta}건" if order_delta >= 0 else f"{order_delta}건"

    lines = [
        "📊 *오늘 아침 브리핑 (코즐리)*",
        "",
        f"어제 매출 {_to_manwon(sales_amount)} ({pct_str})",
        f"주문 {order_count}건 ({delta_str})",
        "",
        "*좋은 신호*",
    ]
    for s in good_signals:
        lines.append(f"• {s}")
    lines.extend(["", "*주의 신호*"])
    for s in warning_signals:
        lines.append(f"• {s}")
    lines.extend(["", "👉 *오늘 해야 할 것*"])
    for s in actions:
        lines.append(f"• {s}")

    text = "\n".join(lines)

    message = {
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": text},
            }
        ]
    }

    requests.post(
        SLACK_WEBHOOK_URL,
        data=json.dumps(message),
        headers={"Content-Type": "application/json"},
    )


def send_alert(title, cause, action):
    message = {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"🚨 {title}"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*원인 추정*\n{cause}"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*권장 행동*\n{action}"},
            },
        ]
    }

    requests.post(
        SLACK_WEBHOOK_URL,
        data=json.dumps(message),
        headers={"Content-Type": "application/json"},
    )
