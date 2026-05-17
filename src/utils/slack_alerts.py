"""Slack webhook alerting for DAG failure callbacks."""

import json
from typing import Any

import requests

from src.config import SLACK_WEBHOOK_URL
from src.utils.logging_config import get_logger

logger = get_logger(__name__, stage="alerting")


def send_failure_alert(context: dict[str, Any]) -> None:
    """Post a formatted failure message to Slack.

    Designed to be wired directly into Airflow's ``on_failure_callback``.
    Silently no-ops when SLACK_WEBHOOK_URL is not configured so local dev
    does not require a real webhook.
    """
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping Slack alert")
        return

    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"
    execution_date = str(context.get("execution_date", "unknown"))
    log_url = context.get("task_instance").log_url if context.get("task_instance") else "N/A"
    exception = str(context.get("exception", "No exception details available"))

    payload = {
        "attachments": [
            {
                "color": "#e01e5a",
                "fallback": f"DAG {dag_id} / Task {task_id} FAILED",
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": ":red_circle: Airflow Task Failed"},
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
                            {"type": "mrkdwn", "text": f"*Task:*\n`{task_id}`"},
                            {"type": "mrkdwn", "text": f"*Execution Date:*\n{execution_date}"},
                            {"type": "mrkdwn", "text": f"*Log URL:*\n<{log_url}|View Logs>"},
                        ],
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Exception:*\n```{exception[:500]}```",
                        },
                    },
                ],
            }
        ]
    }

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        logger.info("Slack alert sent for DAG=%s task=%s", dag_id, task_id)
    except requests.RequestException as exc:
        logger.error("Failed to send Slack alert: %s", exc)
