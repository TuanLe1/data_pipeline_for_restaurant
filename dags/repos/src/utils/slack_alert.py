import logging
import json
import requests
import traceback
from datetime import datetime
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

class SlackAlert:
    def __init__(self, config: dict):
        self.webhook_url = config.get("Slack-Webhook-URL")
        self.env = config.get("Environment", "Production")
        self.enabled = bool(self.webhook_url)

        # Configure Bot name and Icon here
        self.bot_name = "ETL Pipeline Report" 
        self.bot_icon = ":chart_with_upwards_trend:"

        if not self.enabled:
            logger.warning("Slack Webhook URL not found. Alert is disabled.")

    def _send_payload(self, payload: dict):
        if not self.enabled: return
        
        try:
            payload["username"] = self.bot_name
            payload["icon_emoji"] = self.bot_icon
            
            headers = {'Content-Type': 'application/json'}
            response = requests.post(self.webhook_url, data=json.dumps(payload), headers=headers, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"Failed to send Slack alert. Status: {response.status_code}, Resp: {response.text}")
        except Exception as e:
            logger.error(f"Exception sending Slack alert: {e}")

    def send_success(self, message: str, duration: str = None):
        if not self.enabled: return

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "✅ ETL Pipeline Success",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Environment:*\n{self.env}"},
                    {"type": "mrkdwn", "text": f"*Time:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
                ]
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Message:*\n{message}"}
            }
        ]
        
        if duration:
            blocks[1]["fields"].append({"type": "mrkdwn", "text": f"*Duration:*\n{duration}"})

        payload = {
            "attachments": [
                {
                    "color": "#36a64f",
                    "blocks": blocks
                }
            ]
        }
        self._send_payload(payload)

    def send_error(self, error_msg: str, error_obj: Exception = None):
        if not self.enabled: return

        tb_str = ""
        if error_obj:
            tb_str = "".join(traceback.format_exception(None, error_obj, error_obj.__traceback__))
            if len(tb_str) > 1500: tb_str = tb_str[-1500:]

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 ETL Pipeline FAILED",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Environment:*\n{self.env}"},
                    {"type": "mrkdwn", "text": f"*Time:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
                ]
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Error Summary:*\n{error_msg}"}
            }
        ]

        if tb_str:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Traceback:*\n```{tb_str}```"}
            })

        payload = {
            "attachments": [
                {
                    "color": "#ff0000",
                    "blocks": blocks
                }
            ]
        }
        self._send_payload(payload)
    
def task_fail_slack_alert(context):
    """
    Hàm này đóng vai trò cầu nối:
    1. Lấy Webhook từ Airflow Connection ('slack_conn').
    2. Lấy thông tin lỗi từ Airflow Context.
    3. Gọi class SlackAlert để bắn tin nhắn đi.
    """
    try:
        # 1. Lấy Webhook URL từ Connection mà bạn đã tạo trên UI
        # (Lưu ý: Nếu bạn lưu URL trong ô Password thì dùng conn.password, nếu Host thì dùng conn.host)
        conn = BaseHook.get_connection('slack_conn')
        webhook_url = conn.password if conn.password else conn.host
        
        if not webhook_url:
            logger.warning("⚠️ No Slack Webhook found in connection 'slack_conn'.")
            return

        # 2. Tạo config giả lập để khởi tạo Class SlackAlert
        config = {
            "Slack-Webhook-URL": webhook_url,
            "Environment": "Airflow Production"
        }
        
        # 3. Lấy thông tin task bị lỗi
        ti = context.get('task_instance')
        exception = context.get('exception')
        dag_id = ti.dag_id
        task_id = ti.task_id
        log_url = ti.log_url
        
        # Tạo nội dung thông báo
        error_msg = (
            f"🔴 *Task Failed!*\n"
            f"• *DAG:* `{dag_id}`\n"
            f"• *Task:* `{task_id}`\n"
            f"• *Logs:* <{log_url}|Click here to view logs>"
        )

        # 4. Gửi Alert
        alerter = SlackAlert(config)
        alerter.send_error(error_msg=error_msg, error_obj=exception)
        
    except Exception as e:
        logger.error(f"Failed to send Airflow Slack Alert: {e}")