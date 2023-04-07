import os
import io
import csv
import json
import boto3
import requests
from datetime import datetime
from typing import List, Dict, Union
from pydantic import BaseModel


class LogMessage(BaseModel):
    message: str
    severity: str
    source: str
    timestamp: datetime
        
class SlackMessage(BaseModel):
    text: str
    blocks: List[Dict[str, Union[str, List[Dict[str, Union[str, str]]]]]]
    attachments: List[Dict[str, Union[str, List[Dict[str, Union[str, str]]]]]] = []



class SlackLogger:
    def __init__(self, log_group_name: str, filter_pattern: str):
        self.log_group_name = log_group_name
        self.filter_pattern = filter_pattern
        self.client = boto3.client("logs")

    def get_log_messages(self, start_time: datetime, end_time: datetime) -> List[LogMessage]:
        response = self.client.filter_log_events(
            logGroupName=self.log_group_name,
            startTime=int(start_time.timestamp() * 1000),
            endTime=int(end_time.timestamp() * 1000),
            filterPattern=self.filter_pattern
        )
        return [
            LogMessage(
                message=json.loads(event["message"])["message"],
                severity=json.loads(event["message"])["severity"],
                source=json.loads(event["message"])["logger"]["name"],
                timestamp=datetime.fromtimestamp(event["timestamp"] / 1000)
            ) for event in response["events"]
        ]

    def format_slack_message(self, log_message: LogMessage, include_csv: bool) -> SlackMessage
        severity = log_message.severity
        source = log_message.source
        message_text = log_message.message
        timestamp = log_message.timestamp
        pretty_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        slack_message = {
            "text": f"{pretty_timestamp}\n{message_text}",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{severity}* - {source}\n{pretty_timestamp}\n{message_text}"
                    },
                    "color": "danger" if severity == "ERROR" else "warning"
                }
            ]
        }
        if include_csv:
            log_event = log_message.message.strip().replace('\n', ' ')
            csv_message = [timestamp.isoformat(), severity, source, log_event]
            slack_message["attachments"] = [
                {
                    "fallback": "CSV of error logs",
                    "text": "CSV of error logs",
                    "fields": [
                        {"title": "Timestamp", "value": timestamp.isoformat()},
                        {"title": "Severity", "value": severity},
                        {"title": "Source", "value": source},
                        {"title": "Message", "value": log_event},
                    ]
                }
            ]
            slack_message["attachments"][0]["text"] = self.get_csv_message([csv_message])
        return slack_message

    def get_csv_message(self, messages: List[List[str]]) -> str:
        file = io.StringIO()
        writer = csv.writer(file)
        writer.writerow(["Timestamp", "Severity", "Source", "Message"])
        for message in messages:
            writer.writerow(message)
        return file.getvalue()

    def post_to_slack(self, slack_webhook_url: str, messages: List[LogMessage]) -> None:
        for message in messages:
            response = requests.post(slack_webhook_url,
                                     json=message,
                                     headers={"Content-Type": "application/json"})
            if response.status_code != 200:
                raise ValueError("Failed to post to Slack")

    def log_to_slack(self, start_time, end_time, send_csv=False):
        messages = self.get_log_messages(start_time, end_time)
        slack_messages = [self.format_slack_message(message, send_csv) for message in messages]
        slack_webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
        if slack_webhook_url:
            self.post_to_slack(slack_webhook_url, slack_messages)
        else:
            print(slack_messages)
