import os
import io
import csv
import json
import boto3
import requests


class SlackLogger:
    def __init__(self, log_group_name, filter_pattern):
        self.log_group_name = log_group_name
        self.filter_pattern = filter_pattern
        self.client = boto3.client("logs")

    def get_log_messages(self, start_time, end_time):
        response = self.client.filter_log_events(
            logGroupName=self.log_group_name,
            startTime=start_time,
            endTime=end_time,
            filterPattern=self.filter_pattern
        )
        return response["events"]

    def format_slack_message(self, log_message, include_csv):
        message = json.loads(log_message["message"])
        severity = message["severity"]
        source = message["logger"]["name"]
        message_text = message["message"]
        timestamp = log_message["timestamp"]
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
            log_event = log_message["message"].strip().replace('\n', ' ')
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

    def get_csv_message(self, messages):
        file = io.StringIO()
        writer = csv.writer(file)
        writer.writerow(["Timestamp", "Severity", "Source", "Message"])
        for message in messages:
            writer.writerow(message)
        return file.getvalue()

    def post_to_slack(self, slack_webhook_url, messages):
        for message in messages:
            response = requests.post(slack_webhook_url, json=message)
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
