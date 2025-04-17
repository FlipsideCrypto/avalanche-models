import datetime
import requests
import json
import sys
import os


def log_test_result():
    """Reads the run_results.json file and returns a dictionary of failed test results"""

    filepath = "target/run_results.json"

    with open(filepath) as f:
        run = json.load(f)

    logs = []
    failed_messages = []
    test_count = 0
    fail_count = 0

    for test in run["results"]:
        test_count += 1
        if test["status"] == "fail":
            logs.append(test)
            message = f"{test['failures']} record failure(s) in {test['unique_id']}"
            failed_messages.append(message)
            fail_count += 1

    dbt_test_result = {
        "logs": logs,
        "failed_messages": failed_messages,
        "test_count": test_count,
        "fail_count": fail_count,
        "elapsed_time": str(datetime.timedelta(seconds=run["elapsed_time"]))
    }

    return dbt_test_result


def create_message(**kwargs):
    messageBody = {
        "text": f"Hey{' <!here>' if kwargs['fail_count'] > 0 else ''}, DBT test failures for :{os.environ.get('DATABASE').split('_DEV')[0]}: {os.environ.get('DATABASE')}",
        "attachments": [
            {
                "color": "#f44336",  # Red color for failures
                "fields": [
                    {
                        "title": "Total Tests Run",
                        "value": kwargs["test_count"],
                        "short": True
                    },
                    {
                        "title": "Total Time Elapsed",
                        "value": kwargs["elapsed_time"],
                        "short": True
                    },
                    {
                        "title": "Number of Failed Tests",
                        "value": kwargs['fail_count'],
                        "short": True
                    },
                    {
                        "title": "Failed Tests:",
                        "value": "\n".join(kwargs["failed_messages"]) if len(kwargs["failed_messages"]) > 0 else "None",
                        "short": False
                    }
                ],
                "actions": [
                    {
                        "type": "button",
                        "text": "View Workflow Run",
                        "style": "primary",
                        "url": f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/{os.environ.get('GITHUB_REPOSITORY')}/actions/runs/{os.environ.get('GITHUB_RUN_ID')}"
                    }
                ]
            }
        ]
    }

    return messageBody


def send_alert(webhook_url):
    """Sends a message to a slack channel if there are failures"""

    url = webhook_url

    data = log_test_result()
    
    # Only proceed if there are failures
    if data["fail_count"] == 0:
        print("No test failures found. Skipping Slack notification.")
        return

    send_message = create_message(
        fail_count=data["fail_count"],
        test_count=data["test_count"],
        failed_messages=data["failed_messages"],
        elapsed_time=data["elapsed_time"]
    )

    response = requests.post(url, json=send_message)
    
    print(f"Slack notification sent. Response: {response.status_code}")

    # Exit with non-zero code if there are failures (to trigger workflow failure)
    if data['fail_count'] > 0:
        sys.exit(1)


if __name__ == '__main__':
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    
    if not webhook_url:
        print("ERROR: SLACK_WEBHOOK_URL environment variable is required")
        sys.exit(1)
        
    send_alert(webhook_url)