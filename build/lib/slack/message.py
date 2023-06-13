import os
from dotenv import load_dotenv

from slack_sdk import WebClient

load_dotenv()
SLACK_TOKEN = os.getenv('SLACK_TOKEN')

client = WebClient(token=SLACK_TOKEN)


def send_message_to_slack_channel(channel_name: str, message: str):
    # Call the conversations.list method using the WebClient
    client.chat_postMessage(
        channel="#"+channel_name,
        text=message,
        link_names=True,
    )