import os

from slack_sdk import WebClient


client = WebClient(token=os.getenv('SLACK_TOKEN'))


def send_message_to_slack_channel(channel_name: str, message: str):
    # Call the conversations.list method using the WebClient
    client.chat_postMessage(
        channel="#"+channel_name,
        text=message,
        link_names=True,
    )