import os

from slack_sdk import WebClient


client = WebClient(token=os.getenv('SLACK_TOKEN'))


def send_message_to_slack_channel(message: str, channel_name: str = None, thread_ts: str = None) -> None:
    if channel_name is None:
        channel_name = os.getenv('SLACK_CHANNEL')
    # Call the conversations.list method using the WebClient
    client.chat_postMessage(
        channel="#"+channel_name,
        text=message,
        link_names=True,
        thread_ts=thread_ts
    )
