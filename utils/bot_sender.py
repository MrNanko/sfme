#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time       : 2025/7/9 13:42
# @Author     : @MrNanko
# @File       : bot_sender
# @Software   : PyCharm
# @Description:

import requests
from ..config import config

def send_text_message(chat_id, text, reply_to_message_id=None):

    token = config.get('bot_token', None)

    if not chat_id:
        raise ValueError("Chat ID is required. Please provide a valid chat ID.")

    if not token:
        raise ValueError("Bot token is not configured. Please set 'bot_token' in your configuration.")

    """使用 Bot API 发送消息"""
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    data = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'HTML',
        'reply_to_message_id': reply_to_message_id
    }
    response = requests.post(url, data=data)
    return response.json()
