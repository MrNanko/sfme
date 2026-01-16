#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time       : 2025/7/9 13:42
# @Author     : @MrNanko
# @File       : bot_sender
# @Software   : PyCharm
# @Description:

import logging
import requests
from ..config import config

logger = logging.getLogger(__name__)


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


def send_media_message(chat_id, caption, file_path, media_type='photo'):
    """使用 Bot API 发送媒体消息

    Args:
        chat_id: 目标聊天 ID
        caption: 媒体说明文字
        file_path: 本地媒体文件路径
        media_type: 媒体类型 (photo, video, document, animation)
    """
    token = config.get('bot_token', None)

    if not chat_id:
        raise ValueError("Chat ID is required.")
    if not token:
        raise ValueError("Bot token is not configured.")

    api_methods = {
        'photo': 'sendPhoto',
        'video': 'sendVideo',
        'document': 'sendDocument',
        'animation': 'sendAnimation',
    }

    method = api_methods.get(media_type, 'sendDocument')
    url = f'https://api.telegram.org/bot{token}/{method}'

    file_param = 'photo' if media_type == 'photo' else media_type

    with open(file_path, 'rb') as f:
        files = {file_param: f}
        data = {
            'chat_id': chat_id,
            'caption': caption,
            'parse_mode': 'HTML',
        }
        response = requests.post(url, data=data, files=files)

    return response.json()
