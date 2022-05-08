#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2022/5/3 09:26
# @Author  : leamx
# @File    : forward_handler
# @Software: PyCharm
from pyrogram import filters
from sfme.utils.log import logger
from sfme.main import settings, app


@app.on_message(filters.chat([int(i) for i in settings.get('plugins').get('unconditional_forwarding').get('rules').keys()]))
async def unconditional_forwarding_handler(client, message):
    try:
        peer_id = message.chat.id
        message_id = message.id
        chat_ids = settings.get('plugins').get('unconditional_forwarding').get('rules').get(f'{peer_id}').strip().split('&')
        for chat_id in [int(i) for i in chat_ids]:
            await client.forward_messages(chat_id=chat_id, from_chat_id=peer_id, message_ids=message_id)
    except Exception as e:
        logger.error(f'{e}')
