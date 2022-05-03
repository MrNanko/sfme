#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2022/5/3 09:26
# @Author  : leamx
# @File    : forward_handler
# @Software: PyCharm
from telethon import events
from sfme.utils.log import logger
from sfme.main import settings, client


@client.on(events.NewMessage(incoming=True, outgoing=True))
async def forward_handler(event):
    try:
        peer_id = await client.get_peer_id(event.message.peer_id)
        if peer_id in [int(i) for i in settings.get('plugins').get('forward').get('rules').keys()]:
            chat_ids = settings.get('plugins').get('forward').get('rules').get(f'{peer_id}').strip().split('&')
            for chat_id in chat_ids:
                await client.forward_messages(entity=int(chat_id), messages=event.message)
    except Exception as e:
        logger.error(f'{e}')
