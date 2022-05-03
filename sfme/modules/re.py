#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2022/5/3 15:30
# @Author  : leamx
# @File    : re
# @Software: PyCharm
from telethon import events
from sfme.main import client
from sfme.utils.log import logger


@client.on(events.NewMessage(outgoing=True, pattern='^re$'))
async def re_handler(event):
    try:
        entity = await client.get_entity(event.message.peer_id)
        await client.delete_messages(entity, event.message)
        reply_message = await event.get_reply_message()
        await client.forward_messages(entity=entity, messages=reply_message)
    except Exception as e:
        logger.error(f'{e}')
