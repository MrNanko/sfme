#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2022/5/3 15:30
# @Author  : leamx
# @File    : re
# @Software: PyCharm
import re
from pyrogram import filters
from sfme.main import app
from sfme.utils.log import logger


@app.on_message(filters.me & filters.regex('^.re(\s\d+)?$'))
async def forward_handler(client, message):
    try:
        chat_id = message.chat.id
        reply_to_message_id = message.reply_to_message_id
        await client.delete_messages(chat_id, message.id)
        if re.search('.*(\d+).*', message.text, re.S):
            times = re.search('.*(\d+).*', message.text, re.S).group(1)
            for _ in range(int(times)):
                await client.forward_messages(chat_id=chat_id, from_chat_id=chat_id,
                                              message_ids=reply_to_message_id)
        else:
            await client.forward_messages(chat_id=chat_id, from_chat_id=chat_id,
                                          message_ids=reply_to_message_id)
    except Exception as e:
        logger.error(f'{e}')
