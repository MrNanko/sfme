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


@app.on_message(filters.me & filters.regex('^.forward(\s\d+)?$'))
async def forward_handler(client, message):
    try:
        await client.delete_messages(message.chat.id, message.id)
        if re.search('.*(\d+).*', message.text, re.S):
            times = re.search('.*(\d+).*', message.text, re.S).group(1)
            for _ in range(int(times)):
                await client.forward_messages(chat_id=message.chat.id, from_chat_id=message.chat.id,
                                              message_ids=message.reply_to_message_id)
        else:
            await client.forward_messages(chat_id=message.chat.id, from_chat_id=message.chat.id,
                                          message_ids=message.reply_to_message_id)
    except Exception as e:
        logger.error(f'{e}')
