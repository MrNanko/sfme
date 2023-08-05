#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2023/8/5 18:49
# @Author  : leam
# @File    : dme
# @Software: PyCharm
import re
import time

from pyrogram import filters
from sfme.main import app
from sfme.utils.log import logger


@app.on_message(filters.me & filters.regex('^-dme(\s\d+)?$'))
async def dme_handler(client, message):
    try:
        chat_id = message.chat.id
        message_id = message.id
        if re.search('.*(\d+).*', message.text, re.S):
            num = int(re.search('.*(\d+).*', message.text, re.S).group(1))
        else:
            num = -1
        deal_num = 0
        flag = True
        start_time = time.time()
        me = await app.get_me()
        async for message in app.get_chat_history(chat_id):
            if flag or me != message.from_user:
                flag = False
                continue
            await client.delete_messages(message.chat.id, message.id)
            if num == -1 or deal_num < num:
                deal_num += 1
            else:
                break
        end_time = time.time()
        await client.edit_message_text(chat_id=chat_id, message_id=message_id,
                                       text=f"共处理 {deal_num} 条消息，耗时 {round(end_time - start_time, 2)} 秒")
        await client.delete_messages(chat_id, message_id)
    except Exception as e:
        logger.error(f'{e}')
