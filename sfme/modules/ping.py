#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2022/5/3 16:48
# @Author  : leamx
# @File    : ping
# @Software: PyCharm
import re
import traceback
from datetime import datetime
from pyrogram import filters
from sfme.main import app
from sfme.utils.log import logger
from sfme.utils.execute import executer
from pyrogram.raw import functions


@app.on_message(filters.me & filters.regex(
    "^-ping(\s((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3})?$"))
async def ping_handler(client, message):
    try:
        curr_message = str(message.text)
        try:
            ip_address = re.search(r'((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3}',
                                   curr_message, re.S).group(0)
        except AttributeError:
            ip_address = None
        if ip_address:
            result = await executer(f"ping -c 1 {ip_address} | awk -F '/' " + "'END {print $5}'")
            if result:
                await client.edit_message_text(chat_id=message.chat.id, message_id=message.id,
                                               text=f"Pong! | PING: {result} ms")
            else:
                await client.edit_message_text(chat_id=message.chat.id, message_id=message.id,
                                               text=f"ping: {ip_address}: Name or service not known")
        else:
            start = datetime.now()
            functions.Ping(ping_id=0)
            end = datetime.now()
            ping_duration = (end - start).microseconds / 1000
            start = datetime.now()
            await client.edit_message_text(chat_id=message.chat.id, message_id=message.id,
                                           text="Pong!")
            end = datetime.now()
            msg_duration = (end - start).microseconds / 1000
            await client.edit_message_text(chat_id=message.chat.id, message_id=message.id,
                                           text=f"Pong! | PING: {ping_duration} ms | MSG: {msg_duration} ms")
    except Exception as e:
        logger.error(f'{e}')


@app.on_message(filters.me & filters.regex("^-pingdc$"))
async def pingdc_handler(client, message):
    try:
        data_centers = {
            1: "149.154.175.50",
            2: "149.154.167.51",
            3: "149.154.175.100",
            4: "149.154.167.91",
            5: "91.108.56.130"
        }
        data = []
        for dc in range(1, 6):
            result = await executer(f"ping -c 1 {data_centers[dc]} | awk -F '/' " + "'END {print $5}'")
            data.append(result)
        await client.edit_message_text(chat_id=message.chat.id, message_id=message.id,
                                       text=f"`DC1: {data[0]}ms`\n"
                                            f"`DC2: {data[1]}ms`\n"
                                            f"`DC3: {data[2]}ms`\n"
                                            f"`DC4: {data[3]}ms`\n"
                                            f"`DC5: {data[4]}ms`")
    except Exception as e:
        logger.error(f'{e}')
