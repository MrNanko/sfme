#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2022/5/3 16:48
# @Author  : leamx
# @File    : ping
# @Software: PyCharm
import re
from datetime import datetime
from telethon import events
from telethon.tl import functions
from sfme.main import client
from sfme.utils.log import logger
from sfme.utils.execute import executer


@client.on(events.NewMessage(outgoing=True, pattern=r'^ping(\s((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3})?$'))
async def ping_handler(event):
    try:
        curr_message = event.message
        try:
            ip_address = re.search(r'((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3}', curr_message.text, re.S).group(0)
        except AttributeError:
            ip_address = None
        if ip_address:
            result = await executer(f"ping -c 1 {ip_address} | awk -F '/' " + "'END {print $5}'")
            if result:
                await curr_message.edit(f"Pong! | PING: {result} ms")
            else:
                await curr_message.edit(f"ping: {ip_address}: Name or service not known")
        else:
            start = datetime.now()
            await client(functions.PingRequest(ping_id=0))
            end = datetime.now()
            ping_duration = (end - start).microseconds / 1000
            start = datetime.now()
            await curr_message.edit("Pong!")
            end = datetime.now()
            msg_duration = (end - start).microseconds / 1000
            await curr_message.edit(f"Pong! | PING: {ping_duration} ms | MSG: {msg_duration} ms")
    except Exception as e:
        logger.error(f'{e}')


@client.on(events.NewMessage(outgoing=True, pattern='^pingdc$'))
async def pingdc_handler(event):
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
        await event.message.edit(
            f"`DC1: {data[0]}ms`\n"
            f"`DC2: {data[1]}ms`\n"
            f"`DC3: {data[2]}ms`\n"
            f"`DC4: {data[3]}ms`\n"
            f"`DC5: {data[4]}ms`"
        )
    except Exception as e:
        logger.error(f'{e}')
