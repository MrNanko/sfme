#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2022/5/3 16:48
# @Author  : leamx
# @File    : ping
# @Software: PyCharm
from telethon import events
from sfme.main import client
from sfme.utils.log import logger
from sfme.utils.execute import executer


@client.on(events.NewMessage(outgoing=True, pattern='^ping$'))
async def ping_handler(event):
    try:
        pass
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
            f"ping dc_1: `{data[0]}ms`\n"
            f"ping dc_2: `{data[1]}ms`\n"
            f"ping dc_3: `{data[2]}ms`\n"
            f"ping dc_4: `{data[3]}ms`\n"
            f"ping dc_5: `{data[4]}ms`"
        )
    except Exception as e:
        logger.error(f'{e}')
