#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2022/5/26 13:42
# @Author  : leamx
# @File    : qrcode
# @Software: PyCharm
import re
import qrcode
import traceback
from pyrogram import filters
from sfme.main import app, project_path
from sfme.utils.log import logger


@app.on_message(filters.me & filters.regex('^.genqr(\s.*)?$'))
async def generate_qrcode_handler(client, message):
    try:
        chat_id = message.chat.id
        message_text = message.text
        await client.delete_messages(chat_id, message.id)
        text = re.search(r'.genqr\s(.*)', message_text, re.S).group(1)
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_H,
            box_size=15,
            border=4
        )
        qr.clear()
        qr.add_data(text)  # 传入数据
        qr.make(fit=True)
        img = qr.make_image()  # 生成二维码
        img.save(f'{project_path}/log/qrcode.jpg')
        await client.send_photo(chat_id=chat_id, photo=f'{project_path}/log/qrcode.jpg')
    except Exception:
        logger.error(traceback.format_exc())


@app.on_message(filters.me & filters.regex('^.parseqr(\s.*)?$'))
async def parse_qrcode_handler(client, message):
    try:
        pass
    except Exception:
        logger.error(traceback.format_exc())
