#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time       : 2025/6/8 00:00
# @Author     : @MrNanko
# @File       : dme
# @Software   : PyCharm
# @Description:

import asyncio
import logging
import time
from telethon import events
from ..core.client import client

logger = logging.getLogger(__name__)

@client.on(events.NewMessage(pattern=r'^-dme(?:\s+(\d+))?$', from_users='me'))
async def delete_handler(event):
    """ Handle the -dme command to delete messages in the current chat."""
    try:
        start_time = time.time()
        # Get the count of messages to delete from the command argument
        count = int(event.pattern_match.group(1)) if event.pattern_match.group(1) else 1
        count = min(count, 100)  # Limit to a maximum of 100 messages

        # Get the last `count + 1` messages in the chat (including the command message itself)
        messages = await client.get_messages(event.chat_id, limit=count + 1, from_user='me')

        # Delete the messages, excluding the command message itself
        await client.delete_messages(event.chat_id, messages)

        duration = time.time() - start_time
        # Send a confirmation message
        result = await event.respond(f'Successfully deleted {len(messages)} messages, cost {duration:.2f}s')
        await asyncio.sleep(3)
        await result.delete()

        logger.info(f"Deleted {len(messages)} messages in chat {event.chat_id}")

    except Exception as e:
        logger.error(f"Error in delete_message_handler: {e}")
