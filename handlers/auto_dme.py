#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time       : 2025/6/11 11:19
# @Author     : @MrNanko
# @File       : auto_dme
# @Software   : PyCharm
# @Description: Auto delete message handler with Redis

import logging
import time
import json
import asyncio
import traceback
from collections import defaultdict
from telethon import events
from ..config import config
from ..core.client import client
from ..core.scheduler import schedule_cron
from ..utils.redis import redis_ops

logger = logging.getLogger(__name__)

# Redis key patterns
MESSAGE_KEY_PATTERN = 'auto_dme:{uid}'
MESSAGE_CHAT_ID_WHITE_LIST_KEY_PATTERN = 'auto_dme:chat_id_white_list:{uid}'

@client.on(events.NewMessage(from_users='me'))
async def auto_dme_handler(event):
    """Auto delete message handler"""
    try:
        chat_id = event.chat_id
        message_id = event.message.id

        uid = config.get('user_id', 'unknown')
        key = MESSAGE_KEY_PATTERN.format(uid=uid)

        logger.info(f"Message {message_id} from chat {chat_id} added to Redis queue with key {key}")

        if not redis_ops:
            logger.error("Redis operations not initialized")
            return

        # Serialize message data to JSON
        serialized_data = json.dumps({
            'chat_id': chat_id,
            'message_id': message_id,
            'timestamp': int(event.message.date.timestamp()),
        })

        # Use asyncio to run Redis operations in a thread-safe manner
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, redis_ops.lpush, key, serialized_data)

    except Exception as e:
        logger.error(f"Error in auto_dme_handler: {e}")

@client.on(events.NewMessage(pattern=r'^-adme\s+(add|remove|list)$', from_users='me'))
async def auto_dme_manage_handler(event):
    """Auto delete message manage handler"""
    try:
        chat_id = event.chat_id

        if not redis_ops:
            logger.error("Redis operations not initialized")
            return

        uid = config.get('user_id', 'unknown')
        key = MESSAGE_CHAT_ID_WHITE_LIST_KEY_PATTERN.format(uid=uid)

        # Use asyncio to run Redis operations in a thread-safe manner
        loop = asyncio.get_running_loop()

        op = event.pattern_match.group(1) if event.pattern_match.group(1) else None
        if op == 'add':
            await loop.run_in_executor(None, redis_ops.sadd, key, chat_id)
            await client.edit_message(event.chat_id, event.message, f"Chat ID {chat_id} added to auto delete message whitelist.")
        elif op == 'remove':
            await loop.run_in_executor(None, redis_ops.srem, key, chat_id)
            await client.edit_message(event.chat_id, event.message, f"Chat ID {chat_id} removed from auto delete message whitelist.")
        elif op == 'list':
            chat_ids = await loop.run_in_executor(None, redis_ops.smembers, key)
            chat_ids = [int(cid) for cid in chat_ids]
            await client.edit_message(event.chat_id, event.message, f"Auto delete message whitelist chat IDs: {chat_ids}")
        else:
            await client.edit_message(event.chat_id, event.message, "Invalid operation. Use 'add', 'remove', or 'list'.")

        await asyncio.sleep(5)
        await event.delete()

    except Exception as e:
        logger.error(f"Error in auto_dme_handler: {e}")

@schedule_cron("0 * * * *")
async def auto_dme():
    """The scheduled task to process messages from Redis and delete them"""
    try:
        if not redis_ops:
            logger.error("Redis operations not initialized")
            return

        uid = config.get('user_id', 'unknown')
        key = MESSAGE_KEY_PATTERN.format(uid=uid)

        deleted_messages_dict = defaultdict(list)

        # Get current event loop
        loop = asyncio.get_running_loop()

        chat_ids = await loop.run_in_executor(None, redis_ops.smembers, MESSAGE_CHAT_ID_WHITE_LIST_KEY_PATTERN.format(uid=uid))
        white_list = [int(cid) for cid in chat_ids]
        logger.info(f"white_list: {white_list}")

        while True:
            try:
                serialized_message = await loop.run_in_executor(
                    None, redis_ops.rpop, key
                )
            except Exception as e:
                logger.error(f"redis rpop error: {e}")
                break

            if not serialized_message:
                break

            try:
                message_data = json.loads(serialized_message)
                chat_id = message_data['chat_id']
                message_id = message_data['message_id']
                timestamp = int(message_data['timestamp'])

                if chat_id not in white_list:
                    deleted_messages_dict[chat_id].append(message_id)

                if timestamp > (int(time.time()) - 60 * config.get('handlers', {}).get('auto_dme', {}).get('cache_time', 3600)):
                    logger.info(f"Message {message_id} in chat {chat_id} is too recent, skipping deletion")
                    break

            except json.JSONDecodeError as e:
                logger.error(f"Json decode error: {e}")
            except Exception as e:
                logger.error(f"Error processing message data: {e}")

        for chat_id, message_ids in deleted_messages_dict.items():
            logger.info(f"Deleted {message_ids} messages in chat {chat_id}")
            await client.delete_messages(chat_id, message_ids)

    except Exception as e:
        logger.error(f"Auto DME error: {e}")
        logger.error(traceback.format_exc())


