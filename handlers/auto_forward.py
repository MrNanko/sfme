#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time       : 2025/6/8 00:35
# @Author     : @MrNanko
# @File       : auto_forward
# @Software   : PyCharm
# @Description: Auto-forward messages between Telegram chats

import logging
from telethon import events
from ..core.client import client
from ..config import config

logger = logging.getLogger(__name__)

# Get the list of chat IDs to monitor for new messages
chat_ids = [int(rule['forward_from']) for rule in config.get('handlers').get('auto_forward').get('rules')]

# Create a dictionary to map source chat IDs to target chat IDs
from_to_dict = {}
for rule in config.get('handlers').get('auto_forward').get('rules'):
    source_id = int(rule['forward_from'])
    # Handle multiple target chat IDs (separated by &)
    target_ids = [int(target.strip()) for target in rule['forward_to'].strip().split('&')]
    from_to_dict[source_id] = target_ids

@client.on(events.NewMessage(chats=chat_ids))
async def auto_forward_handler(event):
    """Handle incoming messages and forward them to specified target chats."""
    try:
        # Get the source chat ID
        chat_id = event.chat_id

        # Get the list of target chat IDs for forwarding
        target_chat_ids = from_to_dict.get(chat_id, [])

        # Forward the message to each target chat
        for target_id in target_chat_ids:
            try:
                await client.forward_messages(target_id, event.message)
                logger.info(f'Message forwarded from {chat_id} to {target_id}')
            except Exception as e:
                logger.error(f'Error forwarding message to {target_id}: {e}')

    except Exception as e:
        logger.error(f'Error in auto_forward_handler: {e}')
