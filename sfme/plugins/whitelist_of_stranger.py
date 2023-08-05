#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2022/5/26 01:20
# @Author  : leamx
# @File    : block_stranger
# @Software: PyCharm
import traceback
from pyrogram import filters
import redis
import asyncio
from sfme.utils.log import logger
from sfme.utils.models import db, WhitelistOfStranger
from sfme.main import settings, app, redis_pool

redis_expire_time = 86400


@app.on_message(~(filters.group | filters.bot | filters.channel | filters.me))
async def whitelist_of_stranger_handler(client, message):
    try:
        redis_conn = redis.Redis(connection_pool=redis_pool)
        peer_id = message.chat.id  # 获取用户 id
        # 判断 whitelist_of_stranger 是否在 redis 中，不在则更新 redis
        if not redis_conn.exists("whitelist_of_stranger"):
            whitelist = [int(term['peer_id']) for term in
                         WhitelistOfStranger.select(WhitelistOfStranger.peer_id).dicts()]
            logger.info(f"Whitelist_of_stranger does not exist in redis, and whitelist: {whitelist}")
            pipe = redis_conn.pipeline()
            for i in whitelist:
                pipe.sadd("whitelist_of_stranger", i)
            pipe.expire("whitelist_of_stranger", redis_expire_time)
            pipe.execute()

        is_in_whitelist = redis_conn.sismember("whitelist_of_stranger", peer_id)
        if not is_in_whitelist:
            # 判断是否主动聊天过
            is_chat = await check_if_i_chatted_with(client, peer_id)
            if is_chat:
                logger.info(f"Put peer_id: {peer_id} in whitelist")
                # 将 peer_id 添加到 sqlite
                WhitelistOfStranger(peer_id=peer_id).save()
                # 删除 redis 中 whitelist_of_block_stranger
                redis_conn.delete("whitelist_of_stranger")
            else:
                # 处理该用户，默认archive （清空历史并且block/archive）
                deal = settings.get('plugins', {}).get("whitelist_of_stranger", {}).get("deal", "archive")
                logger.info(f"Deal function: {deal}")
                if deal == "archive":
                    logger.info(f"Archive user: {peer_id}")
                    await client.archive_chats(peer_id)
                elif deal == "delete":
                    logger.info(f"Delete chat history, and block user: {peer_id}")
                    async for message in client.get_chat_history(peer_id):
                        await message.delete()
                        await asyncio.sleep(1)
                    logger.info(f"Block user: {peer_id}")
                    await client.block_user(peer_id)
    except Exception as e:
        logger.error(e)


@app.on_message(filters.me & filters.regex("^.add_to_whitelist$"))
async def add_to_whitelist(client, message):
    try:
        peer_id = message.chat.id
        WhitelistOfStranger(peer_id=peer_id).save()
        redis_conn = redis.Redis(connection_pool=redis_pool)
        redis_conn.delete("whitelist_of_stranger")
        logger.info(f"Add user: {peer_id} to whitelist")
    except Exception as e:
        logger.error(f"Error line no: {e.__traceback__.tb_lineno}, info: {e.args}")


@app.on_message(filters.me & filters.regex("^.remove_from_whitelist$"))
async def remove_from_whitelist(client, message):
    try:
        peer_id = message.chat.id
        WhitelistOfStranger.get(peer_id=peer_id).delete_instance()
        redis_conn = redis.Redis(connection_pool=redis_pool)
        await redis_conn.delete("whitelist_of_stranger")
        logger.info(f"Remove user: {peer_id} from whitelist")
    except Exception as e:
        logger.error(f"Error line no: {e.__traceback__.tb_lineno}, info: {e.args}")


async def check_if_i_chatted_with(client, peer_id: int) -> bool:
    """
    检测我是否主动和此人聊天过
    :param peer_id:
    :param client:
    :return:
    """
    try:
        me = await client.get_me()
        async for message in client.get_chat_history(peer_id):
            if message.from_user.id == me.id:
                return True
        return False
    except:
        logger.error(f"{traceback.format_exc()}")
        return False
