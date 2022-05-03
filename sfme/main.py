#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2022/5/3 14:40
# @Author  : leamx
# @File    : main
# @Software: PyCharm

from telethon import TelegramClient
import socket
import redis
import yaml
from pathlib import Path
from importlib import import_module
import traceback
from sfme.modules import module_list
from sfme.plugins import plugin_list
from sfme.utils.log import logger

global settings
global client
global redis_pool


def _load_settings():
    # settings.yml config file
    try:
        with open(file=f"{Path(__file__).resolve().parent.joinpath('settings.yml')}", mode='r',
                  encoding='utf-8') as file_obj:
            global settings
            settings = yaml.load(stream=file_obj, Loader=yaml.FullLoader)
            file_obj.close()
            if not settings:
                logger.error('settings.yml 为空，初始化失败！')
                exit(1)
    except FileNotFoundError:
        logger.error('未找到 settings.yml 初始化失败！')
        exit(1)


def _init_redis_pool():
    global redis_pool
    redis_pool = redis.ConnectionPool(
        host=settings.get('redis').get('host'),
        port=settings.get('redis').get('port'),
        password=settings.get('redis').get('password'),
        decode_responses=True,
        socket_keepalive=True,
        socket_keepalive_options={socket.TCP_KEEPIDLE: 60, socket.TCP_KEEPINTVL: 30, socket.TCP_KEEPCNT: 3}
    )


def _init_client():
    global client
    client = TelegramClient('sfme', api_id=settings.get('api_id'), api_hash=settings.get('api_hash'))
    # if settings.get("proxy").get("socks5", None):
    #     import socks
    #
    #     addr = settings.get("proxy").get("socks5").get("addr")
    #     port = settings.get("proxy").get("socks5").get("port")
    #     client = TelegramClient('sfme', api_id=settings.get('api_id'), api_hash=settings.get('api_hash'),
    #                             proxy=(socks.SOCKS5, addr, port))
    # else:
    #     client = TelegramClient('sfme', api_id=settings.get('api_id'), api_hash=settings.get('api_hash'))


def _import_modules_and_plugins():
    logger.info(f'导入模块 sfme.modules.{module_list}')
    for module_name in module_list:
        try:
            import_module(f'sfme.modules.{module_name}')
        except BaseException as e:
            logger.error(e)

    logger.info(f'导入模块 sfme.plugins.{plugin_list}')
    for plugin_name in plugin_list:
        try:
            if not settings.get('plugins').get(plugin_name).get('enable'):
                continue
            else:
                import_module(f'sfme.plugins.{plugin_name}')
        except BaseException as e:
            logger.error(e)


def start_client():
    _init_client()
    _import_modules_and_plugins()

    with client:
        try:
            client.start()
            client.run_until_disconnected()
        finally:
            client.disconnect()
            logger.error(f'{traceback.format_exc()}')


def main():
    _init_redis_pool()
    _load_settings()
    start_client()
