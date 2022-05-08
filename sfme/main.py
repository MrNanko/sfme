#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2022/5/3 14:40
# @Author  : leamx
# @File    : main
# @Software: PyCharm

from pyrogram import Client
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
global app
global redis_pool


def _load_settings():
    """load settings.yml"""
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
    """
    example:
        redis_conn = redis.Redis(connection_pool=redis_pool)  # 链接 redis
        redis_status = redis_conn.ping()
        logger.info(redis_status)
    """
    global redis_pool
    redis_pool = redis.ConnectionPool(
        host=settings.get('redis').get('host'),
        port=settings.get('redis').get('port'),
        password=settings.get('redis').get('password'),
        decode_responses=True,
        socket_keepalive=True,
        socket_keepalive_options={socket.TCP_KEEPIDLE: 60, socket.TCP_KEEPINTVL: 30, socket.TCP_KEEPCNT: 3}
    )


def _init_application():
    global app
    app_version = "sfme v0.0.2"
    device_model = "PC"
    system_version = "Linux"
    app = Client(name='sfme', api_id=settings.get('api_id'), api_hash=settings.get('api_hash'),
                 app_version=app_version,
                 device_model=device_model,
                 system_version=system_version,
                 hide_password=True)
    # if settings.get('proxy').get('socks5', None):
    #     import socks
    #     socks5_addr = settings.get('proxy').get('socks5').get('addr', '').strip()
    #     socks5_port = settings.get('proxy').get('socks5').get('port', '').strip()
    #     socks5_username = settings.get('proxy').get('socks5').get('socks5_username', '').strip()
    #     socks5_password = settings.get('proxy').get('socks5').get('socks5_password', '').strip()
    #     app = Client(name='sfme', api_id=settings.get('api_id'), api_hash=settings.get('api_hash'),
    #                  proxy=(socks.SOCKS5, socks5_addr, socks5_port, True, socks5_username, socks5_password),
    #                  app_version=app_version,
    #                  device_model=device_model,
    #                  system_version=system_version,
    #                  hide_password=True)
    # elif settings.get(name='proxy').get('http', None):
    #     http_addr = settings.get('proxy').get('http').get('addr', '').strip()
    #     http_port = settings.get('proxy').get('http').get('port', '').strip()
    #     proxies = {
    #         'http': f'http://{http_addr}:{http_port}',
    #         'https': f'https://{http_addr}:{http_port}'
    #     }
    #     app = Client(name='sfme', api_id=settings.get('api_id'), api_hash=settings.get('api_hash'),
    #                  proxy=proxies,
    #                  app_version=app_version,
    #                  device_model=device_model,
    #                  system_version=system_version,
    #                  hide_password=True)
    # elif settings.get('proxy').get('mtp', None):
    #     mtp_addr = settings.get('proxy').get('mtp').get('addr', '').strip()
    #     mtp_port = settings.get('proxy').get('mtp').get('addr', '').strip()
    #     mtp_secret = settings.get('proxy').get('mtp').get('secret', '').strip()
    #     app = Client(name='sfme', api_id=settings.get('api_id'), api_hash=settings.get('api_hash'),
    #                  proxy=(mtp_addr, int(mtp_port), mtp_secret),
    #                  app_version=app_version,
    #                  device_model=device_model,
    #                  system_version=system_version,
    #                  hide_password=True)
    # else:
    #     app = Client('sfme', api_id=settings.get('api_id'), api_hash=settings.get('api_hash'))


def _import_modules_and_plugins():
    logger.info(f'检测到模块列表 sfme.modules.{module_list}')
    for module_name in module_list:
        try:
            import_module(f'sfme.modules.{module_name}')
        except BaseException as e:
            logger.error(e)

    logger.info(f'检测到插件列表 sfme.plugins.{plugin_list}')
    for plugin_name in plugin_list:
        try:
            # Do not import if plugin the option of enable is set to false.
            if not settings.get('plugins').get(plugin_name, None):
                import_module(f'sfme.plugins.{plugin_name}')
            elif not settings.get('plugins').get(plugin_name).get('enable', False):
                logger.info(f'Skip importing plugins - {plugin_name}')
            else:
                import_module(f'sfme.plugins.{plugin_name}')
        except BaseException as e:
            logger.error(e)
            logger.error(traceback.format_exc())


def start_application():
    _init_application()
    _import_modules_and_plugins()

    try:
        app.run()
    finally:
        app.stop()
        logger.error(f'{traceback.format_exc()}')


def main():
    _load_settings()
    _init_redis_pool()
    start_application()
