#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time       : 2025/6/7 23:38
# @Author     : @MrNanko
# @File       : __init__
# @Software   : PyCharm
# @Description:

from . import *
import os

def __list_handlers():
    """List all handlers in the handlers directory"""
    handlers = []
    for file in os.listdir(os.path.dirname(__file__)):
        if file.endswith('.py') and file != '__init__.py':
            handlers.append(file[:-3])
    return handlers

__all__ = __list_handlers()