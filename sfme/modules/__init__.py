#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2022/5/3 07:41
# @Author  : leamx
# @File    : __init__.py
# @Software: PyCharm
from . import *
from os.path import dirname, basename, isfile
from glob import glob


def __list_modules():
    module_paths = glob(dirname(__file__) + "/*.py")
    result = [
        basename(file)[:-3]
        for file in module_paths
        if isfile(file) and file.endswith(".py") and not file.endswith("__init__.py")
    ]
    return result


module_list = sorted(__list_modules())
__all__ = list(module_list)