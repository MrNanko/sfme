#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2022/7/13 20:43
# @Author  : leamx
# @File    : models
# @Software: PyCharm
from pathlib import Path
import peewee
from peewee import Model, SqliteDatabase, AutoField, CharField, DateTimeField
import datetime
import inspect
import sys
from .log import logger

db = SqliteDatabase(Path(__file__).resolve().parent.parent.joinpath("sfme.db"))


class WhitelistOfStranger(Model):
    id = AutoField(primary_key=True)
    peer_id = CharField(null=False, unique=True, max_length=24)
    create_time = DateTimeField(default=datetime.datetime.now)

    class Meta:
        database = db
        table_name = "tb_whitelist_of_stranger"


def create_tables():
    clsmembers = inspect.getmembers(sys.modules[__name__], inspect.isclass)
    for cls in clsmembers:
        if isinstance(cls[1], peewee.ModelBase) and cls[1] != Model:
            cls[1].create_table()
            logger.info(f"create table {cls[1]}")


# if __name__ == '__main__':
#     create_tables()