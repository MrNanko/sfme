#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time    : 2022/5/3 16:56
# @Author  : leamx
# @File    : execute
# @Software: PyCharm

from asyncio import create_subprocess_shell
from asyncio.subprocess import PIPE


async def executer(command, pass_error=True):
    """ Executes command and returns output, with the option of enabling stderr. """
    executor = await create_subprocess_shell(
        command,
        stdout=PIPE,
        stderr=PIPE
    )
    try:
        stdout, stderr = await executor.communicate()
        if pass_error:
            result = str(stdout.decode().strip()) \
                     + str(stderr.decode().strip())
        else:
            result = str(stdout.decode().strip())
        return result
    except:
        return 'execute error.'
