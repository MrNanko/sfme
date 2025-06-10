#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time       : 2025/6/8 18:07
# @Author     : @MrNanko
# @File       : main
# @Software   : PyCharm
# @Description:

import os
import sys
import logging

logger = logging.getLogger(__name__)

# Check if the script is being run as a standalone program
if __name__ == "__main__" and __package__ is None:
    # If running as a script, set up the package path
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    __package__ = "sfme"

# If running as a module
from .core.logging import setup_logging
from .core.client import init_client, start_and_run
from .handlers import __all__ as handlers
from .config import config

def __load_handlers():
    """Import all handlers in the handlers directory"""
    for handler in handlers:
        try:
            __import__(f'sfme.handlers.{handler}', fromlist=[handler])
            logger.info(f"Module {handler} loaded successfully")
        except ImportError as e:
            logger.error(f"Failed to import module {handler}: {e}")
            raise ImportError(f"Could not import module {handler}") from e

def main():

    # Get current working directory
    project_dir = os.path.abspath(os.path.dirname(__file__))

    # Setup logging - ensure logs are stored in a directory named "logs" within the project directory
    setup_logging(log_dir=os.path.join(project_dir, "logs"), log_filename="sfme.log")

    logger.info("Starting SFME bot...")

    __load_handlers()

    client = init_client(config.get("api_id"), config.get("api_hash"), "sfme")

    client.start()
    client.run_until_disconnected()


if __name__ == "__main__":
    main()