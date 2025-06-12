#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time       : 2025/6/8 18:07
# @Author     : @MrNanko
# @File       : main
# @Software   : PyCharm
# @Description: Main entry script for SFME bot initialization and task scheduling.

import os
import sys
import logging
import asyncio

logger = logging.getLogger(__name__)

# Check if the script is being run as a standalone program
if __name__ == "__main__" and __package__ is None:
    # If running as a script, set up the package path
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    __package__ = "sfme"

# If running as a module
from .core.logging import setup_logging
from .core.client import init_client
from .core.scheduler import task_manager, auto_discover_and_start_tasks
from .utils.redis import init_redis_client
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


async def __load_auto_scheduled_tasks():
    """Asynchronously load scheduled tasks"""
    try:
        logger.info("Discovering and starting scheduled tasks...")

        # Wait for Telethon integration to be fully ready
        await asyncio.sleep(0.5)

        # Run directly in the current event loop, not using executor
        result = auto_discover_and_start_tasks(
            package_name="sfme",  # package name
            handlers_package="handlers",  # handlers's package name
            decorator_types=['schedule_cron', 'schedule_interval', 'schedule_delay', 'schedule_at']  # decorator types
        )

        # Show start results
        if result['total_started'] > 0:
            logger.info(f"✅ Successfully started {result['total_started']} scheduled tasks")
            for decorator_type, tasks in result['started'].items():
                if tasks:
                    for task in tasks:
                        logger.info(f"Success start {decorator_type} tasks - {task['module']}.{task['name']}")

        if result['total_failed'] > 0:
            logger.warning(f"❌ {result['total_failed']} tasks failed to start")
            for decorator_type, tasks in result['failed'].items():
                if tasks:
                    for task in tasks:
                        logger.error(
                            f"Failed start {decorator_type} tasks - {task['module']}.{task['name']} - {task['error']}")

        # Show task status
        task_count = task_manager.get_task_count()
        if task_count > 0:
            logger.info(f"📊 Currently running {task_count} tasks")
            task_info = task_manager.get_task_info()
            for task_id, info in task_info.items():
                telethon_mode = info.get('telethon_mode', False)
                mode_text = "Telethon Integrated" if telethon_mode else "Standard Mode"
                logger.info(f"  📋 {task_id}: {info['status']} ({mode_text})")

        return result['total_started'] > 0

    except Exception as e:
        logger.error(f"Error loading auto-scheduled tasks: {e}")
        return False


def __load_redis_client(redis_config):
    """Load Redis client"""
    try:
        return init_redis_client(
            host=redis_config.get("host", "localhost"),
            port=redis_config.get("port", 6379),
            db=redis_config.get("db", 0),
            password=redis_config.get("password", None)
        )
    except Exception as e:
        logger.error(f"Failed to initialize Redis client: {e}")


async def main():
    """Asynchronous main function"""
    try:
        # Get current working directory
        project_dir = os.path.abspath(os.path.dirname(__file__))

        # Setup logging
        setup_logging(log_dir=os.path.join(project_dir, "logs"), log_filename="sfme.log")

        logger.info("Starting SFME bot...")

        # Initialize Redis client
        __load_redis_client(config.get("redis", {}))

        # Initialize Telethon client
        client = init_client(config.get("api_id"), config.get("api_hash"), config.get("session_name"))

        # Start Telethon client
        await client.start()
        logger.info("✅ Telethon client started")

        # Enable Telethon TaskManager events loop
        task_manager.enable_telethon_events_loop(client)
        logger.info("🔗 Telethon TaskManager events loop enabled")

        # Asynchronously load scheduled tasks
        scheduler_success = await __load_auto_scheduled_tasks()

        if scheduler_success:
            logger.info("🚀 Scheduler started successfully")
        else:
            logger.warning("⚠️ Scheduler failed to start or no tasks found")

        # Show final state
        task_count = task_manager.get_task_count()
        logger.info(f"🎉 SFME bot started! {task_count} tasks running")

        # Confirm all tasks are running in Telethon event loop
        if hasattr(task_manager, '_telethon_mode') and task_manager._telethon_mode:
            logger.info("✅ All tasks are running in the Telethon event loop")

        # Load handlers
        __load_handlers()

        # Run client until disconnected
        logger.info("🔄 Running client until disconnected...")
        await client.run_until_disconnected()

    except KeyboardInterrupt:
        logger.info("👋 Interrupt received, shutting down...")
    except Exception as e:
        logger.error(f"❌ Main program error: {e}")
        raise
    finally:
        # Clean up all tasks
        try:
            logger.info("🛑 Cleaning up tasks...")
            task_manager.cancel_all_tasks()
            logger.info("✅ All tasks cleaned up")
        except Exception as e:
            logger.error(f"Error cleaning up tasks: {e}")


if __name__ == "__main__":
    asyncio.run(main())