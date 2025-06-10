#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time       : 2025/6/8 22:16
# @Author     : @MrNanko
# @File       : client
# @Software   : PyCharm
# @Description: Telegram client management and event handler registration

import logging
from typing import Optional, Callable, Any, List, Tuple
from telethon import TelegramClient

logger = logging.getLogger(__name__)

# Global client instance
_client: Optional[TelegramClient] = None


# Store pending event handlers - using a class to ensure reference consistency
class HandlerRegistry:
    def __init__(self):
        self.handlers: List[Tuple[Callable, Any]] = []

    def add(self, handler_func, event_builder):
        self.handlers.append((handler_func, event_builder))
        logger.debug(f"Handler added to registry: {handler_func.__name__}, total: {len(self.handlers)}")

    def get_all(self):
        return self.handlers.copy()

    def clear(self):
        count = len(self.handlers)
        self.handlers.clear()
        return count


# Global handler registry
_handler_registry = HandlerRegistry()


class ClientProxy:
    """Client proxy class for receiving decorator calls before client initialization"""

    def on(self, event):
        """Simulate client.on decorator"""

        def decorator(func):
            logger.info(f"Registering handler decorator for: {func.__name__}")
            # If client is already initialized, register directly
            if _client is not None:
                logger.info(f"Client already exists, registering {func.__name__} immediately")
                _client.add_event_handler(func, event)
            else:
                # Otherwise store in registry
                logger.info(f"Client not ready, storing {func.__name__} in registry")
                _handler_registry.add(func, event)
            return func

        return decorator

    def add_event_handler(self, callback, event=None):
        """Simulate add_event_handler method"""
        logger.info(f"add_event_handler called for: {callback.__name__}")
        if _client is not None:
            _client.add_event_handler(callback, event)
        else:
            _handler_registry.add(callback, event)

    def __getattr__(self, name):
        """Proxy all other method calls to the real client"""
        if _client is None:
            raise RuntimeError(f"Client not initialized. Cannot call {name}(). Call init_client() first.")
        return getattr(_client, name)

    def __str__(self):
        if _client is None:
            return f"ClientProxy(not initialized, {len(_handler_registry.handlers)} pending handlers)"
        return str(_client)

    def __repr__(self):
        if _client is None:
            return f"ClientProxy(not initialized, {len(_handler_registry.handlers)} pending handlers)"
        return repr(_client)


# Create proxy object
client = ClientProxy()


def init_client(api_id: int, api_hash: str, session_name: str = 'sfme') -> TelegramClient:
    """
    Initialize the global client

    Args:
        api_id: Telegram API ID
        api_hash: Telegram API Hash
        session_name: Session file name

    Returns:
        TelegramClient: Client instance
    """
    global _client

    if _client is not None:
        logger.warning("Client already initialized")
        return _client

    try:
        logger.info(f"Initializing Telegram client...")
        logger.info(f"Pending handlers in registry: {len(_handler_registry.handlers)}")

        # Create the actual client
        _client = TelegramClient(session_name, api_id, api_hash)

        # Get all pending handlers
        pending_handlers = _handler_registry.get_all()
        logger.info(f"Found {len(pending_handlers)} pending handlers to register")

        # Register all pending event handlers
        registered_count = 0
        for handler_func, event_builder in pending_handlers:
            try:
                _client.add_event_handler(handler_func, event_builder)
                logger.info(f"✓ Registered handler: {handler_func.__name__}")
                registered_count += 1
            except Exception as e:
                logger.error(f"✗ Failed to register handler {handler_func.__name__}: {e}")

        # Clear the registry
        cleared_count = _handler_registry.clear()

        logger.info(f"Telegram client initialized with session: {session_name}")
        logger.info(f"Successfully registered {registered_count}/{cleared_count} handlers")

        return _client
    except Exception as e:
        logger.error(f"Failed to initialize client: {e}")
        raise


def init_and_run_client(api_id: int, api_hash: str, session_name: str = 'sfme'):
    """
    Initialize and start the global client

    Args:
        api_id: Telegram API ID
        api_hash: Telegram API Hash
        session_name: Session file name
    """
    global _client

    if _client is not None:
        logger.warning("Client already initialized")
        return _client

    try:
        # Initialize client
        init_client(api_id, api_hash, session_name)

        logger.info(f"Starting Telegram client...")
        # Start the client
        _client.start()
        _client.run_until_disconnected()
    except Exception as e:
        logger.error(f"Failed to initialize and run client: {e}")
        raise


def get_client() -> TelegramClient:
    """
    Get the global client instance

    Returns:
        TelegramClient: Client instance

    Raises:
        RuntimeError: If client is not initialized
    """
    if _client is None:
        raise RuntimeError("Client not initialized. Call init_client() first.")
    return _client


async def start_client(phone: str = None, password: str = None) -> bool:
    """
    Start the client

    Args:
        phone: Phone number (if login required)
        password: Two-factor authentication password (if required)

    Returns:
        bool: Whether the client started successfully
    """
    if _client is None:
        raise RuntimeError("Client not initialized. Call init_client() first.")

    try:
        await _client.start(phone=phone, password=password)
        logger.info("Client started successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to start client: {e}")
        return False


async def stop_client():
    """Stop the client"""
    global _client

    if _client is not None:
        try:
            await _client.disconnect()
            logger.info("Client disconnected")
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
        finally:
            _client = None


def is_client_ready() -> bool:
    """Check if the client is ready"""
    return _client is not None and _client.is_connected()


def start_and_run():
    """
    Start the client and block until disconnected
    """
    if _client is None:
        raise RuntimeError("Client not initialized. Call init_client() first.")
    _client.start()
    _client.run_until_disconnected()


def debug_info():
    """Debug information"""
    logger.info(f"=== Debug Info ===")
    logger.info(f"Client initialized: {_client is not None}")
    logger.info(f"Pending handlers: {len(_handler_registry.handlers)}")
    if _handler_registry.handlers:
        for i, (func, event) in enumerate(_handler_registry.handlers):
            logger.info(f"  {i + 1}. {func.__name__} -> {event}")
    logger.info(f"==================")


# Export client for easy importing
__all__ = ['client', 'init_client', 'get_client', 'start_client', 'stop_client', 'is_client_ready', 'start_and_run', 'debug_info']