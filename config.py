#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time       : 2025/6/7 17:08
# @Author     : @MrNanko
# @File       : config
# @Software   : PyCharm
# @Description:

import yaml
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class Config:
    """Configuration management class implementing the Singleton pattern"""
    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._config is None:
            self.load_config()

    def load_config(self):
        """Load configuration from YAML file"""
        try:
            config_path = Path(__file__).parent / 'config.yml'
            with open(config_path, 'r', encoding='utf-8') as file:
                self._config = yaml.safe_load(file)
            logger.info("Configuration loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise

    @property
    def api_id(self):
        """Get Telegram API ID"""
        return self._config['api_id']

    @property
    def api_hash(self):
        """Get Telegram API Hash"""
        return self._config['api_hash']

    def get(self, key, default=None):
        """Get configuration value by key"""
        return self._config.get(key, default)

# Create global configuration instance
config = Config()
