import pytest, os, sys
from envset.config import get_config
from logger import get_logger

BASE_URL = "http://localhost:8000/"
UPLOAD_FILE_DIR = "/home/fxxk/Desktop/Spetrum/upload_file/" # where your files for testing upload APIs are placed

logger = get_logger("pytest")
# 基础存储配置来自 config
config = get_config()
if not config:
    logger.error("pytest configuration is not loaded")
    raise RuntimeError("Pytest configuration is not loaded")
storage_config = config.storage

BYTES_IN_GB = 1024 ** 3
BYTES_IN_MB = 1024 ** 2
ALLOWED_FILE_TYPES = set(storage_config.allowed_file_types)
CHUNK_BUFFER_SIZE = int(storage_config.chunk_buffer_size) * BYTES_IN_MB