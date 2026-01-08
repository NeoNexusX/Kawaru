import pytest, os, sys
from dotenv import load_dotenv
from envset.config import get_config
from pathlib import Path
from urllib.parse import urlparse
from logger import get_logger

# 此处同时加载项目环境变量.env和pytest环境变量.env.pytest
# pytest环境变量用于设置pytest测试相关的变量（如接口地址、测试文件地址等）
# 加载项目环境变量如有测试项涉及到相关变量，方便调用
project_env_path = Path(__file__).resolve().parent.parent / ".env" # 项目环境变量
pytest_env_path = Path(__file__).resolve().parent.parent / ".env.pytest" # pytest环境变量
load_dotenv(project_env_path)
load_dotenv(pytest_env_path)

BASE_URL = os.getenv("BASE_URL")
UPLOAD_FILE_DIR = os.getenv("UPLOAD_FILE_DIR") 
TEST_USER = os.getenv("TEST_USER")
TEST_PWD = os.getenv("TEST_PWD")

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