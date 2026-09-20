import os
import json
import logging
from httpx import AsyncClient, Client

logger = logging.getLogger()

# 获取全局配置的端口
GET_CONFIG_ENDPOINT = os.getenv("GET_CONFIG_ENDPOINT")
if not GET_CONFIG_ENDPOINT:
    logger.error("can not get config, GET_CONFIG_ENDPOINT is not set")
    raise RuntimeError("can not get config, GET_CONFIG_ENDPOINT is not set")

# FC 回调验证 Token
FC_CALLBACK_TOKEN = os.environ["CALLBACK_TOKEN"]

CONFIG = None

def get_config() -> dict:
    """获取全局配置"""
    global CONFIG
    if CONFIG is None:
        with Client(timeout=20) as client:
            res = client.get(
                GET_CONFIG_ENDPOINT,
                headers={"X-FC-Token": FC_CALLBACK_TOKEN},
            )
            res.raise_for_status()
            config = res.json()
            CONFIG = config
    return CONFIG