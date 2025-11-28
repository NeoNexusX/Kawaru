'''
Author: xiongqiurong 1513401658@qq.com
Date: 2025-11-25 17:21:17
LastEditors: xiongqiurong 1513401658@qq.com
LastEditTime: 2025-11-26 16:08:22
FilePath: \Kawaru\envset\config.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
#负责从 config.yaml 文件读取配置，并使用 Pydantic 模型进行数据验证   #TODO:没有检查目标的config是否存在，需要读取完成之后检查一下。已完成
from __future__ import annotations

from pathlib import Path

import yaml
from logger import get_logger
from pydantic import ValidationError
from models.config_models import Config  #在models.config_models中导入了config函数，是基于Pydantic库的数据验证正确

logger = get_logger("main.settings")
CONFIG_PATH = Path("config.yaml")#引入 CONFIG_PATH，在读取前显式检查文件是否存在，缺失时直接记录错误并阻止继续使用空配置。


class ConfigCreate:

    def __init__(self):
        super().__init__()
        self.config = None
        self.read()

    def read(self):
        try:
            if not CONFIG_PATH.exists():
                logger.error(f"config file not found: {CONFIG_PATH.resolve()}")
                self.config = None
                return

            with CONFIG_PATH.open("r", encoding="utf-8") as file:
                data = yaml.safe_load(file) or {}
                self.config = Config(**data)
                logger.info(
                    "config data has been loaded\n" +
                    '\n'.join(f"{k}: {v}" for k, v in self.config)
                )

        except ValidationError as e:
            logger.error(e)
            self.config = None
        except Exception as e:
            logger.error(e)
            self.config = None

    def get_config(self):
        if self.config is None:
            logger.error("config file not found")
        return self.config


CONFIG = ConfigCreate()


def get_config():
    return CONFIG.get_config()