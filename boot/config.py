# 负责从 config.yaml 文件读取配置，并使用 Pydantic 模型进行数据验证
from pathlib import Path
import json
import pprint
import os
from pydantic import ValidationError, BaseModel
import yaml
from models.config_models import Config

CONFIG: "ConfigCreate | None" = None

CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "config.yaml"))  # 从环境变量获取配置文件路径，默认为当前目录下的 config.yaml

REGION_PATH = Path( os.getenv("REGION_PATH", "external/countries-states-cities-database/json/countries.json")
)  # 引入 REGION_PATH，存放世界所有国家或地区的信息，用于校验用户地区合法性。

def format_value(v):
    """格式化输出值，支持类、字典、列表等"""
    if isinstance(v, BaseModel):
        # Pydantic 模型转字典
        return json.dumps(v.model_dump(), indent=2, ensure_ascii=False)
    elif hasattr(v, "__dict__"):
        # 普通类对象转字典
        return pprint.pformat(v.__dict__, indent=2)
    else:
        # 其他类型用 pprint
        return pprint.pformat(v, indent=2)

class ConfigCreate:
    """
    Responsible for reading and validating configuration from config.yaml file using Pydantic models
    create global unique config instance
    """

    def __init__(self):
        super().__init__()
        self.config = None

    def read(self):
        """
        Read and validate configuration from config.yaml file.
        """
        try:
            if not CONFIG_PATH.exists():
                self.config = None
                raise FileNotFoundError(f"config file not found: {CONFIG_PATH.resolve()}")

            if not REGION_PATH.exists():
                self.config = None
                raise FileNotFoundError(f"region data file not found: {REGION_PATH.resolve()}")

            with CONFIG_PATH.open("r", encoding="utf-8") as file:
                data = yaml.safe_load(file) or {}
                self.config = Config(**data)

        except ValidationError as e:
            self.config = None
            raise e

        except Exception as e:
            self.config = None
            raise e

    def summary(self):
        """Print the configuration summary."""
        #logger part
        from tools.logger import get_logger  # pylint: disable=import-outside-toplevel

        logger = get_logger("bootstrap.config")

        if self.config is None:
            logger.error("config file not found")
            raise FileNotFoundError("config file not found")

        logger.info("config data has been loaded\n"
                    + "\n".join(f"{k}:\n{format_value(v)}" for k, v in self.config))

def get_config() -> Config:
    """
    全局唯一的config获取函数
    """
    if CONFIG is None or CONFIG.config is None:
        raise FileNotFoundError("config file not found")
    else:
        return CONFIG.config
