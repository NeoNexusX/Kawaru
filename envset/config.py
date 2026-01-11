# 负责从 config.yaml 文件读取配置，并使用 Pydantic 模型进行数据验证
from pathlib import Path
import json
import pprint
from pydantic import ValidationError, BaseModel
import yaml
from logger import get_logger
from models.config_models import Config  # 在models.config_models中导入了config函数，是基于Pydantic库的数据验证正确
logger = get_logger("main.settings")

CONFIG_PATH = Path(
    "config.yaml"
)  # 引入 CONFIG_PATH，在读取前显式检查文件是否存在，缺失时直接记录错误并阻止继续使用空配置。


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
        self.read()

    def read(self):
        """
        Read and validate configuration from config.yaml file.
        """
        try:
            if not CONFIG_PATH.exists():
                logger.error(f"config file not found: {CONFIG_PATH.resolve()}")
                self.config = None
                return

            with CONFIG_PATH.open("r", encoding="utf-8") as file:
                data = yaml.safe_load(file) or {}
                self.config = Config(**data)
                logger.info(
                    "config data has been loaded\n"
                    + "\n".join(f"{k}:\n{format_value(v)}" for k, v in self.config)
                )

        except ValidationError as e:
            logger.error(e)
            self.config = None

        except Exception as e:
            logger.error(e)
            self.config = None
            raise e

    def get_config(self):
        """ "Get the configuration object."""
        if self.config is None:
            logger.error("config file not found")
        return self.config


CONFIG = ConfigCreate()


def get_config():
    """
    全局唯一的config获取函数
    """
    return CONFIG.get_config()
