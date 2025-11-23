#负责从 config.yaml 文件读取配置，并使用 Pydantic 模型进行数据验证
import yaml
from logger import get_logger
from pydantic import ValidationError
from models.config_models import Config  #在models.config_models中导入了config函数，是基于Pydantic库的数据验证正确

logger = get_logger("main.settings")


class ConfigCreate:

    def __init__(self):
        super().__init__()
        self.config = None
        self.read()

    def read(self):
        try:
            #TODO:没有检查目标的config是否存在，需要读取完成之后检查一下。
            with open("config.yaml", "r") as file:
                self.config = Config(**yaml.safe_load(file))
                logger.info(f"config data has read in \n" +
                            '\n'.join(f"{k}: {v}" for k, v in self.config))

        except ValidationError as e:
            logger.error(e)
        except Exception as e:
            logger.error(e)

    def get_config(self):
        if self.config is None:
            logger.error("config file not found")
        return self.config

CONFIG = ConfigCreate()


def get_config():
    return CONFIG.get_config()