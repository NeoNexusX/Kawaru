import os
from dotenv import load_dotenv
from tools.logger import get_logger

stander_envs=[
    'SMTP_PASSWORD',
    'DATABASE_PASSWORD',
    'LOG_DIR',
    'LOG_LEVEL',
    'CONFIG_PATH',
    'REGION_PATH'
]
class EnvSet:
    """
    这个类负责从 .env 文件加载环境变量之后在代码中通过 os.getenv(ENV_VAL_NAME) 即可加载环境变量
    """
    def __init__(self):
        self.env = load_dotenv(".env")

    def summary(self):
        """输出环境变量的摘要信息，帮助调试和验证环境变量是否正确加载"""
        logger = get_logger("bootstrap.envset")

        logger.info("dotenv file loaded: %s", self.env)
        for env_name in stander_envs:
            env_temp = os.getenv(env_name)
            if env_temp is not None:
                logger.info(f"{env_name}: {env_temp}")
            else:
                logger.error(f"{env_name} is not set")
                raise ValueError(f"{env_name} environment variable is required")
