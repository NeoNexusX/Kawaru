import os
from dotenv import load_dotenv
from logger import get_logger

logger = get_logger("env logger")
stander_envs=[
    'MAILGUN_API_KEY',
    'SMTP_PASSWORD',
    'DATABASE_PASSWORD',
]
class EnvSet:
    """
    这个类负责从 .env 文件加载环境变量之后在代码中通过 os.getenv(ENV_VAL_NAME) 即可加载环境变量
    """
    def __init__(self):
        self.env = load_dotenv(".env")
        if self.env:
            for env_name in stander_envs:
                env_temp = os.getenv(env_name)
                if env_temp is not None:
                    logger.info(f"{env_name}: {env_temp}\r\n")
                else:
                    logger.error(f"{env_name} is not set\r\n")
                    raise ValueError(f"{env_name} environment variable is required")
        else:
            logger.error("loading environment variables failed")
