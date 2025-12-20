import os
#这个模块负责从 .env 文件加载邮件环境变量
#之后在代码中通过 os.getenv(ENV_VAL_NAME) 即可加载环境变量
from dotenv import load_dotenv

from logger import get_logger

logger = get_logger("env logger")



class EnvSet:
    def __init__(self):
        self.env = load_dotenv(".env")
        if self.env:
            logger.info(f"loading environment variables sucessfully\n"
                        f"MAILGUN_DOMAIN :{os.getenv('MAILGUN_DOMAIN')}\n"
                        f"last_smtp_username: {os.getenv('LAST_SMTP_USERNAME')}\n"
                        f"last_smtp_passwd: {os.getenv('LAST_SMTP_PASSWORD')}\n"
                        f"last_smtp_server: {os.getenv('LAST_SMTP_SERVER')}")
        else:
            logger.error("loading environment variables failed")