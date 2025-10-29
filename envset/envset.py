import os

from dotenv import load_dotenv

from logger import get_logger

logger = get_logger("env logger")
load_dotenv(".env")  # laod env file


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
            logger.info("loading environment variables failed")