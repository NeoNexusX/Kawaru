import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from colorama import init, Fore, Back, Style

init(autoreset=True)  # 自动重置颜色


# 新增部分：自定义带颜色的 Formatter
class ColorFormatter(logging.Formatter):
    """带颜色的日志格式化器（仅对控制台生效）"""

    # 定义不同日志级别的颜色
    LEVEL_COLORS = {
        logging.DEBUG: Fore.CYAN,  # 青色
        logging.INFO: Fore.GREEN,  # 绿色
        logging.WARNING: Fore.YELLOW,  # 黄色
        logging.ERROR: Fore.RED,  # 红色
        logging.CRITICAL: Fore.RED + Back.WHITE + Style.BRIGHT,  # 红底白字
    }

    def format(self, record):
        original_message = super().format(record)
        # get color
        color = self.LEVEL_COLORS.get(record.levelno, Fore.RESET)
        # replace [LEVEL] with color
        colored_level = (
            f"{color}{record.levelname}{Fore.RESET}"
        )
        formatted_record = original_message.replace(f"{record.levelname}", colored_level)

        # multilines helper
        if '\n' in record.message:
            # calculate prefix length
            first_line = formatted_record.split('\n')[0]
            # len(f"{color}{Fore.RESET}") == 10
            prefix_length = len(first_line) - len(record.message.split('\n')[0]) - 10
            # split into sever lines
            lines = record.message.split('\n')
            # add first line
            formatted_lines = [first_line]

            # add prefix
            for line in lines[1:]:
                aligned_prefix = ' ' * prefix_length
                formatted_lines.append(f"{aligned_prefix}{line}")

            # rebuild lines
            formatted_record = '\n'.join(formatted_lines)

        return formatted_record


# 日志级别映射
LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL
}


class LoggerManager:
    """logger manager to control the logger"""

    def __init__(self):
        self.loggers = {}
        self.log_dir = "logs"
        self.log_level = logging.INFO
        self.max_bytes = 10 * 1024 * 1024  # 10MB
        self.backup_count = 5
        self.initialized = False

    def init_app(self, log_level="info", log_dir='logs'):
        """init logger system"""
        # set logger level
        self.log_level = LOG_LEVELS.get(log_level.lower(), logging.INFO)

        # set logger dir
        if log_dir:
            self.log_dir = log_dir

        # make sure logger dir exist
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)

        # configure root logger
        self._configure_root_logger()

        self.initialized = True
        return self

    def _configure_root_logger(self):
        """configure root logger"""
        # get root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(self.log_level)

        # clear logger handlers
        if root_logger.handlers:
            for handler in root_logger.handlers:
                root_logger.removeHandler(handler)

        # add console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        formatter = logging.Formatter(
            '%(levelname)s: %(asctime)s %(lineno)d  %(name)s - %(message)s'
        )
        console_formatter = ColorFormatter(  # console Formatter for colors
            '%(levelname)s:     %(asctime)s %(lineno)d %(name)s - %(message)s',
            datefmt='%y-%m-%d %H:%M'
        )
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

        # add file handler
        file_path = os.path.join(self.log_dir, "app.log")
        file_handler = RotatingFileHandler(
            file_path,
            maxBytes=self.max_bytes,
            backupCount=self.backup_count
        )
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

        # add error file logger
        error_file_path = os.path.join(self.log_dir, "error.log")
        error_file_handler = RotatingFileHandler(
            error_file_path,
            maxBytes=self.max_bytes,
            backupCount=self.backup_count
        )
        error_file_handler.setLevel(logging.ERROR)
        error_file_handler.setFormatter(formatter)
        root_logger.addHandler(error_file_handler)

    def get_logger(self, name) -> logging.Logger:
        """get or create specify logger"""
        if not self.initialized:
            self.init_app()

        if name not in self.loggers:
            logger = logging.getLogger(name)
            logger.setLevel(self.log_level)
            self.loggers[name] = logger

        return self.loggers[name]

    def set_level(self, level):
        """dynamic change logger level"""
        if isinstance(level, str):
            level = LOG_LEVELS.get(level.lower(), logging.INFO)

        # update logger level
        self.log_level = level

        # update root logger level
        root_logger = logging.getLogger()
        root_logger.setLevel(level)

        # update all handler
        for handler in root_logger.handlers:
            # error keep level
            if isinstance(handler, logging.FileHandler) and os.path.basename(handler.baseFilename) == "error.log":
                continue
            handler.setLevel(level)

        # update all logger
        for logger in self.loggers.values():
            logger.setLevel(level)


# 创建日志管理器单例实例
logger_manager = LoggerManager()

LOG_LEVEL = os.getenv("LOG_LEVEL", "info")

# 初始化日志管理器
logger_manager.init_app(log_level=LOG_LEVEL, log_dir=os.getenv("LOG_DIR", "logs"))


# 获取日志器的便捷函数
def get_logger(name="app"):
    return logger_manager.get_logger(name)
