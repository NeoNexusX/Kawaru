import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from colorama import init, Fore, Back, Style

init(autoreset=True)  # 自动重置颜色

# 日志级别映射
LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL
}

# 创建日志管理器单例实例
LOGGER_MANAGER = None

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
        levelname_fixed = f"{record.levelname + ':':<10.10}"
        # get color
        color = self.LEVEL_COLORS.get(record.levelno, Fore.RESET)
        # replace [LEVEL] with color
        colored_level = f"{color}{levelname_fixed}{Fore.RESET}"
        formatted_record = original_message.replace(f"{record.levelname}", colored_level)

        # multilines helper
        if '\n' in record.message:
            # calculate prefix length
            first_line = formatted_record.split('\n')[0]
            # (len(colored_level)-len(levelname_fixed)) = 10
            prefix_length = len(first_line) - len(record.message.split('\n')[0])-10
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


class FileFormatter(logging.Formatter):
    """文件日志格式化器，保持原始记录名"""
    def format(self, record):
        original_message = super().format(record)
        levelname_fixed = f"{record.levelname + ':':<10.10}"
        formatted_record = original_message.replace(f"{record.levelname}", levelname_fixed)

        # multilines helper
        if '\n' in record.message:
            # calculate prefix length
            first_line = formatted_record.split('\n')[0]
            prefix_length = len(first_line) - len(record.message.split('\n')[0])
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


class LoggerManager:
    """logger manager to control the logger"""

    def __init__(self):
        self.loggers = {}
        self.log_dir = "logs"
        self.log_level = logging.INFO
        self.max_bytes = 1 * 1024 * 1024  # 1MB
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
        self.get_logger("main").info(
            f"Logger initialized with level {log_level} and log directory '{self.log_dir}' ")
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
        console_formatter = ColorFormatter(  # console Formatter for colors
            '%(levelname)s%(asctime)s %(lineno)d %(name)s - %(message)s',
            datefmt='%y-%m-%d %H:%M'
        )
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

        # add file handler
        root_file_path = os.path.join(self.log_dir, "kawaru.log")
        file_handler = RotatingFileHandler(
            root_file_path,
            maxBytes=self.max_bytes,
            backupCount=self.backup_count
        )
        file_formatter = FileFormatter(
            '%(levelname)s%(asctime)s %(lineno)d %(name)s - %(message)s'
        )
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)


    def _configure_logger(self, logger, name):
        """Configure a named logger with console and rotating file handlers."""
        # Avoid adding duplicate handlers
        if logger.handlers:
            return logger

        # Set base logger level
        logger.setLevel(self.log_level)

        # Prevent propagation to root logger
        logger.propagate = False

        # add console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        console_formatter = ColorFormatter(
            "%(levelname)s%(asctime)s %(lineno)d %(name)s - %(message)s",
            datefmt="%y-%m-%d %H:%M",
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # add file handler
        file_path = os.path.join(self.log_dir, f"{name}.log")
        file_handler = RotatingFileHandler(
            file_path, maxBytes=self.max_bytes, backupCount=self.backup_count
        )
        file_handler.setLevel(self.log_level)
        file_formatter = FileFormatter(
            "%(levelname)s%(asctime)s %(lineno)d %(name)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        return logger


    def _logger_group(self, name: str) -> str:
        """Top-level logger group: 'bootstrap.config' -> 'bootstrap'."""
        return (name or "app").split(".", 1)[0]

    def get_logger(self, name="app") -> logging.Logger:
        """Get logger.

        Desired behavior:
        - child loggers (e.g. bootstrap.xxx) print to console and write to *parent file* (bootstrap.log)
        - file output keeps original record.name (bootstrap.xxx)
        """
        if not self.initialized:
            raise RuntimeError(
                "LoggerManager is not initialized. Call bootstrap.init_logging() (or bootstrap.init_runtime()) first."
            )

        group = self._logger_group(name)

        # 1) Ensure the parent/group logger is configured ONCE (handlers live here)
        if group not in self.loggers:
            group_logger = logging.getLogger(group)
            group_logger = self._configure_logger(group_logger, group)  # writes to logs/{group}.log
            self.loggers[group] = group_logger

        # 2) If requesting the group itself, return it
        if name == group:
            return self.loggers[group]

        # 3) Child logger: no handlers; propagate to parent/group so it uses parent's handlers
        if name not in self.loggers:
            child = logging.getLogger(name)

            # Let records bubble up to 'bootstrap' (parent), which has the file+console handlers
            child.propagate = True

            # Keep it permissive; filtering/levels are enforced by parent/group effective level
            child.setLevel(logging.NOTSET)

            self.loggers[name] = child

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

        # update root handler
        for handler in root_logger.handlers:
            handler.setLevel(level)

        # update all logger
        for logger in self.loggers.values():
            logger.setLevel(level)
            # update all handlers for this logger
            for handler in logger.handlers:
                handler.setLevel(level)

# 获取日志器的便捷函数
def get_logger(name="app"):
    """Get logger by name."""

    if LOGGER_MANAGER is None:
        raise RuntimeError("LoggerManager is not initialized")
    return LOGGER_MANAGER.get_logger(name)
