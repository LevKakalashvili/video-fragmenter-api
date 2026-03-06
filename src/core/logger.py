import logging
import sys

from loguru import logger

__all__ = ["init_logging"]


class InterceptHandler(logging.Handler):
    def emit(self, record):
        level = record.levelname
        if level in ["WARN", "WARNING"]:
            level = "WARNING"
        try:
            level = logger.level(level).name
        except ValueError:
            level = "INFO"

        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def init_logging(is_debug: bool = False) -> None:
    log_level = logging.DEBUG if is_debug else logging.INFO

    handler = InterceptHandler()
    # logging.basicConfig(handlers=[handler], level=log_level)  # распространяет логи на все подкапотные либы

    loggers = (
        logging.getLogger(name)
        for name in logging.root.manager.loggerDict
    )
    for log in loggers:
        log.handlers = [handler]

    logger.remove()
    logger.add(
        sys.stdout,
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
               "<level>{message}</level>",
        level=log_level,
    )
