import logging
import sys

from loguru import logger

__all__ = ["init_logging"]


class InterceptHandler(logging.Handler):
    UVICORN_TRANSLATIONS = {
        "Started server process [{pid}]": "Запущен процесс сервера [{pid}]",
        "Waiting for application startup.": "Ожидание запуска приложения.",
        "Application startup complete.": "Запуск приложения завершен.",
        "Shutting down": "Завершение работы сервера.",
        "Waiting for application shutdown.": "Ожидание остановки приложения.",
        "Application shutdown complete.": "Остановка приложения завершена.",
    }

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

        message = record.getMessage()
        if record.name.startswith("uvicorn"):
            if record.msg == "Started server process [%d]" and record.args:
                message = self.UVICORN_TRANSLATIONS["Started server process [{pid}]"].format(pid=record.args[0])
            else:
                message = self.UVICORN_TRANSLATIONS.get(message, message)

        logger.opt(depth=depth, exception=record.exc_info).log(level, message)


def init_logging(is_debug: bool = False) -> None:
    log_level = logging.DEBUG if is_debug else logging.INFO

    handler = InterceptHandler()
    # logging.basicConfig(handlers=[handler], level=log_level)  # распространяет логи на все подкапотные либы

    loggers = (logging.getLogger(name) for name in logging.root.manager.loggerDict)
    for log in loggers:
        log.handlers = [handler]
        log.propagate = False

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
