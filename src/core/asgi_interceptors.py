from loguru import logger


class ConsoleLifecycleInterceptor:
    def __init__(self) -> None:
        self._startup_completed = False

    def on_startup_complete(self) -> None:
        self._startup_completed = True
        logger.info("Приложение успешно запущено")

    async def run(self, run_app) -> None:
        try:
            await run_app()
        except Exception as exc:
            if not self._startup_completed:
                logger.error(f"Запуск провалился! Причина: {exc}")
            else:
                logger.error(f"Ошибка при выключении или во время работы! Причина: {exc}")

            logger.exception(
                f"Приложение упало с необработанным исключением в консольном lifecycle: {exc}",
            )
            raise
