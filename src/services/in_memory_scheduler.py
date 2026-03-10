import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

from src.core.settings import settings

T = TypeVar("T")


@dataclass(slots=True)
class _QueueItem:
    # Фабрика, которая создаёт coroutine на реальную обработку job.
    # Храним именно callable, а не готовую coroutine, чтобы запускать задачу
    # строго в момент, когда worker получил слот выполнения.
    task_factory: Callable[[], Awaitable[object]]
    # Future, через которую caller `submit()` получает результат
    # или исключение из worker-потока.
    result_future: asyncio.Future[object]


class InMemoryVideoScheduler:
    """
    In-memory scheduler для уровня ограничения `K` (видео на инстанс).

    Основная идея:
    - входящие job складываются в одну очередь `self._queue`;
    - запускается фиксированное число worker-ов (`K`);
    - каждый worker берёт следующий элемент очереди и обрабатывает его;
    - результат/ошибка возвращается тому, кто вызывал `submit`.

    Таким образом сервис всегда обрабатывает не больше `K` видео одновременно,
    даже если Kafka прислала большой batch сообщений.
    """

    def __init__(self, max_videos_in_progress: int | None = None) -> None:
        # Если явный лимит не передали, используем конфиг приложения.
        self._max_videos_in_progress = max_videos_in_progress or settings.app.max_videos_in_progress
        # Очередь задач: сюда subscriber складывает video-job, отсюда worker-ы забирают.
        self._queue: asyncio.Queue[_QueueItem | None] = asyncio.Queue()
        # Список фоновых worker task-ов.
        self._workers: list[asyncio.Task[None]] = []
        # Защищает ленивый старт worker-ов от гонки при параллельных `submit`.
        self._start_lock = asyncio.Lock()
        self._started = False

    async def submit(self, task_factory: Callable[[], Awaitable[T]]) -> T:
        # Гарантируем, что worker-ы подняты до помещения задачи в очередь.
        await self._ensure_started()
        loop = asyncio.get_running_loop()
        # Персональная future на конкретную задачу.
        result_future: asyncio.Future[object] = loop.create_future()
        # Кладём в очередь пару: "что выполнить" + "куда вернуть результат".
        await self._queue.put(_QueueItem(task_factory=task_factory, result_future=result_future))
        # Для caller это синхронная модель: await до завершения обработки в worker.
        result = await result_future
        return result  # type: ignore[return-value]

    async def _ensure_started(self) -> None:
        # Быстрый путь: worker-ы уже подняты.
        if self._started:
            return
        async with self._start_lock:
            # Повторная проверка уже внутри lock (double-checked locking).
            if self._started:
                return
            # Конфиг может быть неверным (0/отрицательное), поэтому минимум 1 worker.
            worker_count = max(1, self._max_videos_in_progress)
            # Поднимаем фиксированный пул worker-ов.
            self._workers = [asyncio.create_task(self._worker_loop()) for _ in range(worker_count)]
            self._started = True

    async def _worker_loop(self) -> None:
        # Бесконечный цикл: worker обслуживает очередь, пока живо приложение.
        while True:
            item = await self._queue.get()
            try:
                # `None` зарезервирован как "poison pill" для будущего graceful shutdown.
                if item is None:
                    return
                try:
                    # Фактический запуск обработки video-job.
                    result = await item.task_factory()
                except Exception as exc:
                    # Пробрасываем ошибку обратно caller-у через его future.
                    if not item.result_future.done():
                        item.result_future.set_exception(exc)
                else:
                    # Возвращаем успешный результат (обычно None для service methods).
                    if not item.result_future.done():
                        item.result_future.set_result(result)
            finally:
                # Всегда подтверждаем, что элемент очереди обработан.
                self._queue.task_done()
