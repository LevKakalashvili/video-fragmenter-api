import asyncio
import shutil
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path

from loguru import logger

from src.core.settings import settings
from src.domain.errors import ChunkExecutionError
from src.services.chunking.resolver import ChunkingStrategyResolver


@dataclass(slots=True, frozen=True)
class ChunkExecutionRequest:
    """Контракт на выполнение одного subprocess для генерации чанка."""

    chunk_index: int
    input_path: Path
    output_path: Path
    start_seconds: int | float
    duration_seconds: int | float
    chunk_mode: str = field(default_factory=lambda: settings.app.chunk_mode)
    ffmpeg_command: str | None = None
    extra_args: tuple[str, ...] = ()


@dataclass(slots=True, frozen=True)
class ChunkExecutionResult:
    """Нормализованный результат выполнения одного чанка."""

    chunk_index: int
    output_path: Path
    file_size_bytes: int
    elapsed_ms: int


class ChunkExecutor(ABC):
    """Интерфейс сервиса выполнения одного чанка."""

    @abstractmethod
    async def execute(self, request: ChunkExecutionRequest) -> ChunkExecutionResult:
        """Выполняет один чанк и возвращает нормализованный результат."""


class FfmpegChunkExecutionService(ChunkExecutor):
    """Сервис подготовки и выполнения одного ffmpeg subprocess для чанка."""

    def __init__(self, chunking_strategy_resolver: ChunkingStrategyResolver):
        self.chunking_strategy_resolver = chunking_strategy_resolver

    async def execute(self, request: ChunkExecutionRequest) -> ChunkExecutionResult:
        # Сначала централизованно строим команду под выбранный chunking mode,
        # затем валидируем входной файл и доступность бинарника.
        command = self._build_command(request=request)
        normalized_command = self._normalize_command(
            input_path=request.input_path, command=command, request=request
        )

        logger.info(
            f"Старт выполнения чанка: chunk_index={request.chunk_index}, "
            f"mode={request.chunk_mode}, output_path={request.output_path}"
        )
        started_at = time.perf_counter()
        # Один chunk = один subprocess. stdout нам не нужен, stderr сохраняем для диагностики.
        process = await asyncio.create_subprocess_exec(
            *normalized_command,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await process.communicate()
        stderr_text = stderr.decode("utf-8", errors="replace").strip()

        if process.returncode != 0:
            logger.error(
                f"Ошибка выполнения чанка: chunk_index={request.chunk_index}, "
                f"code={process.returncode}, stderr={stderr_text}"
            )
            raise ChunkExecutionError(
                chunk_index=request.chunk_index,
                output_path=str(request.output_path),
                message="ffmpeg завершился с ошибкой",
                ffmpeg_stderr=stderr_text,
                return_code=process.returncode,
            )

        if not request.output_path.exists():
            raise ChunkExecutionError(
                chunk_index=request.chunk_index,
                output_path=str(request.output_path),
                message="ffmpeg завершился без ошибки, но файл чанка не был создан",
                ffmpeg_stderr=stderr_text,
                return_code=process.returncode,
            )

        # Размер файла и elapsed time возвращаем наружу как нормализованный результат,
        # чтобы orchestration-слой не работал с Path напрямую и не измерял время сам.
        file_size_bytes = request.output_path.stat().st_size
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        logger.info(
            f"Чанк выполнен: chunk_index={request.chunk_index}, "
            f"elapsed_ms={elapsed_ms}, file_size_bytes={file_size_bytes}"
        )
        return ChunkExecutionResult(
            chunk_index=request.chunk_index,
            output_path=request.output_path,
            file_size_bytes=file_size_bytes,
            elapsed_ms=elapsed_ms,
        )

    def _build_command(self, request: ChunkExecutionRequest) -> list[str]:
        # Команда строится через chunking strategy, чтобы execution-сервис не знал
        # деталей конкретного режима нарезки.
        strategy = self.chunking_strategy_resolver.resolve(mode=request.chunk_mode)
        command = strategy.build_ffmpeg_chunk_command(
            input_path=request.input_path,
            output_path=request.output_path,
            chunk_seconds=request.duration_seconds,
            chunk_start_seconds=request.start_seconds,
        )
        executable = request.ffmpeg_command or str(command[0])
        command = [executable, *[str(arg) for arg in command[1:]]]
        if request.extra_args:
            # Дополнительные аргументы вставляем сразу после бинарника,
            # чтобы можно было централизованно добавлять флаги запуска.
            return [executable, *request.extra_args, *command[1:]]
        return command

    def _normalize_command(
        self,
        input_path: Path,
        command: list[str],
        request: ChunkExecutionRequest,
    ) -> list[str]:
        if not input_path.exists():
            raise ChunkExecutionError(
                chunk_index=request.chunk_index,
                output_path=str(request.output_path),
                message=f"Локальный входной файл не найден: {input_path}",
            )

        executable = str(command[0])
        if shutil.which(executable) is None:
            raise ChunkExecutionError(
                chunk_index=request.chunk_index,
                output_path=str(request.output_path),
                message=f"Не найден исполняемый файл: {executable}",
            )

        # Нормализуем все аргументы в str, чтобы subprocess получал предсказуемый список.
        return [executable, *[str(arg) for arg in command[1:]]]
