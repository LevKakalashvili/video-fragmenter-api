import asyncio
import json
import shutil
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from src.core.settings import settings
from src.domain.errors import VideoProbeError


@dataclass(slots=True, frozen=True)
class VideoProbeRequest:
    """Контракт на анализ одного локального видеофайла через ffprobe."""

    input_path: Path
    ffprobe_command: str | None = None


@dataclass(slots=True, frozen=True)
class VideoProbeResult:
    """Нормализованный результат анализа видеофайла."""

    duration_seconds: float
    format_name: str | None
    size_bytes: int | None
    streams_count: int
    video_codec: str | None
    audio_codec: str | None
    elapsed_ms: int


class VideoProber(ABC):
    """Интерфейс сервиса анализа видеофайла."""

    @abstractmethod
    async def probe(self, request: VideoProbeRequest) -> VideoProbeResult:
        """Выполняет probe локального файла и возвращает нормализованный результат."""


class FfprobeVideoProbeService(VideoProber):
    """Сервис анализа локального видеофайла через ffprobe."""

    async def probe(self, request: VideoProbeRequest) -> VideoProbeResult:
        command = self._build_command(request=request)
        normalized_command = self._normalize_command(request=request, command=command)

        logger.info(f"Старт анализа видео: input_path={request.input_path}")
        started_at = time.perf_counter()
        process = await asyncio.create_subprocess_exec(
            *normalized_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        stderr_text = stderr.decode("utf-8", errors="replace").strip()

        if process.returncode != 0:
            logger.error(
                f"Ошибка анализа видео: input_path={request.input_path}, "
                f"code={process.returncode}, stderr={stderr_text}"
            )
            raise VideoProbeError(
                input_path=str(request.input_path),
                message="ffprobe завершился с ошибкой",
                return_code=process.returncode,
                stderr=stderr_text,
            )

        parsed_result = self._parse_output(stdout=stdout, request=request)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        result = VideoProbeResult(
            duration_seconds=parsed_result.duration_seconds,
            format_name=parsed_result.format_name,
            size_bytes=parsed_result.size_bytes,
            streams_count=parsed_result.streams_count,
            video_codec=parsed_result.video_codec,
            audio_codec=parsed_result.audio_codec,
            elapsed_ms=elapsed_ms,
        )
        logger.info(
            f"Анализ видео завершен: input_path={request.input_path}, "
            f"duration_seconds={result.duration_seconds}, elapsed_ms={elapsed_ms}"
        )
        return result

    def _build_command(self, request: VideoProbeRequest) -> list[str]:
        """Централизованно строит команду ffprobe для извлечения метаданных."""
        executable = request.ffprobe_command or settings.app.ffprobe_command
        return [
            executable,
            "-v",
            "error",
            "-show_entries",
            "format=duration,format_name,size",
            "-show_entries",
            "stream=codec_type,codec_name",
            "-of",
            "json",
            str(request.input_path),
        ]

    def _normalize_command(self, request: VideoProbeRequest, command: list[str]) -> list[str]:
        """Проверяет локальный файл и доступность бинарника ffprobe."""
        if not request.input_path.exists():
            raise VideoProbeError(
                input_path=str(request.input_path),
                message="локальный входной файл не найден",
            )

        executable = str(command[0])
        if shutil.which(executable) is None:
            raise VideoProbeError(
                input_path=str(request.input_path),
                message=f"не найден исполняемый файл: {executable}",
            )

        return [executable, *[str(arg) for arg in command[1:]]]

    def _parse_output(self, stdout: bytes, request: VideoProbeRequest) -> VideoProbeResult:
        """Парсит JSON-ответ ffprobe и валидирует обязательные поля."""
        try:
            payload = json.loads(stdout.decode("utf-8", errors="replace"))
        except Exception as exc:
            raise VideoProbeError(
                input_path=str(request.input_path),
                message="не удалось распарсить JSON-ответ ffprobe",
                original_error=exc,
            ) from exc

        format_payload = payload.get("format") or {}
        duration_text = format_payload.get("duration")
        if duration_text in (None, ""):
            raise VideoProbeError(
                input_path=str(request.input_path),
                message="ffprobe не вернул duration",
            )

        try:
            duration_seconds = float(duration_text)
        except Exception as exc:
            raise VideoProbeError(
                input_path=str(request.input_path),
                message=f"не удалось привести duration к float: {duration_text}",
                original_error=exc,
            ) from exc

        if duration_seconds <= 0:
            raise VideoProbeError(
                input_path=str(request.input_path),
                message=f"duration должен быть больше нуля, получено: {duration_seconds}",
            )

        streams = payload.get("streams") or []
        video_codec = next(
            (stream.get("codec_name") for stream in streams if stream.get("codec_type") == "video"),
            None,
        )
        audio_codec = next(
            (stream.get("codec_name") for stream in streams if stream.get("codec_type") == "audio"),
            None,
        )
        size_text = format_payload.get("size")
        size_bytes = int(size_text) if size_text not in (None, "") else None

        logger.info(
            f"Извлечена длительность видео: input_path={request.input_path}, "
            f"duration_seconds={duration_seconds}"
        )
        return VideoProbeResult(
            duration_seconds=duration_seconds,
            format_name=format_payload.get("format_name"),
            size_bytes=size_bytes,
            streams_count=len(streams),
            video_codec=video_codec,
            audio_codec=audio_codec,
            elapsed_ms=0,
        )
