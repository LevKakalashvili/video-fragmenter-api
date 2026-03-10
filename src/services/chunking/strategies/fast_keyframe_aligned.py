from pathlib import Path

from src.core.settings import settings
from src.services.chunking.base import ChunkingStrategy


class FastKeyframeAlignedChunkingStrategy(ChunkingStrategy):
    """Стратегия: быстрое разбиение по keyframes без перекодирования."""

    mode = "fast_keyframe_aligned"

    def build_ffmpeg_chunk_command(
        self,
        input_path: Path,
        output_path: Path,
        chunk_seconds: int,
        chunk_start_seconds: int,
    ) -> list[str]:
        """Строит ffmpeg-команду для одного чанка без перекодирования."""
        return [
            settings.app.ffmpeg_command,
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            str(chunk_start_seconds),
            "-i",
            str(input_path),
            "-map",
            "0",
            "-c",
            "copy",
            "-t",
            str(chunk_seconds),
            "-reset_timestamps",
            "1",
            "-y",
            str(output_path),
        ]

    def build_ffmpeg_chunk_command(
        self,
        input_path: Path,
        output_path: Path,
        chunk_seconds: int,
        chunk_start_seconds: int,
    ) -> list[str]:
        """Строит ffmpeg-команду для одного чанка без перекодирования."""
        return [
            settings.app.ffmpeg_command,
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            str(chunk_start_seconds),
            "-i",
            str(input_path),
            "-map",
            "0",
            "-c",
            "copy",
            "-t",
            str(chunk_seconds),
            "-reset_timestamps",
            "1",
            "-y",
            str(output_path),
        ]
