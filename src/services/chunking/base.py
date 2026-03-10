from abc import ABC, abstractmethod
from pathlib import Path


class ChunkingStrategy(ABC):
    """Общий интерфейс стратегии фрагментации."""

    mode: str

    @abstractmethod
    def build_ffmpeg_command(self, input_path: Path, output_dir: Path, chunk_seconds: int) -> list[str]:
        """Собирает ffmpeg-команду для фрагментации входного файла."""

    @abstractmethod
    def build_ffmpeg_chunk_command(
        self,
        input_path: Path,
        output_path: Path,
        chunk_seconds: int,
        chunk_start_seconds: int,
    ) -> list[str]:
        """Собирает ffmpeg-команду для генерации одного чанка."""
