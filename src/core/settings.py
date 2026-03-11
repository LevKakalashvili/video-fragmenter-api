import json
import sys
from typing import Annotated
from typing import Literal

from loguru import logger
from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from src.core.utils import _env_name_from_error_loc, _translate_error_message, _translate_error_type


class _BaseEnvSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


class AppSettings(_BaseEnvSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_prefix="APP_",
    )

    project_name: str = "video fragmenter service"
    version: str = "0.0.1"
    debug: bool = False

    # Лимит одновременно обрабатываемых видео на инстанс (K).
    max_videos_in_progress: int = 1
    # Лимит параллельных chunk subprocess для одного видео (M).
    max_chunk_processes_per_video: int = 20
    # Максимум одновременных загрузок чанков в S3/MinIO.
    max_upload_concurrency: int = 20
    # Размер чанка по умолчанию в секундах.
    chunk_seconds: int = 2 * 60
    # Тип чанкирования по умолчанию для входящих Kafka jobs без блока chunking.
    chunk_type: Literal["time"] = "time"
    # Режим чанкирования по умолчанию для входящих Kafka jobs без блока chunking.
    chunk_mode: str = "fast_keyframe_aligned"
    # Максимально допустимый размер входного файла в байтах (500 MB).
    max_file_bytes: int = 500 * 1024 * 1024
    # Поддерживаемые видеоформаты входных файлов.
    supported_video_formats: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["mp4"]
    )
    # Корень временного каталога для локальных артефактов обработки.
    tmp_dir_root: str = "/tmp"
    ffmpeg_command: str = "ffmpeg"
    ffprobe_command: str = "ffprobe"

    @field_validator("supported_video_formats", mode="before")
    @classmethod
    def _normalize_supported_video_formats(cls, value: object) -> object:
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("["):
                try:
                    decoded = json.loads(stripped)
                except json.JSONDecodeError:
                    pass
                else:
                    return decoded
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("supported_video_formats")
    @classmethod
    def _validate_supported_video_formats(cls, value: list[str]) -> list[str]:
        normalized = [item.strip().lower() for item in value if item.strip()]
        if not normalized:
            raise ValueError("Список supported_video_formats не должен быть пустым")
        return normalized


class ConsoleSettings(_BaseEnvSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_prefix="APP_CONSOLE_",
    )

    # Интервал опроса event-loop FastStream на завершение в секундах.
    sleep_time_seconds: float = 0.1
    # Таймаут TCP-проверок внешних ресурсов на старте (Kafka/Redis) в секундах.
    resource_check_timeout_seconds: float = 5.0


class KafkaSettings(_BaseEnvSettings):
    model_config = SettingsConfigDict(
        env_file_encoding="utf-8",
        extra="ignore",
        env_prefix="KAFKA_",
    )

    bootstrap_servers: str
    log4j_root_loglevel: str = "INFO"
    tools_log4j_loglevel: str = "INFO"
    topic_in: str
    topic_progress: str
    group_id: str
    # Максимум сообщений, читаемых из Kafka за один poll.
    batch_size: int = 100
    # Таймаут batch poll из Kafka в миллисекундах.
    poll_timeout_ms: int = 1000


class MinioSettings(_BaseEnvSettings):
    model_config = SettingsConfigDict(
        env_file_encoding="utf-8",
        extra="ignore",
        env_prefix="MINIO_",
    )

    root_user: str
    root_password: str
    log_level: str = "info"
    url: str
    region_name: str = "us-east-1"
    addressing_style: str = "path"
    secure: bool = False
    bucket: str
    # Таймаут установки TCP-соединения с S3/MinIO.
    connect_timeout_seconds: int = 3
    # Таймаут ожидания ответа/чтения от S3/MinIO.
    read_timeout_seconds: int = 8


class RedisSettings(_BaseEnvSettings):
    model_config = SettingsConfigDict(
        env_file_encoding="utf-8",
        extra="ignore",
        env_prefix="REDIS_",
    )

    host: str
    port: int
    db: int = 0
    password: str | None = None
    # TTL состояния job(секунды).
    state_ttl: int = 24 * 60 * 60


class Settings(_BaseEnvSettings):
    app: AppSettings = Field(default_factory=AppSettings)
    console: ConsoleSettings = Field(default_factory=ConsoleSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    minio: MinioSettings = Field(default_factory=MinioSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)


try:
    settings = Settings()  # type: ignore
except ValidationError as exc:
    for error in exc.errors():
        error_type = error.get("type", "validation_error")
        error_message = _translate_error_message(
            error.get("msg", "Некорректное значение переменной окружения")
        )
        error_loc = tuple(error.get("loc", ()))
        env_name = _env_name_from_error_loc(error_loc)
        error_type_ru = _translate_error_type(error_type)

        logger.error(
            f"{error_type_ru} поле окружения: {env_name}. "
            f"Расположение: {error_loc}. {error_message}"
        )
    sys.exit(1)
