from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from src.core.settings import settings


class KafkaJobInputLocationSchema(BaseModel):
    """Источник входного файла в S3/MinIO."""

    bucket: str
    key: str


class KafkaJobOutputLocationSchema(BaseModel):
    """Место сохранения выходных чанков в S3/MinIO."""

    bucket: str
    prefix: str


class KafkaJobChunkingSchema(BaseModel):
    """Параметры фрагментации видео."""

    type: Literal["time"] = Field(default_factory=lambda: settings.app.chunk_type)
    seconds: int = Field(default_factory=lambda: settings.app.chunk_seconds, ge=1)
    mode: str = Field(default_factory=lambda: settings.app.chunk_mode, min_length=1)


class KafkaJobLimitsSchema(BaseModel):
    """Ограничения обработки входного файла."""

    max_bytes: int = Field(default=524288000, ge=1)


class KafkaVideoJobInSchema(BaseModel):
    """Схема входящего сообщения из `kafka.topic_in`."""

    model_config = ConfigDict(extra="ignore")

    file_id: UUID
    input: KafkaJobInputLocationSchema
    output: KafkaJobOutputLocationSchema
    chunking: KafkaJobChunkingSchema = Field(default_factory=KafkaJobChunkingSchema)
    limits: KafkaJobLimitsSchema = Field(default_factory=KafkaJobLimitsSchema)
    state_ttl_s: int = Field(default=86400, ge=1)


class KafkaVideoJobProgressEventSchema(BaseModel):
    """Событие прогресса обработки чанков для `kafka.topic_progress`."""

    job_id: UUID
    file_id: UUID
    status: Literal["started", "chunk_uploaded", "completed", "failed"]
    chunk_index: int = Field(ge=0)
    chunk_key: str
    chunks_done: int = Field(ge=0)
    chunks_total_estimate: int = Field(ge=0)
    error: str = ""
    ts: datetime


class KafkaVideoJobFinalEventSchema(BaseModel):
    """Финальное событие обработки для `kafka.topic_progress`."""

    job_id: UUID
    file_id: UUID
    status: Literal["completed", "failed"]
    chunks_total: int = Field(ge=0)
    manifest_key: str
    error: str = ""
    ts: datetime


class KafkaVideoJobProgressSchema(BaseModel):
    """Упрощенная совместимая схема прогресса (устаревает)."""

    template_id: UUID
    status: Literal["created"] = "created"
