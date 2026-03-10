import json
from datetime import UTC, datetime
from uuid import UUID

from redis.asyncio import Redis

from src.core.settings import settings
from src.infra.broker.schemas import KafkaVideoJobInSchema
from src.services.job_state_handlers.base import JobStateHandler


class RedisJobStateHandler(JobStateHandler):
    """Хранит текущее состояние job и чанков в Redis с TTL."""

    def __init__(self, redis: Redis):
        self.redis = redis
        self.ttl_seconds = settings.redis.state_ttl

    def _active_job_key(self, file_id: UUID) -> str:
        return f"video-job:active:{file_id}"

    def _job_document_key(self, job_id: UUID | str) -> str:
        return str(job_id)

    def _now_iso(self) -> str:
        return datetime.now(UTC).isoformat()

    async def _refresh_ttl(self, job_id: UUID, file_id: UUID) -> None:
        # Продлеваем TTL на active-key и на основном JSON-документе job.
        await self.redis.expire(self._active_job_key(file_id), self.ttl_seconds)
        await self.redis.expire(self._job_document_key(job_id), self.ttl_seconds)

    async def _load_job_document(self, job_id: UUID) -> dict:
        payload = await self.redis.get(self._job_document_key(job_id))
        if not payload:
            return {}
        return json.loads(payload)

    async def _save_job_document(self, job_id: UUID, file_id: UUID, document: dict) -> None:
        await self.redis.set(
            self._job_document_key(job_id),
            json.dumps(document, ensure_ascii=False),
            ex=self.ttl_seconds,
        )
        await self.redis.expire(self._active_job_key(file_id), self.ttl_seconds)

    def _ensure_chunk_slot(self, document: dict, chunk_index: int) -> dict:
        chunks = document.setdefault("chanks", [])
        while len(chunks) <= chunk_index:
            chunks.append({})
        if not isinstance(chunks[chunk_index], dict):
            chunks[chunk_index] = {}
        return chunks[chunk_index]

    async def is_current_job(self, file_id: UUID, job_id: UUID) -> bool:
        # Active-key защищает Redis от устаревших апдейтов старой job после retry/requeue.
        current_job_id = await self.redis.get(self._active_job_key(file_id))
        return current_job_id == str(job_id)

    async def clear_active_job(self, file_id: UUID, job_id: UUID) -> None:
        # Удаляем active-ссылку только если она все еще указывает на текущую job.
        if not await self.is_current_job(file_id=file_id, job_id=job_id):
            return
        await self.redis.delete(self._active_job_key(file_id))

    async def reset_job_state(self, job_id: UUID, payload: KafkaVideoJobInSchema) -> None:
        # При повторном запуске той же file_id удаляем старый JSON-документ
        # и назначаем новую active job.
        file_id = payload.file_id
        active_job_key = self._active_job_key(file_id)
        old_job_id = await self.redis.get(active_job_key)
        if old_job_id and old_job_id != str(job_id):
            await self.redis.delete(self._job_document_key(old_job_id))

        now_iso = self._now_iso()
        await self.redis.set(active_job_key, str(job_id), ex=self.ttl_seconds)
        # Основной JSON-документ хранит параметры job и список состояний чанков.
        document = {
            "job": {
                "job_id": str(job_id),
                "file_id": str(file_id),
                "status": "queued",
                "chunks_done": 0,
                "chunks_total_estimate": 0,
                "manifest_key": "",
                "input_bucket": payload.input.bucket,
                "input_key": payload.input.key,
                "output_bucket": payload.output.bucket,
                "output_prefix": payload.output.prefix,
                "chunk_seconds": payload.chunking.seconds,
                "chunk_mode": payload.chunking.mode,
                "created_at": now_iso,
                "updated_at": now_iso,
                "error": "",
            },
            "chanks": [],
        }
        await self._save_job_document(job_id=job_id, file_id=file_id, document=document)

    async def mark_job_started(
        self, job_id: UUID, file_id: UUID, chunks_total_estimate: int
    ) -> None:
        if not await self.is_current_job(file_id=file_id, job_id=job_id):
            return

        now_iso = self._now_iso()
        # Started фиксирует успешное прохождение pre-check этапов.
        document = await self._load_job_document(job_id=job_id)
        job = document.setdefault("job", {})
        job.update(
            {
                "status": "started",
                "chunks_done": 0,
                "chunks_total_estimate": chunks_total_estimate,
                "updated_at": now_iso,
                "error": "",
            }
        )
        await self._save_job_document(job_id=job_id, file_id=file_id, document=document)

    async def mark_chunk_processing(
        self,
        job_id: UUID,
        file_id: UUID,
        chunk_index: int,
        chunks_total_estimate: int,
    ) -> None:
        if not await self.is_current_job(file_id=file_id, job_id=job_id):
            return

        now_iso = self._now_iso()
        # Для каждого chunk index обновляем словарь в списке `chanks`.
        document = await self._load_job_document(job_id=job_id)
        chunk = self._ensure_chunk_slot(document=document, chunk_index=chunk_index)
        chunk.update(
            {
                "chunk_index": chunk_index,
                "status": "processing",
                "chunk_key": "",
                "error": "",
                "updated_at": now_iso,
            }
        )
        job = document.setdefault("job", {})
        job.update(
            {
                "status": "processing",
                "chunks_total_estimate": chunks_total_estimate,
                "updated_at": now_iso,
            }
        )
        await self._save_job_document(job_id=job_id, file_id=file_id, document=document)

    async def mark_chunk_uploaded(
        self,
        job_id: UUID,
        file_id: UUID,
        chunk_index: int,
        chunk_key: str,
        chunks_done: int,
        chunks_total_estimate: int,
    ) -> None:
        if not await self.is_current_job(file_id=file_id, job_id=job_id):
            return

        now_iso = self._now_iso()
        # После upload обновляем и запись чанка, и агрегированный счетчик chunks_done.
        document = await self._load_job_document(job_id=job_id)
        chunk = self._ensure_chunk_slot(document=document, chunk_index=chunk_index)
        chunk.update(
            {
                "chunk_index": chunk_index,
                "status": "uploaded",
                "chunk_key": chunk_key,
                "error": "",
                "updated_at": now_iso,
            }
        )
        job = document.setdefault("job", {})
        job.update(
            {
                "status": "processing",
                "chunks_done": chunks_done,
                "chunks_total_estimate": chunks_total_estimate,
                "updated_at": now_iso,
            }
        )
        await self._save_job_document(job_id=job_id, file_id=file_id, document=document)

    async def mark_chunk_failed(
        self,
        job_id: UUID,
        file_id: UUID,
        chunk_index: int,
        error_text: str,
        chunks_done: int,
        chunks_total_estimate: int,
    ) -> None:
        if not await self.is_current_job(file_id=file_id, job_id=job_id):
            return

        now_iso = self._now_iso()
        # Ошибка чанка фиксируется и на уровне chunk hash, и на уровне общего job state.
        document = await self._load_job_document(job_id=job_id)
        chunk = self._ensure_chunk_slot(document=document, chunk_index=chunk_index)
        chunk.update(
            {
                "chunk_index": chunk_index,
                "status": "failed",
                "chunk_key": "",
                "error": error_text,
                "updated_at": now_iso,
            }
        )
        job = document.setdefault("job", {})
        job.update(
            {
                "status": "failed",
                "chunks_done": chunks_done,
                "chunks_total_estimate": chunks_total_estimate,
                "updated_at": now_iso,
                "error": error_text,
            }
        )
        await self._save_job_document(job_id=job_id, file_id=file_id, document=document)

    async def mark_job_completed(
        self, job_id: UUID, file_id: UUID, chunks_done: int, manifest_key: str
    ) -> None:
        if not await self.is_current_job(file_id=file_id, job_id=job_id):
            return

        # Completed завершает job и сохраняет итоговый manifest key.
        document = await self._load_job_document(job_id=job_id)
        job = document.setdefault("job", {})
        job.update(
            {
                "status": "completed",
                "chunks_done": chunks_done,
                "manifest_key": manifest_key,
                "updated_at": self._now_iso(),
                "error": "",
            }
        )
        await self._save_job_document(job_id=job_id, file_id=file_id, document=document)

    async def mark_job_failed(
        self,
        job_id: UUID,
        file_id: UUID,
        chunks_done: int,
        chunks_total_estimate: int,
        error_text: str,
    ) -> None:
        if not await self.is_current_job(file_id=file_id, job_id=job_id):
            return

        # Failed оставляет в Redis итоговую ошибку, чтобы ее могли прочитать внешние consumers.
        document = await self._load_job_document(job_id=job_id)
        job = document.setdefault("job", {})
        job.update(
            {
                "status": "failed",
                "chunks_done": chunks_done,
                "chunks_total_estimate": chunks_total_estimate,
                "updated_at": self._now_iso(),
                "error": error_text,
            }
        )
        await self._save_job_document(job_id=job_id, file_id=file_id, document=document)
