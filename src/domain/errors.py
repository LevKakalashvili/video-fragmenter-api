class DomainError(Exception):
    """Базовая ошибка доменного слоя."""


class S3ObjectNotFoundError(DomainError):
    """Объект не найден в S3/MinIO."""

    def __init__(self, bucket: str, key: str):
        super().__init__(f"Объект не найден: s3://{bucket}/{key}")
        self.bucket = bucket
        self.key = key


class S3FileTooLargeError(DomainError):
    """Размер файла в S3 превышает допустимый лимит."""

    def __init__(self, bucket: str, key: str, actual_size: int, max_size: int):
        message = (
            f"Размер объекта s3://{bucket}/{key} = {actual_size} байт "
            f"превышает лимит {max_size} байт"
        )
        super().__init__(message)
        self.bucket = bucket
        self.key = key
        self.actual_size = actual_size
        self.max_size = max_size


class UnsupportedVideoFormatError(DomainError):
    """Формат входного видео не поддерживается сервисом."""

    def __init__(
        self,
        bucket: str,
        key: str,
        file_extension: str | None,
        content_type: str | None,
        supported_formats: tuple[str, ...],
    ):
        supported = ", ".join(supported_formats) if supported_formats else "<empty>"
        detected = content_type or "<missing>"
        extension = file_extension or "<missing>"
        super().__init__(
            f"Неподдерживаемый формат видео для s3://{bucket}/{key}. "
            f"Extension='{extension}', Content-Type='{detected}'. "
            f"Поддерживаемые форматы: {supported}"
        )
        self.bucket = bucket
        self.key = key
        self.file_extension = file_extension
        self.content_type = content_type
        self.supported_formats = supported_formats


class UnsupportedChunkingModeError(DomainError):
    """Неподдерживаемый режим фрагментации видео."""

    def __init__(self, mode: str, available_modes: tuple[str, ...]):
        supported = ", ".join(available_modes) if available_modes else "<empty>"
        super().__init__(f"Неподдерживаемый режим фрагментации: '{mode}'. Доступно: {supported}")
        self.mode = mode
        self.available_modes = available_modes


class ChunkExecutionError(DomainError):
    """Ошибка выполнения одного subprocess для генерации чанка через ffmpeg."""

    def __init__(
        self,
        chunk_index: int,
        output_path: str,
        message: str,
        ffmpeg_stderr: str = "",
        return_code: int | None = None,
    ):
        error_parts = [f"Ошибка выполнения chunk={chunk_index}"]
        if return_code is not None:
            error_parts.append(f"code={return_code}")
        error_parts.append(f"output={output_path}")
        error_parts.append(message)
        if ffmpeg_stderr:
            error_parts.append(f"stderr={ffmpeg_stderr}")

        super().__init__(", ".join(error_parts))
        self.chunk_index = chunk_index
        self.output_path = output_path
        self.ffmpeg_stderr = ffmpeg_stderr
        self.return_code = return_code


class ChunkUploadError(DomainError):
    """Ошибка загрузки одного готового чанка в S3/MinIO."""

    def __init__(
        self,
        chunk_index: int,
        local_path: str,
        bucket: str,
        chunk_key: str,
        message: str,
        original_error: Exception | None = None,
    ):
        error_parts = [
            f"Ошибка загрузки chunk={chunk_index}",
            f"local_path={local_path}",
            f"bucket={bucket}",
            f"chunk_key={chunk_key}",
            message,
        ]
        if original_error is not None:
            error_parts.append(f"details={original_error}")

        super().__init__(", ".join(error_parts))
        self.chunk_index = chunk_index
        self.local_path = local_path
        self.bucket = bucket
        self.chunk_key = chunk_key
        self.original_error = original_error


class ManifestBuildError(DomainError):
    """Ошибка сборки или загрузки manifest.json."""

    def __init__(
        self,
        job_id: str,
        file_id: str,
        manifest_key: str,
        message: str,
        original_error: Exception | None = None,
    ):
        error_parts = [
            f"Ошибка manifest для job_id={job_id}",
            f"file_id={file_id}",
            f"manifest_key={manifest_key}",
            message,
        ]
        if original_error is not None:
            error_parts.append(f"details={original_error}")

        super().__init__(", ".join(error_parts))
        self.job_id = job_id
        self.file_id = file_id
        self.manifest_key = manifest_key
        self.original_error = original_error


class VideoProbeError(DomainError):
    """Ошибка анализа локального видеофайла через ffprobe."""

    def __init__(
        self,
        input_path: str,
        message: str,
        return_code: int | None = None,
        stderr: str = "",
        original_error: Exception | None = None,
    ):
        error_parts = [
            f"Ошибка video probe для input_path={input_path}",
            message,
        ]
        if return_code is not None:
            error_parts.append(f"code={return_code}")
        if stderr:
            error_parts.append(f"stderr={stderr}")
        if original_error is not None:
            error_parts.append(f"details={original_error}")

        super().__init__(", ".join(error_parts))
        self.input_path = input_path
        self.return_code = return_code
        self.stderr = stderr
        self.original_error = original_error
