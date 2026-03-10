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
        message = f"Размер объекта s3://{bucket}/{key} = {actual_size} байт превышает лимит {max_size} байт"
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
            f"Extension='{extension}', Content-Type='{detected}'. Поддерживаемые форматы: {supported}"
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
