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


class UnsupportedChunkingModeError(DomainError):
    """Неподдерживаемый режим фрагментации видео."""

    def __init__(self, mode: str, available_modes: tuple[str, ...]):
        supported = ", ".join(available_modes) if available_modes else "<empty>"
        super().__init__(f"Неподдерживаемый режим фрагментации: '{mode}'. Доступно: {supported}")
        self.mode = mode
        self.available_modes = available_modes
