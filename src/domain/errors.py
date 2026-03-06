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
        super().__init__(
            f"Размер объекта s3://{bucket}/{key} = {actual_size} байт превышает лимит {max_size} байт",
        )
        self.bucket = bucket
        self.key = key
        self.actual_size = actual_size
        self.max_size = max_size
