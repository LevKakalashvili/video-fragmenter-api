from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    api: str = "/api"
    debug: bool = False
    backend_cors_origins: list[str] = Field(default_factory=list)

    # Основные конфигурационные параметры
    max_jobs: int = 1
    max_upload_concurrency: int = 4
    chunk_seconds: int = 30
    max_file_bytes: int = 524288000
    state_ttl: int = 86400

    @field_validator("backend_cors_origins", mode="before")
    @classmethod
    def assemble_cors_origins(cls, value: str | list[str]) -> list[str] | str:
        if isinstance(value, str) and not value.startswith("["):
            return [item.strip() for item in value.split(",")]
        if isinstance(value, (list, str)):
            return value
        raise ValueError(value)


class KafkaSettings(_BaseEnvSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_prefix="KAFKA_",
    )

    bootstrap_servers: str
    log4j_root_loglevel: str = "INFO"
    tools_log4j_loglevel: str = "INFO"
    topic_in: str = "video.jobs"
    topic_progress: str = "video.progress"
    group_id: str = "video-fragmenter"


class MinioSettings(_BaseEnvSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_prefix="MINIO_",
    )

    root_user: str
    root_password: str
    log_level: str = "info"
    endpoint_url: str = "http://minio:9000"
    region_name: str = "us-east-1"
    addressing_style: str = "path"
    secure: bool = False
    bucket: str


class Settings(_BaseEnvSettings):
    app: AppSettings = Field(default_factory=AppSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    minio: MinioSettings = Field(default_factory=MinioSettings)


settings = Settings()
