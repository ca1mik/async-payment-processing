from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    rabbitmq_url: SecretStr
    api_key: SecretStr = Field(min_length=16)
    webhook_allowed_hosts: str = "webhook"
    webhook_timeout: float = Field(default=5, gt=0, le=30)
    outbox_poll_interval: float = Field(default=0.5, gt=0, le=10)
    retry_base_seconds: float = Field(default=1, gt=0, le=60)

    def permits_webhook(self, host: str | None) -> bool:
        allowed = {part.strip().lower() for part in self.webhook_allowed_hosts.split(",")}
        return bool(host and host.lower() in allowed)


@lru_cache
def settings() -> Settings:
    return Settings()
