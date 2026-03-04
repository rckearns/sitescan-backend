"""Application configuration loaded from environment variables."""

from pydantic_settings import BaseSettings
from pydantic import field_validator
from functools import lru_cache


class Settings(BaseSettings):
    # App
    app_name: str = "SiteScan"
    app_env: str = "development"
    secret_key: str = "sitescan-dev-key-2026-charleston-masonry-restore"
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Auth
    access_token_expire_minutes: int = 60 * 24 * 7  # 7 days
    algorithm: str = "HS256"

    # Database
    database_url: str = "sqlite+aiosqlite:///./sitescan.db"

    @field_validator("database_url", mode="before")
    @classmethod
    def fix_db_url(cls, v: str) -> str:
        if v.startswith("postgres://"):
            return v.replace("postgres://", "postgresql+asyncpg://", 1)
        if v.startswith("postgresql://"):
            return v.replace("postgresql://", "postgresql+asyncpg://", 1)
        return v

    # SAM.gov
    sam_gov_api_key: str = ""

    # Notifications — Postmark
    postmark_api_key: str = ""
    postmark_from_email: str = "alerts@yourdomain.com"

    # Twilio (SMS)
    twilio_account_sid: str = ""
    twilio_auth_token: str = ""
    twilio_from_number: str = ""

    # Scan
    scan_cron_hours: int = 6
    charleston_arcgis_url: str = (
        "https://gis.charleston-sc.gov/arcgis2/rest/services/"
        "External/Applications/MapServer/20/query"
    )
    constructconnect_api_key: str = ""
    zenrows_api_key: str = ""

    # AI
    anthropic_api_key: str = ""

    model_config = {"env_file": None}


@lru_cache
def get_settings() -> Settings:
    return Settings()
