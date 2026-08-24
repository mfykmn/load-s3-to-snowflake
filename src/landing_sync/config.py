from __future__ import annotations

from typing import Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Snowflake 接続設定。環境変数または .env ファイルから読み込む。"""

    SNOWFLAKE_ACCOUNT: str
    SNOWFLAKE_USER: str
    SNOWFLAKE_PASSWORD: Optional[str] = None
    SNOWFLAKE_PRIVATE_KEY: Optional[str] = None
    SNOWFLAKE_PRIVATE_KEY_PATH: Optional[str] = None
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE: Optional[str] = None
    SNOWFLAKE_WAREHOUSE: str = "COMPUTE_WH"
    SNOWFLAKE_ROLE: Optional[str] = None

    SNOWFLAKE_LOGIN_TIMEOUT: int = 60
    SNOWFLAKE_NETWORK_TIMEOUT: int = 300

    @field_validator("SNOWFLAKE_PRIVATE_KEY")
    @classmethod
    def format_private_key(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        if v.startswith("-----BEGIN") and v.endswith("-----"):
            return v
        return f"-----BEGIN PRIVATE KEY-----\n{v}\n-----END PRIVATE KEY-----"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        extra = "ignore"
