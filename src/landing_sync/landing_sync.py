from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import structlog

from .config import Settings
from .snowflake_client import SnowflakeClient

logger = structlog.get_logger()


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    warehouse: str = "COMPUTE_WH"
    password: Optional[str] = None
    private_key: Optional[str] = None
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[str] = None
    role: Optional[str] = None


@dataclass
class LandingSyncOptions:
    """LandingSync 処理オプション

    Attributes:
        source_type: ソース DB の種類（"oracle" / "sqlserver" / "mysql"）
        dry_run: True の場合、SQL を生成するが実行しない
    """

    source_type: str = "sqlserver"
    dry_run: bool = False


class LandingSync:
    def __init__(self, options: LandingSyncOptions, snowflake_config: SnowflakeConfig) -> None:
        self.options = options

        settings_kwargs: dict[str, Any] = {
            "SNOWFLAKE_ACCOUNT": snowflake_config.account,
            "SNOWFLAKE_USER": snowflake_config.user,
            "SNOWFLAKE_WAREHOUSE": snowflake_config.warehouse,
        }

        if snowflake_config.password:
            settings_kwargs["SNOWFLAKE_PASSWORD"] = snowflake_config.password
        if snowflake_config.private_key_path:
            settings_kwargs["SNOWFLAKE_PRIVATE_KEY_PATH"] = snowflake_config.private_key_path
        elif snowflake_config.private_key:
            settings_kwargs["SNOWFLAKE_PRIVATE_KEY"] = snowflake_config.private_key
        if snowflake_config.private_key_passphrase:
            settings_kwargs["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"] = (
                snowflake_config.private_key_passphrase
            )
        if snowflake_config.role:
            settings_kwargs["SNOWFLAKE_ROLE"] = snowflake_config.role

        self.client = SnowflakeClient(Settings(**settings_kwargs))

    def connect(self) -> None:
        """Snowflake に接続する"""
        self.client.connect()

    def close(self) -> None:
        """Snowflake 接続を閉じる"""
        self.client.close()

    def execute_sql(self, sql: str) -> list[dict[str, Any]]:
        """任意の SQL を実行する"""
        self.connect()
        try:
            return self.client.execute_query(sql)
        finally:
            self.close()

    def ping(self) -> dict[str, Any]:
        """Snowflake への接続確認として SELECT 1 を実行する"""
        result = self.execute_sql("SELECT 1 AS alive")
        logger.info("ping 成功", result=result)
        return {"ok": True, "result": result}
