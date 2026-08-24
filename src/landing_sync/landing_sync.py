from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Optional

from snowflake.connector.errors import ProgrammingError

from .config import Settings
from .snowflake_client import SnowflakeClient


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


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

    def run(self, raw_table: str, limit: int = 10) -> list[dict[str, Any]]:
        """LandingSync の実行エントリポイント。

        現在は最小実装として RAW テーブルの先頭行を参照する。
        """
        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        table_name = self._validate_fq_table_name(raw_table)
        database, schema, table = table_name.split(".")
        exists_sql = f"""
        SELECT COUNT(*) AS CNT
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_CATALOG = '{database}'
          AND TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table}'
          AND TABLE_TYPE = 'BASE TABLE'
        """
        sql = (
            "SELECT PAYLOAD, SOURCE_FILE, SOURCE_ROW_NUMBER, LOADED_AT "
            f"FROM {table_name} ORDER BY LOADED_AT DESC LIMIT {limit}"
        )

        self.connect()
        try:
            try:
                exists_result = self.client.execute_query(exists_sql)
                exists = bool(exists_result and exists_result[0].get("CNT", 0) > 0)
            except ProgrammingError as e:
                context = self._get_current_context()
                role = context.get("ROLE")
                current_db = context.get("DB")
                current_schema = context.get("SC")
                raise RuntimeError(
                    "RAW テーブルの事前確認に失敗しました。"
                    f" object={table_name}, role={role}, current_db={current_db}, current_schema={current_schema}. "
                    "USAGE 権限（DATABASE/SCHEMA）と SELECT 権限を確認してください。"
                ) from e

            if not exists:
                context = self._get_current_context()
                role = context.get("ROLE")
                current_db = context.get("DB")
                current_schema = context.get("SC")
                raise RuntimeError(
                    "RAW テーブルが見つからないか、参照権限がありません。"
                    f" object={table_name}, role={role}, current_db={current_db}, current_schema={current_schema}. "
                    "テーブル存在確認と USAGE/SELECT 権限付与を確認してください。"
                )

            return self.client.execute_query(sql)
        except ProgrammingError as e:
            if getattr(e, "sqlstate", None) == "42S02":
                context = self._get_current_context()
                role = context.get("ROLE")
                current_db = context.get("DB")
                current_schema = context.get("SC")
                raise RuntimeError(
                    "RAW テーブルにアクセスできません。"
                    f" object={table_name}, role={role}, current_db={current_db}, current_schema={current_schema}. "
                    "テーブル存在確認と SELECT 権限付与を確認してください。"
                ) from e
            raise
        finally:
            self.close()

    def _get_current_context(self) -> dict[str, Any]:
        context = self.client.execute_query(
            "SELECT CURRENT_ROLE() AS ROLE, CURRENT_DATABASE() AS DB, CURRENT_SCHEMA() AS SC"
        )
        return context[0] if context else {}

    @staticmethod
    def _validate_fq_table_name(name: str) -> str:
        parts = name.split(".")
        if len(parts) != 3 or not all(IDENTIFIER_RE.fullmatch(p) for p in parts):
            raise ValueError(
                "raw_table must be in 'database.schema.table' format with valid identifiers"
            )
        return ".".join(parts)
