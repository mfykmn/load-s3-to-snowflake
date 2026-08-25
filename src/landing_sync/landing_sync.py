from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any, Optional

from snowflake.connector.errors import ProgrammingError

from .config import Settings
from .debezium_type_converter import DebeziumTypeConverter
from .landing_table_manager import LandingTableManager
from .raw_schema_fetcher import RawSchemaFetcher
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

    def fetch_after_schema(self, raw_table: str) -> list[dict[str, Any]]:
        """RAW テーブルから Debezium の after フィールド定義を取得する。"""
        raw_table_name = self._validate_fq_table_name(raw_table)
        self.connect()
        try:
            fetcher = RawSchemaFetcher(self.client)
            return fetcher.fetch_after_fields(raw_table_name)
        finally:
            self.close()

    def run(self, raw_table: str, landing_table: str) -> list[dict[str, Any]]:
        """LandingSync の実行エントリポイント。

        現在は最小実装として RAW テーブルから after スキーマ定義を取得し、
        Snowflake 型付きの列定義へ変換する。
        landing_table のテーブル管理も実行する。
        """
        raw_table_name = self._validate_fq_table_name(raw_table)
        landing_table_name = self._validate_fq_table_name(landing_table)
        as_of = datetime.now(timezone.utc)
        self.connect()
        try:
            try:
                # Step 1: DML レコードから after スキーマ定義を取得
                # fetcher 内で RAW テーブル存在確認も実施する。
                fetcher = RawSchemaFetcher(self.client)
                after_fields = fetcher.fetch_after_fields(raw_table_name, end_loaded_at=as_of)

                # Step 2: Debezium/Kafka Connect schema を Snowflake 型へ変換
                converter = DebeziumTypeConverter()
                typed_columns = self._build_typed_columns(after_fields, converter)

                # Step 3: Landing テーブル管理
                manager = LandingTableManager(self.client, source_type=self.options.source_type)
                manager.sync_table_schema(landing_table_name, typed_columns)

                return typed_columns
            except RuntimeError as e:
                context = self._get_current_context()
                role = context.get("ROLE")
                current_db = context.get("DB")
                current_schema = context.get("SC")
                raise RuntimeError(
                    f"{e} role={role}, current_db={current_db}, current_schema={current_schema}."
                ) from e
        except ProgrammingError as e:
            if getattr(e, "sqlstate", None) == "42S02":
                context = self._get_current_context()
                role = context.get("ROLE")
                current_db = context.get("DB")
                current_schema = context.get("SC")
                raise RuntimeError(
                    "RAW テーブルにアクセスできません。"
                    f" object={raw_table_name}, role={role}, current_db={current_db}, current_schema={current_schema}. "
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
    def _build_typed_columns(
        after_fields: list[dict[str, Any]], converter: DebeziumTypeConverter
    ) -> list[dict[str, Any]]:
        converted_columns = converter.convert_fields(after_fields)
        return [
            {
                **field_def,
                "snowflake_type": converted.snowflake_type,
            }
            for field_def, converted in zip(after_fields, converted_columns)
        ]

    @staticmethod
    def _validate_fq_table_name(name: str) -> str:
        parts = name.split(".")
        if len(parts) != 3 or not all(IDENTIFIER_RE.fullmatch(p) for p in parts):
            raise ValueError(
                "raw_table must be in 'database.schema.table' format with valid identifiers"
            )
        return ".".join(parts)
