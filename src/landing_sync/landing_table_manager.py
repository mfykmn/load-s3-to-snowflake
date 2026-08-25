from __future__ import annotations

import re
from typing import Any

from .snowflake_client import SnowflakeClient


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
CREATE_TABLE_COMMENT = "created by automatic table creation from LandingSync"
SCHEMA_EVOLUTION_COMMENT = "created by automatic schema evolution from LandingSync"

class LandingTableManager:
    """Landing テーブルの存在確認・作成・差分列追加を行う。"""

    def __init__(self, client: SnowflakeClient, source_type: str = "sqlserver") -> None:
        self.client = client
        self.source_type = source_type.lower()

    def sync_table_schema(
        self, landing_table: str, typed_columns: list[dict[str, Any]]
    ) -> dict[str, Any]:
        table_name = self._validate_fq_table_name(landing_table)
        desired_columns = self._build_desired_columns(typed_columns)
        executed_sql: list[str] = []

        if not self._table_exists(table_name):
            create_sql = self._build_initial_create_table_sql(table_name, self._meta_columns())
            self.client.execute_query(create_sql)
            executed_sql.append(create_sql)

        existing_columns = self._get_existing_columns(table_name)
        missing_columns = [
            col for col in desired_columns if col["name"].upper() not in existing_columns
        ]
        columns_to_drop_not_null = [
            col["name"]
            for col in desired_columns
            if col["name"].upper() in existing_columns
            and col.get("nullable", True)
            and not existing_columns[col["name"].upper()]["nullable"]
        ]

        if not missing_columns and not columns_to_drop_not_null:
            return {
                "created": bool(executed_sql),
                "added_columns": [],
                "dropped_not_null_columns": [],
                "executed_sql": executed_sql,
            }

        for column in missing_columns:
            alter_sql = self._build_add_column_sql(table_name, column)
            self.client.execute_query(alter_sql)
            executed_sql.append(alter_sql)

        for column_name in columns_to_drop_not_null:
            alter_sql = self._build_drop_not_null_sql(table_name, column_name)
            self.client.execute_query(alter_sql)
            executed_sql.append(alter_sql)

        return {
            "created": bool(executed_sql and executed_sql[0].startswith("CREATE TABLE")),
            "added_columns": [col["name"] for col in missing_columns],
            "dropped_not_null_columns": columns_to_drop_not_null,
            "executed_sql": executed_sql,
        }

    def _table_exists(self, landing_table: str) -> bool:
        database, schema, table = landing_table.split(".")
        sql = f"""
        SELECT COUNT(*) AS CNT
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_CATALOG = '{database}'
          AND TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table}'
          AND TABLE_TYPE = 'BASE TABLE'
        """
        result = self.client.execute_query(sql)
        return bool(result and result[0].get("CNT", 0) > 0)

    def _get_existing_columns(self, landing_table: str) -> dict[str, dict[str, bool]]:
        database, schema, table = landing_table.split(".")
        sql = f"""
        SELECT COLUMN_NAME, IS_NULLABLE
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = '{database}'
          AND TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table}'
        """
        result = self.client.execute_query(sql)
        columns: dict[str, dict[str, bool]] = {}
        for row in result:
            column_name = str(row.get("COLUMN_NAME", "")).upper()
            if not column_name:
                continue
            is_nullable = str(row.get("IS_NULLABLE", "Y")).upper() == "Y"
            columns[column_name] = {"nullable": is_nullable}
        return columns

    def _build_desired_columns(self, typed_columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
        columns: list[dict[str, Any]] = []

        for column in typed_columns:
            field_name = str(column.get("field") or "").strip()
            snowflake_type = str(column.get("snowflake_type") or "").strip()
            optional = bool(column.get("optional", True))

            if not field_name or not snowflake_type:
                continue

            validated_name = self._validate_column_name(field_name)
            columns.append(
                {
                    "name": validated_name,
                    "type": snowflake_type,
                    "nullable": optional,
                }
            )

        columns.extend(self._meta_columns())

        unique: dict[str, dict[str, Any]] = {}
        for col in columns:
            unique[col["name"].upper()] = col

        return list(unique.values())

    def _meta_columns(self) -> list[dict[str, Any]]:
        base = [
            {"name": "__OP", "type": "VARCHAR"},
            {"name": "__SOURCE_TS_MS", "type": "NUMBER(38,0)"},
            {"name": "__TRANSACTION_ID", "type": "VARCHAR"},
            {"name": "__TRANSACTION_TOTAL_ORDER", "type": "NUMBER(38,0)"},
            {"name": "__CDC_SYNCED", "type": "TIMESTAMP_NTZ"},
        ]

        if self.source_type == "oracle":
            base.append({"name": "__SOURCE_COMMIT_SCN", "type": "VARCHAR"})
        elif self.source_type == "sqlserver":
            base.append({"name": "__SOURCE_COMMIT_LSN", "type": "VARCHAR"})

        return base

    @staticmethod
    def _build_initial_create_table_sql(
        landing_table: str, meta_columns: list[dict[str, Any]]
    ) -> str:
        column_lines = [
            f"  RECORD_METADATA VARIANT COMMENT '{CREATE_TABLE_COMMENT}'"
        ]
        for col in meta_columns:
            column_lines.append(
                f"  {col['name']} {col['type']} NULL COMMENT '{CREATE_TABLE_COMMENT}'"
            )

        return (
            f"CREATE TABLE IF NOT EXISTS {landing_table} (\n"
            + ",\n".join(column_lines)
            + "\n)\n"
            + "ENABLE_SCHEMA_EVOLUTION = TRUE ERROR_LOGGING = TRUE"
        )

    def _build_add_column_sql(self, landing_table: str, column: dict[str, Any]) -> str:
        nullability = "NULL" if column.get("nullable", True) else "NOT NULL"
        comment_sql = f" COMMENT '{SCHEMA_EVOLUTION_COMMENT}'"
        return (
            f"ALTER TABLE {landing_table} "
            f"ADD COLUMN IF NOT EXISTS {column['name']} {column['type']} {nullability}{comment_sql}"
        )

    @staticmethod
    def _build_drop_not_null_sql(landing_table: str, column_name: str) -> str:
        return f"ALTER TABLE {landing_table} ALTER COLUMN {column_name} DROP NOT NULL"

    @staticmethod
    def _validate_fq_table_name(name: str) -> str:
        parts = name.split(".")
        if len(parts) != 3 or not all(IDENTIFIER_RE.fullmatch(p) for p in parts):
            raise ValueError(
                "landing_table must be in 'database.schema.table' format with valid identifiers"
            )
        return ".".join(parts)

    @staticmethod
    def _validate_column_name(name: str) -> str:
        if not IDENTIFIER_RE.fullmatch(name):
            raise ValueError(f"invalid column identifier: {name}")
        return name.upper()
