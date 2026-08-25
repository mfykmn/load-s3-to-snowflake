from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .snowflake_client import SnowflakeClient


class RawSchemaFetcher:
    """RAW テーブル内の Debezium schema から after フィールド定義を取得する。"""

    def __init__(self, client: SnowflakeClient) -> None:
        self.client = client

    def fetch_after_fields(
        self, raw_table: str, end_loaded_at: datetime | None = None
    ) -> list[dict[str, Any]]:
        self._ensure_raw_table_exists(raw_table)

        # 呼び出し時点までに取り込まれた RAW レコードだけを参照する。
        if end_loaded_at is None:
            end_loaded_at = datetime.now(timezone.utc)

        normalized = self._fetch_from_envelope_schema(raw_table, end_loaded_at)

        if not normalized:
            raise RuntimeError(
                "RAW からフィールド定義を取得できませんでした。"
                " Debezium DML レコード（schema/payload）と削除マーカー形式を確認してください。"
            )

        return normalized

    def _ensure_raw_table_exists(self, raw_table: str) -> None:
        database, schema, table = raw_table.split(".")
        exists_sql = f"""
        SELECT COUNT(*) AS CNT
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_CATALOG = '{database}'
          AND TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table}'
          AND TABLE_TYPE = 'BASE TABLE'
        """
        exists_result = self.client.execute_query(exists_sql)
        exists = bool(exists_result and exists_result[0].get("CNT", 0) > 0)
        if not exists:
            raise RuntimeError(
                "RAW テーブルが見つからないか、参照権限がありません。"
                f" object={raw_table}. テーブル存在確認と USAGE/SELECT 権限付与を確認してください。"
            )

    def _fetch_from_envelope_schema(
        self, raw_table: str, end_loaded_at: datetime
    ) -> list[dict[str, Any]]:
        end_literal = end_loaded_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        query = f"""
        WITH latest_schema AS (
            SELECT PAYLOAD
            FROM {raw_table}
            WHERE PAYLOAD:schema IS NOT NULL
              AND PAYLOAD:payload IS NOT NULL
              AND PAYLOAD:payload:op::STRING IN ('c', 'u', 'r', 'd')
              AND LOADED_AT <= TO_TIMESTAMP_NTZ('{end_literal}')
            ORDER BY LOADED_AT DESC, SOURCE_FILE DESC, SOURCE_ROW_NUMBER DESC
            LIMIT 1
        ),
        after_node AS (
            SELECT f.value AS after_field
            FROM latest_schema,
                 LATERAL FLATTEN(input => PAYLOAD:schema:fields) f
            WHERE f.value:field::STRING = 'after'
            LIMIT 1
        )
        SELECT
            c.value:field::STRING AS FIELD,
            c.value:type::STRING AS TYPE,
            c.value:name::STRING AS LOGICAL_NAME,
            COALESCE(c.value:optional::BOOLEAN, TRUE) AS OPTIONAL,
            c.index AS POSITION,
            'envelope_schema' AS SOURCE
        FROM after_node,
             LATERAL FLATTEN(input => after_field:fields) c
        ORDER BY POSITION
        """
        rows = self.client.execute_query(query)
        return self._normalize_rows(rows)

    @staticmethod
    def _normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "field": row.get("FIELD"),
                "type": row.get("TYPE"),
                "name": row.get("LOGICAL_NAME"),
                "optional": row.get("OPTIONAL"),
                "position": row.get("POSITION"),
                "source": row.get("SOURCE"),
            }
            for row in rows
            if row.get("FIELD")
        ]
