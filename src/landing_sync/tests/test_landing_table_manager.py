from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from landing_sync.landing_table_manager import LandingTableManager


class StubClient:
    def __init__(self, scripted_results: list[list[dict[str, object]]]) -> None:
        self.scripted_results = scripted_results
        self.queries: list[str] = []

    def execute_query(self, query: str) -> list[dict[str, object]]:
        self.queries.append(query)
        if self.scripted_results:
            return self.scripted_results.pop(0)
        return []


def _typed_columns() -> list[dict[str, object]]:
    return [
        {
            "field": "id",
            "type": "int32",
            "name": None,
            "optional": False,
            "position": 0,
            "source": "envelope_schema",
            "snowflake_type": "INT",
        },
        {
            "field": "col_decimal_10_2",
            "type": "bytes",
            "name": "org.apache.kafka.connect.data.Decimal",
            "optional": True,
            "position": 1,
            "source": "envelope_schema",
            "snowflake_type": "VARCHAR",
        },
    ]


def test_sync_table_schema_create_table_when_not_exists() -> None:
    client = StubClient(
        scripted_results=[
            [{"CNT": 0}],
            [],
            [
                {"COLUMN_NAME": "RECORD_METADATA"},
                {"COLUMN_NAME": "__OP"},
                {"COLUMN_NAME": "__SOURCE_TS_MS"},
                {"COLUMN_NAME": "__TRANSACTION_ID"},
                {"COLUMN_NAME": "__TRANSACTION_TOTAL_ORDER"},
                {"COLUMN_NAME": "__CDC_SYNCED"},
                {"COLUMN_NAME": "__SOURCE_COMMIT_LSN"},
            ],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
        ]
    )
    manager = LandingTableManager(client=client, source_type="sqlserver")

    result = manager.sync_table_schema(
        "KAFKA_DB.KAFKA_SCHEMA.LANDING_TABLE",
        _typed_columns(),
    )

    assert result["created"] is True
    assert "ID" in result["added_columns"]
    assert "COL_DECIMAL_10_2" in result["added_columns"]
    assert any(
        "CREATE TABLE IF NOT EXISTS KAFKA_DB.KAFKA_SCHEMA.LANDING_TABLE" in q
        and "RECORD_METADATA VARIANT" in q
        and "__OP VARCHAR NULL" in q
        and "__OP VARCHAR NULL COMMENT 'created by automatic table creation from LandingSync'" in q
        and "ENABLE_SCHEMA_EVOLUTION = TRUE" in q
        and "ERROR_LOGGING = TRUE" in q
        for q in client.queries
    )
    assert any(
        "ADD COLUMN IF NOT EXISTS ID INT NOT NULL COMMENT "
        "'created by automatic schema evolution from LandingSync'" in q
        for q in client.queries
    )
    assert not any("ADD COLUMN IF NOT EXISTS __OP" in q for q in client.queries)


def test_sync_table_schema_add_only_missing_columns() -> None:
    client = StubClient(
        scripted_results=[
            [{"CNT": 1}],
            [
                {"COLUMN_NAME": "ID"},
                {"COLUMN_NAME": "__OP"},
                {"COLUMN_NAME": "__SOURCE_TS_MS"},
                {"COLUMN_NAME": "__TRANSACTION_ID"},
                {"COLUMN_NAME": "__TRANSACTION_TOTAL_ORDER"},
                {"COLUMN_NAME": "__CDC_SYNCED"},
                {"COLUMN_NAME": "__SOURCE_COMMIT_LSN"},
            ],
            [],
        ]
    )
    manager = LandingTableManager(client=client, source_type="sqlserver")

    result = manager.sync_table_schema(
        "KAFKA_DB.KAFKA_SCHEMA.LANDING_TABLE",
        _typed_columns(),
    )

    assert result["created"] is False
    assert result["added_columns"] == ["COL_DECIMAL_10_2"]
    assert result["dropped_not_null_columns"] == []
    assert any(
        "ADD COLUMN IF NOT EXISTS COL_DECIMAL_10_2 VARCHAR NULL COMMENT "
        "'created by automatic schema evolution from LandingSync'" in q
        for q in client.queries
    )


def test_sync_table_schema_no_change_when_all_columns_exist() -> None:
    client = StubClient(
        scripted_results=[
            [{"CNT": 1}],
            [
                {"COLUMN_NAME": "ID"},
                {"COLUMN_NAME": "COL_DECIMAL_10_2"},
                {"COLUMN_NAME": "__OP"},
                {"COLUMN_NAME": "__SOURCE_TS_MS"},
                {"COLUMN_NAME": "__TRANSACTION_ID"},
                {"COLUMN_NAME": "__TRANSACTION_TOTAL_ORDER"},
                {"COLUMN_NAME": "__CDC_SYNCED"},
                {"COLUMN_NAME": "__SOURCE_COMMIT_LSN"},
            ],
        ]
    )
    manager = LandingTableManager(client=client, source_type="sqlserver")

    result = manager.sync_table_schema(
        "KAFKA_DB.KAFKA_SCHEMA.LANDING_TABLE",
        _typed_columns(),
    )

    assert result["created"] is False
    assert result["added_columns"] == []
    assert result["dropped_not_null_columns"] == []
    assert result["executed_sql"] == []


def test_sync_table_schema_drop_not_null_for_nullable_field() -> None:
    client = StubClient(
        scripted_results=[
            [{"CNT": 1}],
            [
                {"COLUMN_NAME": "ID", "IS_NULLABLE": "N"},
                {"COLUMN_NAME": "__OP", "IS_NULLABLE": "Y"},
                {"COLUMN_NAME": "__SOURCE_TS_MS", "IS_NULLABLE": "Y"},
                {"COLUMN_NAME": "__TRANSACTION_ID", "IS_NULLABLE": "Y"},
                {"COLUMN_NAME": "__TRANSACTION_TOTAL_ORDER", "IS_NULLABLE": "Y"},
                {"COLUMN_NAME": "__CDC_SYNCED", "IS_NULLABLE": "Y"},
                {"COLUMN_NAME": "__SOURCE_COMMIT_LSN", "IS_NULLABLE": "Y"},
            ],
            [],
        ]
    )
    manager = LandingTableManager(client=client, source_type="sqlserver")

    result = manager.sync_table_schema(
        "KAFKA_DB.KAFKA_SCHEMA.LANDING_TABLE",
        [
            {
                "field": "id",
                "type": "int32",
                "name": None,
                "optional": True,
                "position": 0,
                "source": "envelope_schema",
                "snowflake_type": "INT",
            }
        ],
    )

    assert result["created"] is False
    assert result["added_columns"] == []
    assert result["dropped_not_null_columns"] == ["ID"]
    assert any(
        "ALTER TABLE KAFKA_DB.KAFKA_SCHEMA.LANDING_TABLE ALTER COLUMN ID DROP NOT NULL" in q
        for q in client.queries
    )


def test_sync_table_schema_add_metadata_columns_with_comment() -> None:
    client = StubClient(
        scripted_results=[
            [{"CNT": 1}],
            [
                {"COLUMN_NAME": "ID", "IS_NULLABLE": "N"},
                {"COLUMN_NAME": "COL_DECIMAL_10_2", "IS_NULLABLE": "Y"},
            ],
            [],
            [],
            [],
            [],
            [],
            [],
        ]
    )
    manager = LandingTableManager(client=client, source_type="sqlserver")

    result = manager.sync_table_schema(
        "KAFKA_DB.KAFKA_SCHEMA.LANDING_TABLE",
        _typed_columns(),
    )

    assert result["created"] is False
    assert "__OP" in result["added_columns"]
    assert any(
        "ADD COLUMN IF NOT EXISTS __OP VARCHAR NULL COMMENT "
        "'created by automatic schema evolution from LandingSync'" in q
        for q in client.queries
    )
    assert not any("ADD COLUMN IF NOT EXISTS COL_DECIMAL_10_2 VARCHAR NULL" in q for q in client.queries)
