from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from landing_sync.landing_sync import LandingSync, LandingSyncOptions, SnowflakeConfig


class StubFetcher:
    def __init__(self, client: object) -> None:
        self.client = client

    def fetch_after_fields(
        self, raw_table: str, end_loaded_at: object | None = None
    ) -> list[dict[str, object]]:
        assert raw_table == "KAFKA_DB.KAFKA_SCHEMA.RAW_TABLE"
        assert end_loaded_at is not None
        return [
            {
                "field": "id",
                "type": "int32",
                "name": None,
                "optional": False,
                "position": 0,
                "source": "envelope_schema",
            },
            {
                "field": "updated_at",
                "type": "int64",
                "name": "org.apache.kafka.connect.data.Timestamp",
                "optional": True,
                "position": 1,
                "source": "envelope_schema",
            },
            {
                "field": "external_id",
                "type": "bytes",
                "name": "org.apache.kafka.connect.data.Decimal",
                "optional": True,
                "position": 2,
                "source": "envelope_schema",
            },
        ]


class StubLandingTableManager:
    def __init__(self, client: object, source_type: str = "sqlserver") -> None:
        self.client = client
        self.source_type = source_type

    def sync_table_schema(
        self, landing_table: str, typed_columns: list[dict[str, object]]
    ) -> dict[str, object]:
        assert landing_table == "KAFKA_DB.KAFKA_SCHEMA.LANDING_TABLE"
        assert any(col.get("snowflake_type") == "INT" for col in typed_columns)
        return {
            "created": False,
            "added_columns": [],
            "executed_sql": [],
        }


def test_run_returns_snowflake_typed_columns(monkeypatch) -> None:
    monkeypatch.setattr("landing_sync.landing_sync.RawSchemaFetcher", StubFetcher)
    monkeypatch.setattr("landing_sync.landing_sync.LandingTableManager", StubLandingTableManager)
    monkeypatch.setattr(LandingSync, "connect", lambda self: None)
    monkeypatch.setattr(LandingSync, "close", lambda self: None)

    sync = LandingSync(
        LandingSyncOptions(source_type="sqlserver"),
        SnowflakeConfig(account="test_account", user="test_user", password="test_password"),
    )

    result = sync.run(
        "KAFKA_DB.KAFKA_SCHEMA.RAW_TABLE",
        landing_table="KAFKA_DB.KAFKA_SCHEMA.LANDING_TABLE",
    )

    assert result == [
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
            "field": "updated_at",
            "type": "int64",
            "name": "org.apache.kafka.connect.data.Timestamp",
            "optional": True,
            "position": 1,
            "source": "envelope_schema",
            "snowflake_type": "TIMESTAMP(6)",
        },
        {
            "field": "external_id",
            "type": "bytes",
            "name": "org.apache.kafka.connect.data.Decimal",
            "optional": True,
            "position": 2,
            "source": "envelope_schema",
            "snowflake_type": "VARCHAR",
        },
    ]


def test_run_executes_phase3_when_landing_table_given(monkeypatch) -> None:
    monkeypatch.setattr("landing_sync.landing_sync.RawSchemaFetcher", StubFetcher)
    monkeypatch.setattr("landing_sync.landing_sync.LandingTableManager", StubLandingTableManager)
    monkeypatch.setattr(LandingSync, "connect", lambda self: None)
    monkeypatch.setattr(LandingSync, "close", lambda self: None)

    sync = LandingSync(
        LandingSyncOptions(source_type="sqlserver"),
        SnowflakeConfig(account="test_account", user="test_user", password="test_password"),
    )

    result = sync.run(
        "KAFKA_DB.KAFKA_SCHEMA.RAW_TABLE",
        landing_table="KAFKA_DB.KAFKA_SCHEMA.LANDING_TABLE",
    )

    assert result[0]["snowflake_type"] == "INT"
