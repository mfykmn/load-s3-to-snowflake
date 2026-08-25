from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from debezium_type_converter import DebeziumTypeConverter


def test_convert_int32_date_to_date() -> None:
    converter = DebeziumTypeConverter()

    column = converter.convert_field(
        {
            "field": "created_date",
            "type": "int32",
            "name": "io.debezium.time.Date",
            "optional": False,
            "position": 0,
        }
    )

    assert column.snowflake_type == "DATE"
    assert column.optional is False


def test_convert_int32_without_logical_name_to_int() -> None:
    converter = DebeziumTypeConverter()

    column = converter.convert_field({"field": "id", "type": "int32"})

    assert column.snowflake_type == "INT"


def test_convert_int64_timestamp_to_timestamp6() -> None:
    converter = DebeziumTypeConverter()

    column = converter.convert_field(
        {
            "field": "updated_at",
            "type": "int64",
            "name": "org.apache.kafka.connect.data.Timestamp",
        }
    )

    assert column.snowflake_type == "TIMESTAMP(6)"


def test_convert_microtimestamp_to_timestamp6() -> None:
    converter = DebeziumTypeConverter()

    column = converter.convert_field(
        {
            "field": "updated_at",
            "type": "int64",
            "name": "io.debezium.time.MicroTimestamp",
        }
    )

    assert column.snowflake_type == "TIMESTAMP(6)"


def test_convert_zoned_timestamp_to_timestamp_tz() -> None:
    converter = DebeziumTypeConverter()

    column = converter.convert_field(
        {
            "field": "updated_at_tz",
            "type": "string",
            "name": "io.debezium.time.ZonedTimestamp",
        }
    )

    assert column.snowflake_type == "TIMESTAMP_TZ"


def test_convert_decimal_with_precision_and_scale_to_varchar() -> None:
    converter = DebeziumTypeConverter()

    column = converter.convert_field(
        {
            "field": "amount",
            "type": "bytes",
            "name": "org.apache.kafka.connect.data.Decimal",
            "parameters": {"scale": "2", "connect.decimal.precision": "18"},
        }
    )

    assert column.snowflake_type == "VARCHAR"


def test_convert_decimal_without_precision_falls_back_to_varchar() -> None:
    converter = DebeziumTypeConverter()

    column = converter.convert_field(
        {
            "field": "amount",
            "type": "bytes",
            "name": "org.apache.kafka.connect.data.Decimal",
            "parameters": {"scale": "2"},
        }
    )

    assert column.snowflake_type == "VARCHAR"


def test_convert_struct_to_variant() -> None:
    converter = DebeziumTypeConverter()

    column = converter.convert_field({"field": "payload", "type": "struct"})

    assert column.snowflake_type == "VARIANT"


def test_unknown_type_falls_back_to_varchar() -> None:
    converter = DebeziumTypeConverter()

    column = converter.convert_field({"field": "mystery", "type": "uuid"})

    assert column.snowflake_type == "VARCHAR"
