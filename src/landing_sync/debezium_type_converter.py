from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog


logger = structlog.get_logger()


KAFKA_CONNECT_DECIMAL_LOGICAL_NAME = "org.apache.kafka.connect.data.Decimal"
KAFKA_CONNECT_DATE_LOGICAL_NAME = "org.apache.kafka.connect.data.Date"
KAFKA_CONNECT_TIME_LOGICAL_NAME = "org.apache.kafka.connect.data.Time"
KAFKA_CONNECT_TIMESTAMP_LOGICAL_NAME = "org.apache.kafka.connect.data.Timestamp"

DEBEZIUM_DATE_LOGICAL_NAME = "io.debezium.time.Date"
DEBEZIUM_TIME_LOGICAL_NAME = "io.debezium.time.Time"
DEBEZIUM_TIMESTAMP_LOGICAL_NAME = "io.debezium.time.Timestamp"
DEBEZIUM_MICRO_TIMESTAMP_LOGICAL_NAME = "io.debezium.time.MicroTimestamp"
DEBEZIUM_ZONED_TIMESTAMP_LOGICAL_NAME = "io.debezium.time.ZonedTimestamp"


@dataclass(frozen=True)
class ConvertedColumn:
    field: str
    snowflake_type: str
    optional: bool = True
    source_type: str | None = None
    logical_name: str | None = None
    position: int | None = None


class DebeziumTypeConverter:
    """Kafka Connect 準拠の Debezium type/name から Snowflake 型への変換。

    References:
    - https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/src/main/java/com/snowflake/kafka/connector/internal/schemaevolution/SnowflakeColumnTypeMapper.java
    - https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/src/main/java/com/snowflake/kafka/connector/internal/schemaevolution/TableSchemaResolver.java#L134-L153
    - https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/src/test/java/com/snowflake/kafka/connector/internal/schemaevolution/SnowflakeColumnTypeMapperTest.java
    """

    def __init__(self) -> None:
        self.logger = logger.bind(component="DebeziumTypeConverter")

    def convert_field(self, field_def: dict[str, Any]) -> ConvertedColumn:
        field_name = str(field_def.get("field") or "")
        source_type = self._normalized_lower(field_def.get("type"))
        logical_name = field_def.get("name")
        optional = bool(field_def.get("optional", True))
        position = self._to_optional_int(field_def.get("position"))

        if not field_name:
            raise ValueError("field definition must include non-empty 'field'")
        if not source_type:
            raise ValueError(f"field definition must include 'type': field={field_name}")

        snowflake_type = self.to_snowflake_type(source_type, logical_name, field_def)
        return ConvertedColumn(
            field=field_name,
            snowflake_type=snowflake_type,
            optional=optional,
            source_type=source_type,
            logical_name=logical_name,
            position=position,
        )

    def convert_fields(self, field_defs: list[dict[str, Any]]) -> list[ConvertedColumn]:
        return [self.convert_field(field_def) for field_def in field_defs]

    def to_snowflake_type(
        self,
        source_type: str,
        logical_name: str | None = None,
        field_def: dict[str, Any] | None = None,
    ) -> str:
        normalized_type = source_type.lower()
        normalized_name = logical_name or ""

        if normalized_type == "int8":
            return "BYTEINT"
        if normalized_type == "int16":
            return "SMALLINT"
        if normalized_type == "int32":
            if normalized_name in (KAFKA_CONNECT_DATE_LOGICAL_NAME, DEBEZIUM_DATE_LOGICAL_NAME):
                return "DATE"
            if normalized_name in (KAFKA_CONNECT_TIME_LOGICAL_NAME, DEBEZIUM_TIME_LOGICAL_NAME):
                return "TIME(6)"
            return "INT"
        if normalized_type == "int64":
            if normalized_name in (
                KAFKA_CONNECT_TIMESTAMP_LOGICAL_NAME,
                DEBEZIUM_TIMESTAMP_LOGICAL_NAME,
                DEBEZIUM_MICRO_TIMESTAMP_LOGICAL_NAME,
            ):
                return "TIMESTAMP(6)"
            return "BIGINT"
        if normalized_type == "float32":
            return "FLOAT"
        if normalized_type == "float64":
            return "DOUBLE"
        if normalized_type == "boolean":
            return "BOOLEAN"
        if normalized_type == "string":
            if normalized_name == DEBEZIUM_ZONED_TIMESTAMP_LOGICAL_NAME:
                return "TIMESTAMP_TZ"
            return "VARCHAR"
        if normalized_type == "bytes":
            if normalized_name == KAFKA_CONNECT_DECIMAL_LOGICAL_NAME:
                # Kafka Connect の SnowflakeColumnTypeMapper 準拠:
                # Decimal logical type は VARCHAR にマップする。
                return "VARCHAR"
            return "BINARY"
        if normalized_type == "array":
            return "ARRAY"
        if normalized_type in {"struct", "map"}:
            return "VARIANT"

        self.logger.warning(
            "未対応の Debezium/Kafka Connect 型のため VARCHAR にフォールバックします",
            source_type=source_type,
            logical_name=logical_name,
        )
        return "VARCHAR"

    @staticmethod
    def _normalized_lower(value: Any) -> str:
        if value is None:
            return ""
        return str(value).lower()

    @staticmethod
    def _to_optional_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

