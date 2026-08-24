# Kafka Connect実装調査

## 目的
Snowflake Kafka Connector の実装を確認し、以下を明確化する。

- テーブル未存在時にどの CREATE TABLE が実行されるか
- そのクエリが何を元に構築されるか
- 追加カラムがどの経路で作成されるか

## 調査範囲
本資料は通常の Snowflake テーブル挙動を対象とし、Iceberg は対象外とする。

## 1. テーブル自動作成の入口
テーブル作成の分岐入口は `createTableIfNotExists`。

- 実装: [SnowflakeSinkServiceV2.java](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/src/main/java/com/snowflake/kafka/connector/internal/streaming/SnowflakeSinkServiceV2.java#L283-L357)
- 関連テスト: [SnowflakeSinkServiceV2Test.java](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/src/test/java/com/snowflake/kafka/connector/internal/streaming/SnowflakeSinkServiceV2Test.java#L563-L676)

要点:

- `tableExist(tableName)` が true の場合は既存テーブルをそのまま使用
- false の場合、設定に応じて作成系メソッドを実行
- 通常テーブル前提では `createTableWithOnlyMetadataColumn` に到達

## 2. 未存在時に実行される CREATE TABLE
実装本体:

- [StandardSnowflakeConnectionService.java](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/src/main/java/com/snowflake/kafka/connector/internal/StandardSnowflakeConnectionService.java#L71-L92)

発行されるDDL（実装文字列）:

```sql
create table if not exists identifier(?)
(record_metadata variant comment 'created by automatic table creation from Snowflake Kafka Connector High Performance')
enable_schema_evolution = true error_logging = true
```

要点:

- 初期作成時は `record_metadata` 列のみ
- payload 側の業務カラムはこの CREATE には含まれない
- `identifier(?)` によりテーブル名をバインドして実行

## 3. CREATEクエリが何を元に構築されるか
初期 CREATE の構築元は次の3点。

1. 解決済みテーブル名（topic -> table 解決結果）
2. 固定列定義（`record_metadata variant` と固定コメント）
3. Connector 側固定オプション（`enable_schema_evolution = true`, `error_logging = true`）

関連:

- コネクションサービスIF: [SnowflakeConnectionService.java](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/src/main/java/com/snowflake/kafka/connector/internal/SnowflakeConnectionService.java#L50-L85)

## 4. 追加カラムはどこで作られるか
追加カラムは初期 CREATE ではなく、schema evolution 経路で `ALTER TABLE ADD COLUMN` される。

- 実行サービス: [SnowflakeSchemaEvolutionService.java](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/src/main/java/com/snowflake/kafka/connector/internal/schemaevolution/SnowflakeSchemaEvolutionService.java#L45-L95)
- 差分マッピング: [ValidationResultMapper.java](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/src/main/java/com/snowflake/kafka/connector/internal/schemaevolution/ValidationResultMapper.java#L27-L37)
- 実際の DDL 発行: [StandardSnowflakeConnectionService.java](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/src/main/java/com/snowflake/kafka/connector/internal/StandardSnowflakeConnectionService.java#L590-L620)

要点:

- `columnsToAdd` があると `appendColumnsToTable` が実行される
- DDL は `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...`

## 5. 追加カラムの型決定ロジック
型解決は `TableSchemaResolver` が担当。

- 実装: [TableSchemaResolver.java](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/src/main/java/com/snowflake/kafka/connector/internal/schemaevolution/TableSchemaResolver.java#L53-L165)

分岐:

- schema あり: Kafka Connect schema から型を取得
  - [TableSchemaResolver.java](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/src/main/java/com/snowflake/kafka/connector/internal/schemaevolution/TableSchemaResolver.java#L134-L153)
- schema なし: JSON 値から型推論
  - [TableSchemaResolver.java](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/src/main/java/com/snowflake/kafka/connector/internal/schemaevolution/TableSchemaResolver.java#L109-L126)
  - [TableSchemaResolver.java](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/src/main/java/com/snowflake/kafka/connector/internal/schemaevolution/TableSchemaResolver.java#L155-L165)

## 6. テスト根拠
公式テストでも「最初に metadata 列中心で作成し、その後 schema evolution で列追加」を明示。

- [test_se_auto_table_creation_json.py](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/test/tests/schema_evolution/test_se_auto_table_creation_json.py#L1-L9)

## 7. まとめ
通常の Snowflake テーブル前提では、挙動は次の通り。

1. テーブル未存在時は metadata 中心の最小 DDL で作成
2. 業務カラムは後段の schema evolution で追加
3. 追加カラム型は schema 有無で決まる（schema -> 明示型、schema-less -> 値推論）

このため、RAW テーブルへまず取り込み、後段で整形する運用は Kafka Connector 実装方針と整合する。
