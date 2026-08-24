# LandingSync 設計方針

## 概要

`LandingSync` は、Snowflake の RAW テーブル（VARIANT）から Landing テーブル（型付きカラム）へのデータ同期を行う Python ライブラリ。

Kafka Connect による Snowflake Kafka Connector が担っていた schema evolution とデータ書き込みを、S3 経由のパイプラインで代替することを目的とする。

---

## パイプライン全体における位置づけ

```
S3 (Debezium JSON)
  → [COPY INTO]     → RAW テーブル    (VARIANT)    ← 既存・対象外
  → [LandingSync]   → Landing テーブル (型付き)     ← 本ライブラリ
  → [Compaction]    → Target テーブル  (整形済み)    ← 既存
```

---

## RAW テーブルの構造

```sql
CREATE TABLE KAFKA_DB.KAFKA_SCHEMA.RAW_TABLE (
    PAYLOAD          VARIANT,
    SOURCE_FILE      VARCHAR,
    SOURCE_ROW_NUMBER NUMBER,
    LOADED_AT        TIMESTAMP_NTZ
);
```

`PAYLOAD` に Debezium が出力した JSON がそのまま格納される。

---

## データソースのフォーマット

S3 上のデータは `schema` と `payload` が分離した JsonConverter envelope 形式。

```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "field": "after",
        "type": "struct",
        "fields": [
          { "field": "id",   "type": "int32",  "optional": false },
          { "field": "name", "type": "string", "optional": true  }
        ]
      },
      ...
    ]
  },
  "payload": {
    "op": "c",
    "after": { "id": 1, "name": "test" },
    "source": { "ts_ms": 1234567890 },
    "transaction": { "id": "...", "total_order": 1 },
    "before": null
  }
}
```

### VARIANT 内のパス対応

| 用途 | Snowflake パス |
|------|---------------|
| スキーマ定義 | `PAYLOAD:schema:fields` |
| 業務データ（変更後） | `PAYLOAD:payload:after` |
| 操作種別 | `PAYLOAD:payload:op` |
| ソース時刻 | `PAYLOAD:payload:source:ts_ms` |
| コミット LSN | `PAYLOAD:payload:source:commit_lsn` |
| コミット SCN | `PAYLOAD:payload:source:commit_scn` |
| トランザクション ID | `PAYLOAD:payload:transaction:id` |
| トランザクション内順序 | `PAYLOAD:payload:transaction:total_order` |

---

## Landing テーブルのカラム構成

### 業務カラム（動的）

`PAYLOAD:schema:fields` のうち `field = "after"` の子フィールド定義から動的に生成する。

### メタカラム（固定）

| カラム名 | 型 | 値の取得元 | 付与条件 |
|---------|---|-----------|---------|
| `__op` | `VARCHAR` | `PAYLOAD:payload:op` | 常時 |
| `__source_ts_ms` | `NUMBER` | `PAYLOAD:payload:source:ts_ms` | 常時 |
| `__transaction_id` | `VARCHAR` | `PAYLOAD:payload:transaction:id` | 常時 |
| `__transaction_total_order` | `NUMBER` | `PAYLOAD:payload:transaction:total_order` | 常時 |
| `__source_commit_scn` | `VARCHAR` | `PAYLOAD:payload:source:commit_scn` | ソースタイプ = oracle |
| `__source_commit_lsn` | `VARCHAR` | `PAYLOAD:payload:source:commit_lsn` | ソースタイプ = sqlserver |
| `__cdc_synced` | `TIMESTAMP_NTZ` | `CURRENT_TIMESTAMP()` | 常時（MERGE 実行時刻） |

---

## 型変換マッピング（Debezium → Snowflake）

| Debezium `type` | `name`（論理型） | Snowflake 型 |
|----------------|----------------|-------------|
| `int32` | なし | `NUMBER(10,0)` |
| `int64` | なし | `NUMBER(19,0)` |
| `string` | なし | `VARCHAR` |
| `boolean` | なし | `BOOLEAN` |
| `float32` | なし | `FLOAT` |
| `float64` | なし | `DOUBLE` |
| `bytes` | `org.apache.kafka.connect.data.Decimal` | `NUMBER(p, s)` |
| `int32` | `io.debezium.time.Date` | `DATE` |
| `int32` | `io.debezium.time.Time` | `TIME` |
| `int64` | `io.debezium.time.Timestamp` | `TIMESTAMP_NTZ` |
| `int64` | `io.debezium.time.MicroTimestamp` | `TIMESTAMP_NTZ` |
| `string` | `io.debezium.time.ZonedTimestamp` | `TIMESTAMP_TZ` |

---

## モジュール構成

Compaction の構造を参考に以下の責務で分割する。

```
src/
  landing_sync/
    raw_schema_fetcher.py       # RAW テーブルから after のフィールド定義を取得
    debezium_type_converter.py  # Debezium type/name → Snowflake 型変換
    landing_table_manager.py    # Landing テーブルの作成・ALTER TABLE
    raw_to_landing_merger.py    # RAW → Landing の MERGE SQL 生成・実行
    landing_sync.py             # 上記を束ねるオーケストレーター
```

---

## 処理フロー

```
landing_sync.run()
  │
  ├─ Step 1: RawSchemaFetcher
  │     RAW テーブルの PAYLOAD から schema.fields[field="after"] を取得
  │     → list[dict] 形式のフィールド定義を返す
  │     （Snowflake クエリ: SELECT + LATERAL FLATTEN）
  │
  ├─ Step 2: DebeziumTypeConverter
  │     フィールド定義の type/name を Snowflake 型文字列に変換
  │     （Python 内処理、クエリなし）
  │
  ├─ Step 3: LandingTableManager
  │     テーブル存在確認（INFORMATION_SCHEMA SELECT）
  │     → 未存在: CREATE TABLE（業務カラム + メタカラム）
  │     → 存在: 差分カラムを ALTER TABLE ADD COLUMN IF NOT EXISTS
  │
  └─ Step 4: RawToLandingMerger
        RAW テーブルの PAYLOAD を展開して Landing テーブルに MERGE
        op に応じて INSERT / UPDATE / DELETE（または論理削除）
        __cdc_synced = CURRENT_TIMESTAMP()
```

### Snowflake へのクエリ発行回数

| Step | クエリ | 回数 |
|------|-------|------|
| Step 1 | RAW テーブル SELECT + LATERAL FLATTEN | 1回 |
| Step 3 | INFORMATION_SCHEMA SELECT（存在確認） | 1回 |
| Step 3 | INFORMATION_SCHEMA SELECT（既存カラム取得） | 1回（存在時） |
| Step 3 | CREATE TABLE or ALTER TABLE | 1〜N回 |
| Step 4 | MERGE INTO | 1回 |

---

## 設定パラメータ

| パラメータ | 型 | デフォルト | 説明 |
|-----------|---|----------|------|
| `source_type` | `str` | 必須 | `oracle` / `sqlserver` / `mysql` |
| `raw_table` | `str` | 必須 | RAW テーブル名（`db.schema.table` 形式） |
| `landing_table` | `str` | 必須 | Landing テーブル名（`db.schema.table` 形式） |
| `primary_keys` | `list[str]` | 必須 | MERGE の結合キー列名リスト |
| `is_logical_delete` | `bool` | `True` | DELETE を論理削除（`__CDC_DELETED=true`）で処理するか |
| `dry_run` | `bool` | `False` | SQL を生成するが実行しない |
| `skip_schema_update` | `bool` | `False` | テーブル作成・ALTER をスキップする |

---

## Compaction との比較

| | LandingSync | Compaction |
|--|------------|------------|
| 入力 | RAW テーブル（VARIANT） | Landing テーブル（型付き） |
| 出力 | Landing テーブル（型付き） | Target テーブル（整形済み） |
| スキーマ情報源 | RAW の `PAYLOAD:schema:fields` | DDL テーブル |
| 型変換 | Debezium type/name → Snowflake | DB型 → Snowflake |
| MERGE のソース | PAYLOAD の VARIANT 展開 | Landing テーブルの型付きカラム |
