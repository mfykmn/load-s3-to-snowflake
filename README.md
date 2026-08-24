# load-s3-to-snowflake

Debezium CDC データを S3 経由で Snowflake に取り込むパイプラインです。

## パイプライン概要

```text
S3 (Debezium JSON)
  → [COPY INTO]    → RAW テーブル    (VARIANT)
  → [LandingSync]  → Landing テーブル (型付きカラム)
  → [Compaction]   → Target テーブル  (整形済み)
```

- **COPY INTO** (`src/copy_into/`): S3 → RAW テーブルへのデータ転送
- **LandingSync** (`src/landing_sync/`): RAW テーブル → Landing テーブルへのスキーマ同期・データ同期
- **Compaction**: Landing → Target テーブルへの整形・MERGE（別リポジトリ）
