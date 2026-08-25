# landing_sync

RAW テーブル（VARIANT）から Landing テーブル（型付きカラム）へのスキーマ同期・データ同期を行う CLI / ライブラリ。

## 役割

```text
RAW テーブル (VARIANT)  →  [landing_sync]  →  Landing テーブル (型付きカラム)
```

- RAW テーブルの `PAYLOAD:schema:fields` からカラム定義を取得
- Debezium type/name → Snowflake 型に変換
- Landing テーブルを自動作成・カラム追加（schema evolution）
- RAW → Landing への MERGE（INSERT / UPDATE / DELETE）

詳細は [設計方針](../../docs/LandingSync設計方針.md) を参照。
Python ライブラリとしての利用方法は [README_PYLIB.md](README_PYLIB.md) を参照。

## セットアップ

```bash
cd src/landing_sync
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
```

`.env` を環境に合わせて編集してください。

## CLI 利用

Compaction と同じように、コマンドラインから Snowflake に接続して RAW テーブルを参照できます。

```bash
python main.py --raw-table KAFKA_DB.KAFKA_SCHEMA.RAW_TABLE
```

環境変数を使う場合は `.env` に接続情報と `RAW_TABLE` を入れて実行できます。
`SNOWFLAKE_PRIVATE_KEY_PATH` を指定すると、秘密鍵ファイルをそのまま使えます。

```bash
python main.py
```

## ライブラリ利用

Python から直接使う場合は [README_PYLIB.md](README_PYLIB.md) を参照してください。

## ファイル構成

```text
landing_sync/
  main.py              # CLI エントリポイント
  __init__.py             # パッケージエクスポート
  config.py               # Snowflake 接続設定（pydantic-settings）
  snowflake_client.py     # 接続・クエリ実行クライアント
  landing_sync.py         # オーケストレーター
```
