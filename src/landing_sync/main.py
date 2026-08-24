#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from landing_sync import LandingSync, LandingSyncOptions, SnowflakeConfig  # noqa: E402


def _env_or_default(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="LandingSync CLI - RAW テーブルから Landing テーブルへの同期処理",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
    RAW テーブル参照:
        python src/landing_sync/main.py \
            --raw-table KAFKA_DB.KAFKA_SCHEMA.RAW_TABLE

  環境変数を使う場合:
    export SNOWFLAKE_ACCOUNT=...
    export SNOWFLAKE_USER=...
        export SNOWFLAKE_PRIVATE_KEY_PATH=...
    python src/landing_sync/main.py
        """,
    )

    parser.add_argument("--source-type", default="sqlserver", choices=["oracle", "sqlserver", "mysql"])
    parser.add_argument(
        "--raw-table",
        default=_env_or_default("RAW_TABLE"),
        help="参照する RAW テーブル名（db.schema.table 形式）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="RAW テーブル参照時の取得件数（デフォルト: 10）",
    )
    parser.add_argument("--account", default=_env_or_default("SNOWFLAKE_ACCOUNT"), help="Snowflake account")
    parser.add_argument("--user", default=_env_or_default("SNOWFLAKE_USER"), help="Snowflake user")
    parser.add_argument("--password", default=_env_or_default("SNOWFLAKE_PASSWORD"), help="Snowflake password")
    parser.add_argument(
        "--private-key",
        default=_env_or_default("SNOWFLAKE_PRIVATE_KEY"),
        help="Snowflake private key (PEM)",
    )
    parser.add_argument(
        "--private-key-path",
        default=_env_or_default("SNOWFLAKE_PRIVATE_KEY_PATH"),
        help="Snowflake private key file path",
    )
    parser.add_argument(
        "--private-key-passphrase",
        default=_env_or_default("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        help="Snowflake private key passphrase",
    )
    parser.add_argument(
        "--warehouse",
        default=_env_or_default("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        help="Snowflake warehouse",
    )
    parser.add_argument("--role", default=_env_or_default("SNOWFLAKE_ROLE"), help="Snowflake role")
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_arguments()

    if not args.account:
        raise SystemExit("SNOWFLAKE_ACCOUNT or --account is required")
    if not args.user:
        raise SystemExit("SNOWFLAKE_USER or --user is required")

    if args.private_key_path:
        args.private_key = None

    if not args.password and not args.private_key and not args.private_key_path:
        raise SystemExit(
            "SNOWFLAKE_PASSWORD, SNOWFLAKE_PRIVATE_KEY, or SNOWFLAKE_PRIVATE_KEY_PATH is required"
        )

    options = LandingSyncOptions(source_type=args.source_type)
    snowflake_config = SnowflakeConfig(
        account=args.account,
        user=args.user,
        password=args.password,
        private_key=args.private_key,
        private_key_path=args.private_key_path,
        private_key_passphrase=args.private_key_passphrase,
        warehouse=args.warehouse,
        role=args.role,
    )

    sync = LandingSync(options, snowflake_config)
    if not args.raw_table:
        raise SystemExit("RAW_TABLE or --raw-table is required")

    result = sync.run(args.raw_table, args.limit)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
