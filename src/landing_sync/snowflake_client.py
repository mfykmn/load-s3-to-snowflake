from __future__ import annotations

from typing import Any, Optional

import snowflake.connector
import structlog
from snowflake.connector import DictCursor
from snowflake.connector.connection import SnowflakeConnection

from .config import Settings

logger = structlog.get_logger()


class SnowflakeClient:
    """Snowflake 接続とクエリ実行を行うクライアント"""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.connection: Optional[SnowflakeConnection] = None
        self.logger = logger.bind(component="SnowflakeClient")

    def connect(self) -> None:
        """Snowflake への接続を確立する"""
        connection_params: dict[str, Any] = {
            "account": self.settings.SNOWFLAKE_ACCOUNT,
            "user": self.settings.SNOWFLAKE_USER,
            "warehouse": self.settings.SNOWFLAKE_WAREHOUSE,
            "login_timeout": self.settings.SNOWFLAKE_LOGIN_TIMEOUT,
            "network_timeout": self.settings.SNOWFLAKE_NETWORK_TIMEOUT,
        }

        if self.settings.SNOWFLAKE_PRIVATE_KEY_PATH:
            connection_params["private_key_file"] = self.settings.SNOWFLAKE_PRIVATE_KEY_PATH
            if self.settings.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE:
                connection_params["private_key_file_pwd"] = (
                    self.settings.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
                )
        elif self.settings.SNOWFLAKE_PRIVATE_KEY:
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            private_key_str = self.settings.SNOWFLAKE_PRIVATE_KEY
            if not private_key_str.startswith("-----BEGIN"):
                private_key_str = (
                    f"-----BEGIN PRIVATE KEY-----\n{private_key_str}\n-----END PRIVATE KEY-----"
                )

            passphrase = (
                self.settings.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE.encode()
                if self.settings.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
                else None
            )
            private_key = serialization.load_pem_private_key(
                private_key_str.encode(), password=passphrase, backend=default_backend()
            )
            connection_params["private_key"] = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        elif self.settings.SNOWFLAKE_PASSWORD:
            connection_params["password"] = self.settings.SNOWFLAKE_PASSWORD
        else:
            raise ValueError(
                "認証方式が指定されていません。"
                "SNOWFLAKE_PASSWORD, SNOWFLAKE_PRIVATE_KEY, または SNOWFLAKE_PRIVATE_KEY_PATH を設定してください。"
            )

        if self.settings.SNOWFLAKE_ROLE:
            connection_params["role"] = self.settings.SNOWFLAKE_ROLE

        self.logger.info(
            "Snowflake に接続中",
            account=self.settings.SNOWFLAKE_ACCOUNT,
            warehouse=self.settings.SNOWFLAKE_WAREHOUSE,
        )
        self.connection = snowflake.connector.connect(**connection_params)
        self.logger.info("Snowflake への接続に成功しました")

    def close(self) -> None:
        """接続を閉じる"""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("接続を閉じました")

    def execute_query(self, query: str) -> list[dict[str, Any]]:
        """クエリを実行し、結果を辞書のリストとして返す"""
        if not self.connection:
            raise RuntimeError("Snowflake に接続されていません。connect() を先に呼び出してください。")

        cursor = self.connection.cursor(DictCursor)
        try:
            cursor.execute(query)
            return cursor.fetchall() or []
        finally:
            cursor.close()
