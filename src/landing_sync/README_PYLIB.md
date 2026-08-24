# LandingSync Python ライブラリとしての利用方法

## 1. インストール方法

whl ファイルをプロジェクトの vendor ディレクトリ等にコピーし、requirements.txt でパスを指定してインストールしてください。

```txt
./vendor/landing_sync-0.1.0-py3-none-any.whl
```

またはローカルで直接インストール:

```bash
pip install -e ./src/landing_sync
```

## 2. Python コードからの利用例

```python
from landing_sync import LandingSync, LandingSyncOptions, SnowflakeConfig

options = LandingSyncOptions(
    source_type="sqlserver",  # "oracle" / "sqlserver" / "mysql"
    dry_run=False,
)

snowflake_config = SnowflakeConfig(
    account="your_account",
    user="your_user",
    private_key_path="./rsa_key.pem",
    private_key_passphrase="your_passphrase",  # 任意
    warehouse="COMPUTE_WH",
    role="your_role",  # 任意
)

sync = LandingSync(options, snowflake_config)

# 接続確認
result = sync.run()
print(result)  # [{'ALIVE': 1}]
```

## 3. whl ファイルのビルド方法

```bash
cd src/landing_sync
pip install hatchling
python -m hatchling build
# dist/landing_sync-0.1.0-py3-none-any.whl が生成される
```
