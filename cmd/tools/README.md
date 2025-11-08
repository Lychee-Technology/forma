Forma Tools CLI
===============

命令通过 `go run ./cmd/tools <command> [options]` 调用。

可用命令
--------
- `generate-attributes`：从 JSON Schema 生成 `<schema>_attributes.json`。
- `init-db`：创建 schema registry、entity main、EAV 数据表及索引。

generate-attributes
-------------------
选项（带默认值）：
- `-schema-dir`（`cmd/server/schemas`）：Schema 所在目录。
- `-schema`：Schema 名（不含 `.json`）；与 `-schema-file` 互斥。
- `-schema-file`：Schema 文件全路径；优先于 `-schema/-schema-dir`。
- `-out`：输出文件路径；默认写到 schema 同目录的 `<name>_attributes.json`。

示例：
- `go run ./cmd/tools generate-attributes -schema lead`
- `go run ./cmd/tools generate-attributes -schema-file /path/to/schema.json -out /tmp/schema_attributes.json`

init-db
-------
选项（可用环境变量同名大写带下划线作默认值）：
- `-db-host`（`DB_HOST`，默认 `localhost`）
- `-db-port`（`DB_PORT`，默认 `5432`）
- `-db-name`（`DB_NAME`，默认 `forma`）
- `-db-user`（`DB_USER`，默认 `postgres`）
- `-db-password`（`DB_PASSWORD`，默认空）
- `-db-ssl-mode`（`DB_SSL_MODE`，默认 `disable`）
- `-schema-table`（`SCHEMA_TABLE`，默认 `schema_registry`）
- `-eav-table`（`EAV_TABLE`，默认 `eav_data_2`）
- `-entity-main-table`（`ENTITY_MAIN_TABLE`，默认 `entity_main`）

示例：
- `go run ./cmd/tools init-db`
- `go run ./cmd/tools init-db -db-host 127.0.0.1 -db-name forma_dev -entity-main-table entity_main_dev -eav-table eav_dev`
