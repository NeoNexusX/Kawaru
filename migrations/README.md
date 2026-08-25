# Alembic Database Migrations

Kawaru 数据库版本迁移脚本目录。

- 详细迁移与升级流程请参阅 [数据库迁移指南](../docs/zh/数据库迁移指南.md)。
- 新建迁移脚本请运行：`uv run alembic revision -m "<description>"`
- 升级到最新版本：`uv run python -m database.migration upgrade`
- 校验当前模型与数据库结构一致性：`uv run python -m database.migration verify`
