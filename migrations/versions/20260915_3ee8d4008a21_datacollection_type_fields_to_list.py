"""convert datacollection member_type / collection_type to JSON lists

Revision ID: 3ee8d4008a21
Revises: 3840a26d8fa5
Create Date: 2026-09-15 14:00:00.000000

集合的 member_type（包含的数据类型，如 MSI、H&E、ST）与 collection_type（构成方式，如连续切片、
时间序列）需求确认为多选，与 organism 等汇总字段一样改为 JSON 列表：
- VARCHAR(32) → JSON，NOT NULL
- 存量数据回填：空串 → []，非空 'MSI' → ['MSI']（去首尾空白）

步骤：先放宽为 TEXT（回填后的 JSON 文本可能超过 32 字符）→ 逐行回填 JSON 文本 → 再改为 JSON。
MariaDB 的 JSON 是 LONGTEXT + json_valid 检查，回填后数据已合法，直接 ALTER 即可。

注意（降级有损）：列表回写为逗号拼接的字符串并截断到 32 字符。
"""
import json
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '3ee8d4008a21'
down_revision: str | Sequence[str] | None = '3840a26d8fa5'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLE = 'datacollection'
COLUMNS = ('member_type', 'collection_type')
LEGACY_LENGTH = 32


def _alter_columns(type_: sa.types.TypeEngine, existing_type: sa.types.TypeEngine) -> None:
    with op.batch_alter_table(TABLE) as batch_op:
        for column in COLUMNS:
            batch_op.alter_column(column, type_=type_, existing_type=existing_type, existing_nullable=False)


def _rewrite_rows(convert) -> None:
    """逐行把 COLUMNS 的值按 convert 改写（集合表规模小，逐行更新即可，且与方言无关）"""
    bind = op.get_bind()
    table = sa.table(TABLE, sa.column('id'), *(sa.column(c) for c in COLUMNS))
    rows = bind.execute(sa.select(table.c.id, *(table.c[c] for c in COLUMNS))).all()
    for row in rows:
        values = {c: convert(getattr(row, c)) for c in COLUMNS}
        bind.execute(sa.update(table).where(table.c.id == row.id).values(**values))


def _to_json_list(value: str | None) -> str:
    text = (value or '').strip()
    return json.dumps([text] if text else [], ensure_ascii=False)


def _to_legacy_string(value: str | None) -> str:
    try:
        items = json.loads(value) if value else []
    except ValueError:
        return (value or '')[:LEGACY_LENGTH]
    if not isinstance(items, list):
        return str(items)[:LEGACY_LENGTH]
    return ', '.join(str(item) for item in items)[:LEGACY_LENGTH]


def upgrade() -> None:
    """Upgrade schema."""
    _alter_columns(sa.Text(), sa.String(LEGACY_LENGTH))
    _rewrite_rows(_to_json_list)
    _alter_columns(sa.JSON(), sa.Text())


def downgrade() -> None:
    """Downgrade schema: JSON 列表回写为逗号拼接字符串（截断到 32 字符，有损）"""
    _alter_columns(sa.Text(), sa.JSON())
    _rewrite_rows(_to_legacy_string)
    _alter_columns(sa.String(LEGACY_LENGTH), sa.Text())
