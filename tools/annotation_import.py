#########################
# SI 注释数据库导入工具
# 职责: 把本地 SI_DB.csv 写入 annotation_entry 表（单事务清空重写）
# 说明:
#   - 只读本地 CSV、只写数据库；不碰 OSS（CSV 由人工上传，路径配在 config.yaml）
#   - 导入即"全量替换"：同一条命令重复运行结果一致（天然幂等）
#   - InnoDB MVCC 保证读侧要么全是旧数据、要么全是新数据，不会读到半份
# 用法:
#   uv run python -m tools.annotation_import --file SI_DB.csv --dry-run   # 只校验
#   uv run python -m tools.annotation_import --file SI_DB.csv --yes       # 全量导入
#########################
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import re
import sys
from collections.abc import Iterator
from pathlib import Path

from sqlalchemy import delete, func, insert, select

from database.migration_config import initialize_migration_runtime
from models.annotation_models import AnnotationEntryInDB

EXPECTED_HEADER = ("ID", "Name", "Formula", "InChI", "Mass", "Origin")
ID_PATTERN = re.compile(r"^HMDB\d{7}$")
KNOWN_ORIGINS = frozenset({"endogenous", "exogenous", "expected", "unknown"})
# 列宽直接取自表模型：超长在入库前就报错，避免 MySQL 严格模式下的半截失败
MAX_LENGTHS = {
    field_name: AnnotationEntryInDB.__table__.c[field_name].type.length
    for field_name in ("name", "formula", "origin")
}
DEFAULT_BATCH_SIZE = 5000
PROGRESS_EVERY = 20000
HASH_CHUNK_SIZE = 1024 * 1024
# InChI 最长 2133 字符；csv 模块默认字段上限 131072，这里放宽
CSV_FIELD_SIZE_LIMIT = 10 ** 9


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="把 SI 注释库 CSV 导入 annotation_entry 表（单事务全量替换）"
    )
    parser.add_argument("--file", required=True, type=Path, help="本地 CSV 路径，如 SI_DB.csv")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"每批插入行数（默认 {DEFAULT_BATCH_SIZE}）",
    )
    parser.add_argument("--dry-run", action="store_true", help="只解析校验并打印统计，不写数据库")
    parser.add_argument("--yes", action="store_true", help="表非空时确认全量覆盖（防手滑）")
    return parser


def file_digest(path: Path) -> tuple[str, str, int]:
    """返回 (sha256, md5, size_bytes)。MD5 可与 OSS 控制台该对象的 ETag 人工核对。"""
    sha256 = hashlib.sha256()
    md5 = hashlib.md5()
    size = 0
    with path.open("rb") as fh:
        while chunk := fh.read(HASH_CHUNK_SIZE):
            sha256.update(chunk)
            md5.update(chunk)
            size += len(chunk)
    return sha256.hexdigest(), md5.hexdigest(), size


def iter_rows(path: Path, unknown_origins: set[str]) -> Iterator[dict[str, object]]:
    """解析并逐行校验 CSV，产出可直接 insert 的 dict。

    校验失败抛 ValueError（带行号）。unknown_origins 收集未知 Origin 值，供调用方告警后放行。
    """
    csv.field_size_limit(CSV_FIELD_SIZE_LIMIT)
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header is None or tuple(header) != EXPECTED_HEADER:
            raise ValueError(f"表头不符：期望 {EXPECTED_HEADER}，实际 {tuple(header or ())}")

        seen: set[str] = set()
        for line_no, row in enumerate(reader, start=2):
            if not row:
                continue  # 容忍空行
            if len(row) != len(EXPECTED_HEADER):
                raise ValueError(
                    f"第 {line_no} 行：期望 {len(EXPECTED_HEADER)} 列，实际 {len(row)} 列"
                )

            entry_key, name, formula, inchi, mass_text, origin = row
            if not all(value.strip() for value in row):
                raise ValueError(f"第 {line_no} 行：存在空值")
            if not ID_PATTERN.match(entry_key):
                raise ValueError(f"第 {line_no} 行：ID 格式非法 {entry_key!r}（期望 HMDB + 7 位数字）")
            if entry_key in seen:
                raise ValueError(f"第 {line_no} 行：ID 重复 {entry_key}")
            seen.add(entry_key)

            for field_name, value in (("name", name), ("formula", formula), ("origin", origin)):
                if len(value) > MAX_LENGTHS[field_name]:
                    raise ValueError(
                        f"第 {line_no} 行：{field_name} 超长（{len(value)} > {MAX_LENGTHS[field_name]}）"
                    )

            try:
                mass = float(mass_text)
            except ValueError as exc:
                raise ValueError(f"第 {line_no} 行：Mass 无法解析 {mass_text!r}") from exc

            if origin not in KNOWN_ORIGINS:
                unknown_origins.add(origin)

            yield {
                "entry_key": entry_key,
                "name": name,
                "formula": formula,
                "mass": mass,
                "origin": origin,
                "inchi": inchi,
            }


def validate_file(path: Path) -> tuple[int, set[str]]:
    """第一遍：完整解析校验（不写库），返回行数与未知 Origin 集合。"""
    unknown_origins: set[str] = set()
    count = 0
    for _ in iter_rows(path, unknown_origins):
        count += 1
    return count, unknown_origins


async def count_existing() -> int:
    from database.db import get_standalone_async_session  # pylint: disable=import-outside-toplevel

    async with get_standalone_async_session() as session:
        result = await session.scalar(select(func.count()).select_from(AnnotationEntryInDB))
        return int(result or 0)


async def replace_all(path: Path, batch_size: int) -> int:
    """单事务清空重写；返回写入行数（异常自动回滚，旧数据不受影响）。"""
    from database.db import get_standalone_async_session  # pylint: disable=import-outside-toplevel

    inserted = 0
    async with get_standalone_async_session() as session, session.begin():
        await session.execute(delete(AnnotationEntryInDB))
        batch: list[dict[str, object]] = []
        for row in iter_rows(path, set()):
            batch.append(row)
            if len(batch) >= batch_size:
                await session.execute(insert(AnnotationEntryInDB), batch)
                inserted += len(batch)
                batch.clear()
                if inserted % PROGRESS_EVERY == 0:
                    print(f"  已写入 {inserted} 行...", flush=True)
        if batch:
            await session.execute(insert(AnnotationEntryInDB), batch)
            inserted += len(batch)
    return inserted


async def main() -> int:
    args = build_parser().parse_args()
    path: Path = args.file
    if not path.is_file():
        print(f"❌ 文件不存在：{path}")
        return 1

    print(f"文件：{path}")
    sha256, md5, size = file_digest(path)
    print(f"大小：{size} 字节")
    print(f"MD5：{md5}（可与 OSS 控制台该对象的 ETag 核对）")
    print(f"SHA256：{sha256}")

    try:
        count, unknown_origins = validate_file(path)
    except ValueError as exc:
        print(f"❌ 校验失败：{exc}")
        return 1

    if count == 0:
        print("❌ CSV 没有数据行，拒绝清空数据表")
        return 1

    print(f"校验通过：{count} 行")
    if unknown_origins:
        print(f"⚠️  未知 Origin 值（已放行，请确认是否为上游新增）：{sorted(unknown_origins)}")

    if args.dry_run:
        print("--dry-run：未写数据库")
        return 0

    # database.db 在 import 时会调用 get_logger，只能在 init_logging 之后延迟导入
    initialize_migration_runtime()

    existing = await count_existing()
    if existing and not args.yes:
        print(f"❌ 表 annotation_entry 已有 {existing} 行；确认全量覆盖请加 --yes")
        return 1

    print(f"开始导入（表内现有 {existing} 行，将整体替换）...")
    inserted = await replace_all(path, max(1, args.batch_size))
    print(f"✅ 导入完成：{inserted} 行，单事务提交")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
