"""Collection 元数据 PATCH 与筛选的纯单测：不访问数据库。"""
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from api_services.collection_service import build_collection_filters, update_collection
from constants.collection_constants import (
    COLLECTION_EXACT_FILTER_FIELDS,
    COLLECTION_JSON_FILTER_FIELDS,
)
from models.collection_models import CollectionCreate, CollectionPatch
from models.filter_models import CollectionFilter


@pytest.mark.parametrize("payload", [
    {"name": None},
    {"member_type": None},
    {"doi": None},
    {"abstract": None},
])
def test_patch_rejects_explicit_null(payload):
    """非空列显式传 null → 请求校验失败（FastAPI 返回 422）。"""
    with pytest.raises(ValidationError, match="must not be null"):
        CollectionPatch(**payload)


def test_patch_allows_explicit_null_publish_time():
    """publish_time 可空，显式 null 用于清空。"""
    assert CollectionPatch(publish_time=None).publish_time is None


@pytest.mark.parametrize("name", ["", "   "])
def test_name_must_not_be_blank(name):
    """name 去空白后不能为空（创建与 PATCH 同口径）。"""
    with pytest.raises(ValidationError):
        CollectionCreate(name=name)
    with pytest.raises(ValidationError):
        CollectionPatch(name=name)


def test_name_is_trimmed():
    assert CollectionCreate(name="  coll  ").name == "coll"
    assert CollectionPatch(name="  coll  ").name == "coll"


@pytest.mark.parametrize("field", ["abstract", "cite_information"])
def test_text_fields_are_capped(field):
    """TEXT 列按最坏 4 字节/字符预留：16000 字符可写入，16001 拒绝。"""
    assert len(getattr(CollectionCreate(name="c", **{field: "a" * 16000}), field)) == 16000
    with pytest.raises(ValidationError):
        CollectionCreate(name="c", **{field: "a" * 16001})


def test_list_fields_are_trimmed_deduplicated_and_drop_empty():
    create = CollectionCreate(
        name="c",
        organism=[" mouse ", "mouse", "", "Mouse"],
        member_type=[" MSI ", "MSI", "  "],
    )

    assert create.organism == ["mouse", "Mouse"]
    assert create.member_type == ["MSI"]


def test_patch_list_fields_are_normalized():
    patch = CollectionPatch(doi=[" 10.1/a ", "10.1/a", ""])

    assert patch.doi == ["10.1/a"]


async def test_update_collection_empty_patch_keeps_updated_at():
    """空 PATCH 不刷新 updated_at（避免集合被顶到列表最前）。"""
    original = datetime(2026, 1, 1, tzinfo=timezone.utc)
    collection = SimpleNamespace(updated_at=original, name="old")
    added = []
    session = SimpleNamespace(add=added.append)

    await update_collection(session, collection, CollectionPatch())

    assert collection.updated_at == original
    assert added == []


async def test_update_collection_non_empty_patch_updates_updated_at():
    """有字段变更时仍刷新 updated_at。"""
    original = datetime(2026, 1, 1, tzinfo=timezone.utc)
    collection = SimpleNamespace(updated_at=original, name="old")
    added = []
    session = SimpleNamespace(add=added.append)

    await update_collection(session, collection, CollectionPatch(name="new"))

    assert collection.name == "new"
    assert collection.updated_at > original
    assert added == [collection]


def test_type_fields_use_json_filter_group():
    """member_type / collection_type 已是 JSON 数组，不再走 IN 精确匹配。"""
    for field in ("member_type", "collection_type"):
        assert field in COLLECTION_JSON_FILTER_FIELDS
        assert field not in COLLECTION_EXACT_FILTER_FIELDS


async def test_member_type_filter_builds_json_overlaps():
    """按 member_type 筛选用 JSON_OVERLAPS 判交集，而不是 IN。"""
    clauses = await build_collection_filters(None, CollectionFilter(member_type=["MSI"]))

    assert len(clauses) == 1
    sql = str(clauses[0]).lower()
    assert "json_overlaps" in sql
    assert "member_type" in sql
    assert " in " not in sql