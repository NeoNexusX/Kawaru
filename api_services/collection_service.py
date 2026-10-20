#########################
# Collection Service - 文件集合业务逻辑层
# 职责:集合 CRUD、成员管理（含排序）、权限校验、公开页数据聚合
#########################
from __future__ import annotations

import json
from typing import Any

from sqlalchemy import false
from sqlmodel import select, and_, col, delete, func, or_
from sqlmodel.sql.expression import SelectOfScalar
from sqlmodel.ext.asyncio.session import AsyncSession

from api_services.file_service import get_file_ids_by_public_ids
from api_services.user_service import count_items, get_user_by_id
from api_helpers.deletion_helper import file_is_deleting
from constants.collection_constants import (
    COLLECTION_EXACT_FILTER_FIELDS,
    COLLECTION_FUZZY_FILTER_FIELDS,
    COLLECTION_JSON_FILTER_FIELDS,
    MAX_COLLECTION_MEMBERS,
    ALLOWED_MEMBER_EXPERIMENT_TYPES,
)
from exceptions.collection_exceptions import (
    collection_not_found_exception,
    collection_member_limit_exception,
    collection_permission_denied_exception,
    invalid_members_exception,
    member_order_conflict_exception,
)
from models.collection_models import (
    CollectionCreate,
    CollectionDetail,
    CollectionMember,
    CollectionPatch,
    CollectionPublic,
    DataCollectionInDB,
)
from models.file_models import FileInDB, FilePublic, FileStatus
from models.filter_models import CollectionFilter
from models.user_models import UserInDB
from tools import get_logger, utc_time

logger = get_logger("api_services.collection_service")


def member_visibility_clause() -> Any:
    """集合成员的读侧口径（单一出口）：只算 status == COMPLETED 的文件

    加入成员时就要求 COMPLETED（validate_member_files），删除中的文件又已在登记事务内
    摘掉成员行，所以这里是防老数据/异常态的兜底；同时它让集合列表、owner 详情、公开页
    三处口径完全一致（DELETING / DELETE_FAILED 不是 COMPLETED，天然被排除）。
    调序的成员全集比对必须用同一口径——否则 UI 看不到的成员会让调序永远 409。
    """
    return FileInDB.status == FileStatus.COMPLETED


#########################
# Collection - 查（基础记录查询）
#########################
async def get_collection_by_id(
    session: AsyncSession, collection_id: int
) -> DataCollectionInDB:
    """按数字 id 查集合（管理端用）

    Raises:
        collection_not_found_exception: 集合不存在
    """
    collection = (
        await session.exec(
            select(DataCollectionInDB).where(DataCollectionInDB.id == collection_id)
        )
    ).first()
    if not collection:
        logger.warning("collection not found collection_id=%s", collection_id)
        raise collection_not_found_exception
    return collection


async def get_collection_id_by_public_id(
    session: AsyncSession, public_id: str
) -> int:
    """按 public_id 查内部数字 id（对外接口统一入口：外部一律 public_id，内部一律数字 id）

    只取 id 一列，不加载整行（调用方随后按 id 走 get_owned_collection 等内部函数）。

    Raises:
        collection_not_found_exception: 集合不存在
    """
    collection_id = (
        await session.exec(
            select(DataCollectionInDB.id).where(DataCollectionInDB.public_id == public_id)  # type: ignore
        )
    ).first()
    if not collection_id:
        logger.warning("collection not found public_id=%s", public_id)
        raise collection_not_found_exception
    return collection_id


async def get_public_collection_by_public_id(
    session: AsyncSession, public_id: str
) -> DataCollectionInDB:
    """按 public_id 查公开集合（公开页用，免登录）

    非公开集合即使 public_id 正确也按 404 处理，不暴露存在性。

    Raises:
        collection_not_found_exception: 集合不存在或非公开
    """
    collection = (
        await session.exec(
            select(DataCollectionInDB).where(DataCollectionInDB.public_id == public_id)
        )
    ).first()
    if not collection or not collection.is_public:
        logger.warning("public collection not found public_id=%s", public_id)
        raise collection_not_found_exception
    return collection


def assert_collection_owner(collection: DataCollectionInDB, current_user: UserInDB) -> bool:
    """校验用户是否可修改该集合（与 assert_file_owner 同构）

    admin 与 owner 拥有相同的集合操作权限。

    Returns:
        True 表示有权限,False 表示无权限
    """
    if current_user.identity == "admin":
        return True
    return collection.owner_id == current_user.id


async def get_owned_collection(
    session: AsyncSession,
    collection_id: int,
    current_user: UserInDB,
) -> DataCollectionInDB:
    """查集合并校验修改权（owner 或 admin）——管理端 owner 系操作的统一入口

    Raises:
        collection_not_found_exception: 集合不存在
        collection_permission_denied_exception: 非 owner/admin
    """
    collection = await get_collection_by_id(session, collection_id)
    if not assert_collection_owner(collection, current_user):
        logger.warning(
            "permission denied user=%s collection_id=%s", current_user.id, collection_id
        )
        raise collection_permission_denied_exception
    return collection


#########################
# Collection - 增
#########################
async def create_collection(
    session: AsyncSession,
    current_user: UserInDB,
    data: CollectionCreate,
    member_file_ids: list[int],
) -> DataCollectionInDB:
    """创建集合 + 初始成员（member_file_ids 数组顺序即初始 position 1..n）

    成员资格校验与加成员共用 validate_member_files。
    请求以 public_id 标识成员，由 api 层换算成 file_id 传入（本层只认内部 file_id）。

    Raises:
        collection_member_limit_exception: 初始成员数超过上限
        HTTPException(409): 存在不合法的成员文件
    """
    fields = data.model_dump(exclude={"file_public_ids"})

    # 成员资格校验（创建与加成员共用）
    files = await validate_member_files(session, member_file_ids)

    # 成员数上限与 add_members 同口径（validate_member_files 已去重，len(files) 即有效唯一成员数）
    if len(files) > MAX_COLLECTION_MEMBERS:
        logger.warning(
            "collection member limit exceeded at create user=%s members=%s",
            current_user.id, len(files),
        )
        raise collection_member_limit_exception

    collection = DataCollectionInDB(**fields, owner_id=current_user.id) # type: ignore
    session.add(collection)
    await session.flush()  # 拿自增 id，供成员外键使用

    for position, file in enumerate(files, start=1):
        session.add(
            CollectionMember(
                collection_id=collection.id,
                file_id=file.file_id,
                position=position,
                added_by=current_user.id,
            )
        )

    logger.info(
        "collection created user=%s collection_id=%s members=%s",
        current_user.id, collection.id, len(files),
    )
    return collection


#########################
# Collection - 改
#########################
async def update_collection(
    session: AsyncSession,
    collection: DataCollectionInDB,
    patch: CollectionPatch,
) -> None:
    """部分更新集合元数据（只改请求中出现的字段；成员不在 PATCH 中修改）"""
    updates = patch.model_dump(exclude_unset=True)

    for field, value in updates.items():
        setattr(collection, field, value)
    collection.updated_at = utc_time()
    session.add(collection)


async def delete_collection(
    session: AsyncSession,
    collection: DataCollectionInDB,
) -> None:
    """删除集合：只删集合行，成员行由外键 CASCADE 删除，不动任何文件"""
    assert collection.id is not None
    await session.delete(collection)
    logger.info("collection deleted collection_id=%s", collection.id)


#########################
# Collection - 列表 / 详情聚合
#########################
async def list_user_collections(
    session: AsyncSession,
    current_user: UserInDB,
    filters: CollectionFilter,
    page: int,
    size: int,
) -> tuple[list[CollectionPublic], int]:
    """当前用户自己的集合列表分页查询（按 updated_at 倒序），供集合管理与"加入集合"选择器

    filters 对两个列表接口语义一致（"我的集合"下按 owner 筛选通常无意义，但无害）。

    Returns:
        (当前页集合列表, 符合条件的记录总数)
    """
    base_query = select(DataCollectionInDB).where(DataCollectionInDB.owner_id == current_user.id)
    base_query = await _apply_collection_filters(session, base_query, filters)
    return await _paginate_collections(session, base_query, page, size)


async def list_all_collections(
    session: AsyncSession,
    filters: CollectionFilter,
    page: int,
    size: int,
) -> tuple[list[CollectionPublic], int]:
    """数据表内所有用户的集合列表分页查询（按 updated_at 倒序），供登录后的全量集合列表

    Returns:
        (当前页集合列表, 符合条件的记录总数)
    """
    base_query = await _apply_collection_filters(session, select(DataCollectionInDB), filters)
    return await _paginate_collections(session, base_query, page, size)


async def _apply_collection_filters(
    session: AsyncSession,
    base_query: SelectOfScalar[DataCollectionInDB],
    filters: CollectionFilter,
) -> SelectOfScalar[DataCollectionInDB]:
    """把 CollectionFilter 生成的谓词挂到查询上（同字段多值 OR、不同字段之间 AND）"""
    for clause in await build_collection_filters(session, filters):
        base_query = base_query.where(clause)
    return base_query


async def build_collection_filters(
    session: AsyncSession,
    filters: CollectionFilter,
) -> list[Any]:
    """CollectionFilter → SQL 谓词列表（同一字段多值 OR，不同字段之间 AND）

    三类字段的匹配方式（字段分组见 constants/collection_constants.py）：
    - 文本字段（name/title/journal_name）：OR 连接的 LIKE，大小写由列排序规则
      （utf8mb4_unicode_ci）保证不敏感
    - 枚举字段（member_type/collection_type）：IN，同上不敏感
    - JSON 数组字段（organism 等）：元素成员判断——集合内任一成员含该值即命中。
      用 JSON_OVERLAPS 判交集（多值天然 OR 语义），并强制 LOWER 两侧：
      JSON 值比较本身大小写敏感（与普通列排序规则不一致），故统一降为小写后比较。

    owner_username 需先模糊查用户 id（owner_id 有索引，无需 join 集合表）。

    空值不产生谓词（归一化已在 Filter 模型内完成，空值一律为 None）。
    """
    clauses: list[Any] = []

    for field in COLLECTION_FUZZY_FILTER_FIELDS:
        values = getattr(filters, field)
        if values:
            clauses.append(
                or_(*[col(getattr(DataCollectionInDB, field)).like(f"%{value}%") for value in values])
            )

    for field in COLLECTION_EXACT_FILTER_FIELDS:
        values = getattr(filters, field)
        if values:
            clauses.append(col(getattr(DataCollectionInDB, field)).in_(values))

    if filters.owner_username:
        owner_ids = (
            await session.exec(
                select(UserInDB.id).where(
                    or_(*[
                        col(UserInDB.username).like(f"%{username}%")
                        for username in filters.owner_username
                    ])
                )
            )
        ).all()
        clauses.append(
            DataCollectionInDB.owner_id.in_(list(owner_ids)) if owner_ids else false() # type: ignore # pylint: disable=no-member
        )

    for field in COLLECTION_JSON_FILTER_FIELDS:
        values = getattr(filters, field)
        if values:
            # 值数组交给 JSON_OVERLAPS 判交集：集合含任一选中值即命中（同字段多值 OR）
            needle = json.dumps([value.lower() for value in values])
            clauses.append(
                func.json_overlaps(func.lower(getattr(DataCollectionInDB, field)), needle) == 1 # type: ignore
            )

    return clauses


async def _paginate_collections(
    session: AsyncSession,
    base_query: SelectOfScalar[DataCollectionInDB],
    page: int,
    size: int,
) -> tuple[list[CollectionPublic], int]:
    """集合列表分页公共实现：分页查询 + 当前页成员数/总大小聚合 + owner 用户名回填"""
    # 总数（分页元数据；口径与列表查询一致）
    total_records = await count_items(session, base_query, DataCollectionInDB)

    collections = (
        await session.exec(
            base_query
            .order_by(DataCollectionInDB.updated_at.desc()) # type: ignore # pylint: disable=no-member
            .offset((page - 1) * size)
            .limit(size)
        )
    ).all()
    if not collections:
        return [], total_records

    # 单条分组查询聚合当前页集合的成员数与总大小（避免逐集合 2N+1 查询）
    stats_rows = (
        await session.exec(
            select(
                CollectionMember.collection_id,
                func.count(),  # pylint: disable=not-callable
                func.coalesce(func.sum(FileInDB.size), 0),  # pylint: disable=not-callable
            )
            .join(FileInDB, CollectionMember.file_id == FileInDB.file_id) # type: ignore
            .where(CollectionMember.collection_id.in_([ # type: ignore # pylint: disable=no-member
                collection.id for collection in collections
            ]))
            .where(member_visibility_clause())
            .group_by(CollectionMember.collection_id) #type: ignore
        )
    ).all()
    stats = {row[0]: (int(row[1]), int(row[2])) for row in stats_rows}

    # 单条查询回填当前页集合的 owner 用户名（"我的集合"与全量列表共用同一口径）
    owner_rows = (
        await session.exec(
            select(UserInDB.id, UserInDB.username).where(
                UserInDB.id.in_(list({collection.owner_id for collection in collections})) # type: ignore # pylint: disable=no-member
            )
        )
    ).all()
    owner_names = {row[0]: row[1] for row in owner_rows}

    result = []
    for collection in collections:
        member_count, total_size = stats.get(collection.id, (0, 0))
        result.append(
            CollectionPublic.model_validate(
                collection,
                update={
                    "owner_username": owner_names.get(collection.owner_id, ""),
                    "member_count": member_count,
                    "total_size": total_size,
                },
            )
        )
    return result, total_records


async def build_collection_detail(
    session: AsyncSession,
    collection: DataCollectionInDB,
) -> CollectionDetail:
    """聚合集合详情：元数据 + 成员文件列表（按 position 排序）+ 成员数/总大小 + owner 用户名

    成员口径与公开页一致（见 member_visibility_clause）：不再区分 owner 视角与公开视角。
    """
    assert collection.id is not None
    files = await get_member_files(session, collection.id)
    member_count, total_size = await get_member_stats(session, collection.id)
    owner = await get_user_by_id(collection.owner_id, session)

    return CollectionDetail.model_validate(
        collection,
        update={
            # owner 用户名供公开页展示；响应不含数字 id（CollectionDetail 骨架已剔除）
            "owner_username": owner.username if owner else "",
            "member_count": member_count,
            "total_size": total_size,
            "members": [FilePublic.model_validate(file) for file in files],
        },
    )


#########################
# Member - 查
#########################
async def get_member_file_ids(
    session: AsyncSession, collection_id: int
) -> list[int]:
    """获取集合成员的文件 ID 列表（不加载文件对象）"""
    stmt = select(CollectionMember.file_id).where(
        CollectionMember.collection_id == collection_id
    )
    res = await session.exec(stmt)
    return list(res.all()) #type: ignore


async def get_member_files(
    session: AsyncSession,
    collection_id: int,
) -> list[FileInDB]:
    """获取集合成员文件列表，固定按 position 排序（file_id 兜底稳定排序）"""
    stmt = (
        select(FileInDB)
        .join(CollectionMember, CollectionMember.file_id == FileInDB.file_id) # type: ignore
        .where(CollectionMember.collection_id == collection_id)
        .where(member_visibility_clause())
    )
    stmt = stmt.order_by(CollectionMember.position, FileInDB.file_id) # type: ignore
    files = (await session.exec(stmt)).all()
    return list(files)


async def get_member_stats(
    session: AsyncSession,
    collection_id: int,
) -> tuple[int, int]:
    """成员数与总大小（实时聚合，不落库；返回 (member_count, total_size)）

    口径与 get_member_files 一致，避免"数量比列表多"的矛盾。
    """
    stmt = (
        select(
            func.count(),  # pylint: disable=not-callable
            func.coalesce(func.sum(FileInDB.size), 0),  # pylint: disable=not-callable
        )
        .select_from(CollectionMember)
        .join(FileInDB, CollectionMember.file_id == FileInDB.file_id) # type: ignore
        .where(CollectionMember.collection_id == collection_id)
        .where(member_visibility_clause())
    )
    count, total_size = (await session.exec(stmt)).one()
    return int(count), int(total_size)


#########################
# Member - 校验器（创建与加成员共用）
#########################
async def resolve_member_file_ids(
    session: AsyncSession,
    public_ids: list[str],
) -> list[int]:
    """成员引用换算（对外接口统一入口）：成员 public_id → file_id（保序、保留重复）

    API 层调用本函数完成换算后，成员相关 service 函数一律收 file_id。
    幂等去重/重复校验交给 add_members / validate_member_files；
    查不到的 public_id（文件不存在）按 not_found 口径整批 409，
    与 validate_member_files 的 not_found 语义一致（创建集合与加成员共用）。

    Raises:
        HTTPException(409): 有 public_id 无法换算（invalid_members_exception）
    """
    id_map = await get_file_ids_by_public_ids(session, public_ids)
    missing = [p for p in dict.fromkeys(public_ids) if p not in id_map]
    if missing:
        logger.warning(
            "invalid collection members public_ids=%s reasons=%s",
            missing, {p: "not_found" for p in missing},
        )
        raise invalid_members_exception
    return [id_map[p] for p in public_ids]


async def validate_member_files(
    session: AsyncSession, file_ids: list[int]
) -> list[FileInDB]:
    """成员资格校验：文件存在 → status == completed → is_public → experiment_type 合法

    "必须是公开文件"的校验第一期保留（当前恒真），未来引入私有文件时即为安全闸。
    请求内重复 id 去重（保序）——否则重复成员会触发联合主键冲突（500）。

    Raises:
        HTTPException(409): 存在不合法文件
    """
    if not file_ids:
        return []

    unique_ids = list(dict.fromkeys(file_ids))
    # 行锁（主键 IN，锁序与登记侧一致：file 行 → 成员行）：与文件删除登记并发时，
    # 先拿 file 行锁可避免「登记删成员行 / 加成员插成员行」锁序相反的死锁，
    # 同时关掉「给正在删除的文件加成员」的窗口（登记提交后这里读到 DELETING → 409）
    records = (
        await session.exec(
            select(FileInDB)
            .where(FileInDB.file_id.in_(unique_ids))  # type: ignore # pylint: disable=no-member
            .with_for_update()
        )
    ).all()
    found = {record.file_id: record for record in records}

    reasons: dict[str, str] = {}
    valid: list[FileInDB] = []
    for file_id in unique_ids:
        record = found.get(file_id)
        # 删除流程中的文件按不存在处理（与 get_file_record_by_id 的 404 语义一致）
        if record is None or file_is_deleting(record.status):
            reasons[str(file_id)] = "not_found"
        elif record.status != FileStatus.COMPLETED:
            reasons[str(file_id)] = "not_completed"
        elif not record.is_public:
            reasons[str(file_id)] = "not_public"
        elif record.experiment_type not in ALLOWED_MEMBER_EXPERIMENT_TYPES:
            reasons[str(file_id)] = "invalid_experiment_type"
        else:
            valid.append(record)

    if reasons:
        logger.warning(
            "invalid collection members file_ids=%s reasons=%s",
            list(reasons.keys()), reasons,
        )
        raise invalid_members_exception

    return valid


#########################
# Member - 增 / 删 / 排序
#########################
async def add_members(
    session: AsyncSession,
    collection: DataCollectionInDB,
    current_user: UserInDB,
    file_ids: list[int],
) -> int:
    """往集合追加成员（owner 或 admin，权限由 api 层校验）

    - 幂等跳过：请求内重复、已在集合内的文件直接跳过，只加新文件
    - 新成员 position 取 MAX(position)+1 起追加到末尾
    - 追加后刷新集合 updated_at（前端"最近更新"排序依赖）

    Returns:
        实际新增的成员数

    Raises:
        collection_member_limit_exception: 超过成员数量上限
        HTTPException(409): 存在不合法的成员文件
    """
    assert collection.id is not None
    existing = set(await get_member_file_ids(session, collection.id))

    new_ids: list[int] = []
    seen: set[int] = set()
    for file_id in file_ids:
        if file_id in seen or file_id in existing:
            continue
        seen.add(file_id)
        new_ids.append(file_id)

    if not new_ids:
        return 0

    if len(existing) + len(new_ids) > MAX_COLLECTION_MEMBERS:
        logger.warning(
            "collection member limit exceeded collection_id=%s current=%s new=%s",
            collection.id, len(existing), len(new_ids),
        )
        raise collection_member_limit_exception

    files = await validate_member_files(session, new_ids)

    max_position = (
        await session.exec(
            select(func.max(CollectionMember.position))  # pylint: disable=not-callable
            .where(CollectionMember.collection_id == collection.id)
        )
    ).one() or 0

    for offset, file in enumerate(files, start=1):
        session.add(
            CollectionMember(
                collection_id=collection.id,
                file_id=file.file_id,
                position=max_position + offset,
                added_by=current_user.id,
            )
        )

    collection.updated_at = utc_time()
    session.add(collection)

    logger.info(
        "collection members added collection_id=%s user=%s added=%s skipped=%s",
        collection.id, current_user.id, len(files), len(file_ids) - len(files),
    )
    return len(files)


async def remove_members(
    session: AsyncSession,
    collection: DataCollectionInDB,
    file_ids: list[int],
) -> tuple[list[int], list[int]]:
    """批量移除集合成员（owner 或 admin，权限由 api 层校验）：只删链接行，不动文件

    与批量添加的幂等语义对称：不在集合内的 file_id 跳过、不报错——
    批量场景下误选一个不存在的 id 不应让整批失败。
    与单删一致：position 空洞保留不重排（下次全量调序自然压紧）。

    Returns:
        (实际移除的 file_id 列表, 跳过的 file_id 列表)
    """
    assert collection.id is not None
    if not file_ids:
        return [], []

    unique_ids = list(dict.fromkeys(file_ids))
    current_ids = set(await get_member_file_ids(session, collection.id))
    to_remove = [file_id for file_id in unique_ids if file_id in current_ids]
    skipped = [file_id for file_id in unique_ids if file_id not in current_ids]

    if to_remove:
        await session.exec(
            delete(CollectionMember).where(
                and_(
                    CollectionMember.collection_id == collection.id,
                    CollectionMember.file_id.in_(to_remove),  # type: ignore # pylint: disable=no-member
                )
            )
        )
        collection.updated_at = utc_time()
        session.add(collection)

    logger.info(
        "collection members removed collection_id=%s removed=%s skipped=%s",
        collection.id, len(to_remove), len(skipped),
    )
    return to_remove, skipped


async def reorder_members(
    session: AsyncSession,
    collection: DataCollectionInDB,
    file_ids: list[int],
) -> None:
    """调整成员顺序（owner 或 admin，权限由 api 层校验）：全量数组重写 position = 1..n

    请求体必须恰好等于集合当前成员全集（缺失/多出/重复 → 409；
    不存在的文件 id 必然使集合比对不一致，同样 409，不会被静默跳过）。
    与其他用户并发加/删成员时，事务内成员集与请求快照不一致即 409，
    前端刷新后重试（乐观并发控制）。

    Raises:
        HTTPException(409): 数组与成员全集不一致或含重复
    """
    assert collection.id is not None
    if len(file_ids) != len(set(file_ids)):
        logger.warning(
            "reorder failed, duplicate file ids collection_id=%s", collection.id
        )
        raise member_order_conflict_exception

    # 成员全集比对用与读侧同一口径（member_visibility_clause）：被口径隐藏的成员
    # 不在全集里，UI 发来的可见成员数组才能对上；隐藏成员保留旧 position（允许空洞/重复）
    members = (
        await session.exec(
            select(CollectionMember)
            .join(FileInDB, CollectionMember.file_id == FileInDB.file_id) # type: ignore
            .where(CollectionMember.collection_id == collection.id)
            .where(member_visibility_clause())
        )
    ).all()
    current_ids = {member.file_id for member in members}

    request_ids = set(file_ids)
    if request_ids != current_ids:
        logger.warning(
            "reorder failed, member set changed collection_id=%s missing=%s extra=%s",
            collection.id,
            sorted(current_ids - request_ids), # type: ignore
            sorted(request_ids - current_ids),
        )
        raise member_order_conflict_exception

    member_by_file_id = {member.file_id: member for member in members}
    for position, file_id in enumerate(file_ids, start=1):
        member = member_by_file_id[file_id]
        member.position = position
        session.add(member)

    collection.updated_at = utc_time()
    session.add(collection)

    logger.info("collection members reordered collection_id=%s", collection.id)
