#########################
# Collection API - 文件集合接口层
# 管理端（需登录 token_authen）+ 公开端（免登录）
#########################
from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Query

from api.user_api import token_authen
from api_services.collection_service import (
    add_members,
    build_collection_detail,
    create_collection,
    delete_collection,
    get_collection_id_by_public_id,
    get_owned_collection,
    get_public_collection_by_public_id,
    list_all_collections,
    list_user_collections,
    remove_members,
    reorder_members,
    resolve_member_file_ids,
    update_collection,
)
from api_services.file_service import get_file_ids_by_public_ids
from database.db import AsyncSessionDep
from exceptions.collection_exceptions import (
    member_order_conflict_exception,
)
from models.filter_models import CollectionFilter, ListDataResponse, PaginationMeta
from models.collection_models import (
    CollectionCreate,
    CollectionDetail,
    CollectionMembersOperation,
    CollectionPatch,
)
from models.user_models import UserInDB

from tools import PublicIdPath, get_logger
logger = get_logger("api.collection_api")


#########################
# 管理端（需登录）
#########################
async def create_collection_api(
    data: CollectionCreate,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> CollectionDetail:
    """
    【创建集合接口】

    功能:创建集合并写入元数据与初始成员（file_public_ids 数组顺序即初始 position 1..n）

    权限:任何登录用户

    流程:
    1. 成员 public_id → file_id 换算（API 层），成员资格校验（存在 → completed → is_public → experiment_type 合法）
    2. 创建集合 + 成员链接行
    """
    member_file_ids = await resolve_member_file_ids(async_session, data.file_public_ids)
    collection = await create_collection(async_session, current_user, data, member_file_ids)
    await async_session.commit()
    await async_session.refresh(collection)

    detail = await build_collection_detail(async_session, collection)
    logger.info(f"create collection user={current_user.id} collection_id={collection.id}")
    return detail


async def list_collections_api(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
    filters: CollectionFilter,
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
) -> ListDataResponse:
    """
    【集合列表接口】

    功能:分页返回当前用户自己的集合（按 updated_at 倒序）

    用途:集合管理列表与"加入集合"选择器

    注意:
    - 筛选条件走 body（CollectionFilter，不筛选也要发 {}），page/size 走 query
    - 每个筛选字段可传单值或数组（旧前端单值兼容），同字段多值 OR、不同字段之间 AND
    - 分页参数 page≥1、1≤size≤100，越界由 FastAPI 返回 422
    - 响应结构与 list_user_files / list_files 一致（meta + data）
    """
    collections, total_records = await list_user_collections(
        async_session, current_user, filters, page, size
    )
    total_pages = (total_records + size - 1) // size
    pagination_meta = PaginationMeta(
        current_page=page,
        current_records=len(collections),
        total_pages=total_pages,
        total_records=total_records,
    )
    logger.info(
        f"list collections user={current_user.id} page={page} size={size} total={total_records}"
    )
    return ListDataResponse(meta=pagination_meta, data=collections)


async def list_all_collections_api(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
    filters: CollectionFilter,
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
) -> ListDataResponse:
    """
    【全部集合列表接口】

    功能:分页返回数据表内所有用户的集合（按 updated_at 倒序）

    权限:任何登录用户

    注意:
    - 与 list_collections_api 的区别：不做 owner 过滤，返回全量集合（可按 owner_username 筛）
    - 筛选条件走 body（CollectionFilter，不筛选也要发 {}），page/size 走 query
    - 每个筛选字段可传单值或数组，同字段多值 OR、不同字段之间 AND
    - 分页参数 page≥1、1≤size≤100，越界由 FastAPI 返回 422
    - 响应结构与 list_collections_api 一致（meta + data，含自增 id）
    """
    collections, total_records = await list_all_collections(async_session, filters, page, size)
    total_pages = (total_records + size - 1) // size
    pagination_meta = PaginationMeta(
        current_page=page,
        current_records=len(collections),
        total_pages=total_pages,
        total_records=total_records,
    )
    logger.info(
        f"list all collections user={current_user.id} page={page} size={size} total={total_records}"
    )
    return ListDataResponse(meta=pagination_meta, data=collections)


async def get_collection_api(
    public_id: PublicIdPath,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> CollectionDetail:
    """
    【集合详情接口】

    功能:集合元数据 + 成员文件列表（按 position 排序）+ 实时聚合统计

    权限:集合 owner 或 admin（两者权限相同）——免登录浏览走公开端 public_id 接口
    """
    collection_id = await get_collection_id_by_public_id(async_session, public_id)
    collection = await get_owned_collection(async_session, collection_id, current_user)

    detail = await build_collection_detail(async_session, collection)
    logger.info(f"get collection user={current_user.id} collection_id={collection.id}")
    return detail


async def patch_collection_api(
    public_id: PublicIdPath,
    patch: CollectionPatch,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> CollectionDetail:
    """
    【修改集合元数据接口】

    功能:部分更新元数据（只改请求中出现的字段）；文件成员不在 PATCH 中修改

    权限:集合 owner 或 admin（两者权限相同）
    """
    collection_id = await get_collection_id_by_public_id(async_session, public_id)
    collection = await get_owned_collection(async_session, collection_id, current_user)

    await update_collection(async_session, collection, patch)
    await async_session.commit()
    await async_session.refresh(collection)

    detail = await build_collection_detail(async_session, collection)
    logger.info(f"patch collection user={current_user.id} collection_id={collection.id}")
    return detail


async def delete_collection_api(
    public_id: PublicIdPath,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> dict:
    """
    【删除集合接口】

    功能:删除集合——只删 DataCollection + CollectionMember（外键 CASCADE），不动任何文件

    权限:集合 owner 或 admin（两者权限相同）
    """
    collection_id = await get_collection_id_by_public_id(async_session, public_id)
    collection = await get_owned_collection(async_session, collection_id, current_user)

    await delete_collection(async_session, collection)
    await async_session.commit()
    await async_session.refresh(current_user)

    logger.info(f"delete collection user={current_user.id} collection_id={collection.id}")
    return {"public_id": public_id, "deleted": True}


async def add_members_api(
    public_id: PublicIdPath,
    operation: CollectionMembersOperation,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> CollectionDetail:
    """
    【批量添加成员接口】

    功能:往集合追加成员，新成员固定追加到末尾（MAX(position)+1）

    权限:集合 owner 或 admin（两者权限相同）

    幂等:已在集合内的文件跳过、只加新文件，不报错
    """
    collection_id = await get_collection_id_by_public_id(async_session, public_id)
    collection = await get_owned_collection(async_session, collection_id, current_user)

    member_file_ids = await resolve_member_file_ids(async_session, operation.file_public_ids)
    await add_members(async_session, collection, current_user, member_file_ids)
    await async_session.commit()
    await async_session.refresh(collection)

    detail = await build_collection_detail(async_session, collection)
    logger.info(f"add collection members user={current_user.id} collection_id={collection.id}")
    return detail


async def remove_members_api(
    public_id: PublicIdPath,
    operation: CollectionMembersOperation,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> dict:
    """
    【批量移除成员接口】

    功能:批量移除集合成员——只删 CollectionMember 链接行，不动文件（单个移除即传长度为 1 的数组）

    权限:集合 owner 或 admin（两者权限相同）

    幂等:不在集合内的 public_id 跳过、不报错（与批量添加的幂等语义对称），
    响应返回 removed / skipped 两个 public_id 列表供前端对账
    """
    collection_id = await get_collection_id_by_public_id(async_session, public_id)
    collection = await get_owned_collection(async_session, collection_id, current_user)

    # API 层换算 public_id → file_id；查不到的 public_id（文件不存在）与"不在集合内"
    # 同口径记 skipped。service 以 file_id 集合操作，响应再映射回 public_id
    unique_public_ids = list(dict.fromkeys(operation.file_public_ids))
    id_map = await get_file_ids_by_public_ids(async_session, unique_public_ids)
    public_by_file_id = {file_id: pub for pub, file_id in id_map.items()}

    removed_fids, _ = await remove_members(
        async_session, collection, [id_map[p] for p in unique_public_ids if p in id_map]
    )
    removed = [public_by_file_id[fid] for fid in removed_fids]
    removed_set = set(removed)
    skipped = [p for p in unique_public_ids if p not in removed_set]
    await async_session.commit()
    await async_session.refresh(current_user)
    await async_session.refresh(collection)

    logger.info(
        f"remove collection members user={current_user.id} collection_id={collection.id} "
        f"removed={len(removed)} skipped={len(skipped)}"
    )
    return {"public_id": public_id, "removed": removed, "skipped": skipped}


async def reorder_members_api(
    public_id: PublicIdPath,
    operation: CollectionMembersOperation,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> CollectionDetail:
    """
    【调整成员顺序接口】

    功能:传全量文件 public_id 数组，按数组顺序重写 position = 1..n

    权限:集合 owner 或 admin（两者权限相同）

    校验:数组必须恰好等于集合当前成员全集（缺失/多出/重复 → 409，
    前端刷新成员列表后重试——乐观并发控制）
    """
    collection_id = await get_collection_id_by_public_id(async_session, public_id)
    collection = await get_owned_collection(async_session, collection_id, current_user)

    # API 层换算 public_id → file_id（保留重复：service 对重复 409）；
    # 查不到的 public_id 使请求集合必然 ≠ 成员全集，同口径 409 不静默跳过
    id_map = await get_file_ids_by_public_ids(async_session, operation.file_public_ids)
    missing = [p for p in dict.fromkeys(operation.file_public_ids) if p not in id_map]
    if missing:
        logger.warning(
            "reorder failed, unknown public ids collection_id=%s missing=%s",
            collection.id, missing,
        )
        raise member_order_conflict_exception

    await reorder_members(
        async_session, collection, [id_map[p] for p in operation.file_public_ids]
    )
    await async_session.commit()
    await async_session.refresh(collection)

    detail = await build_collection_detail(async_session, collection)
    logger.info(f"reorder collection members user={current_user.id} collection_id={collection.id}")
    return detail


#########################
# 公开端（免登录）
#########################
async def get_public_collection_api(
    public_id: PublicIdPath,
    async_session: AsyncSessionDep,
) -> CollectionDetail:
    """
    【公开页集合详情接口（免登录）】

    功能:凭 URL 中的 public_id 返回集合详情 + 成员文件列表（按 position 排序）

    安全:
    - 非公开集合即使 public_id 正确也返回 404（不暴露存在性）
    - 成员仅返回 status == completed 的文件（与 owner 视角同一口径）
    - 响应不包含集合数字 id：禁止暴露自增 id，对外标识一律用 public_id
      （管理端操作要求 owner/admin 且走登录态接口，owner 从自己的集合列表拿 public_id，
      非 owner 拿到 public_id 也无权调用管理端，故无暴露必要）
    """
    collection = await get_public_collection_by_public_id(async_session, public_id)
    detail = await build_collection_detail(async_session, collection)
    logger.info(f"get public collection public_id={public_id}")
    return detail


#########################
# 依赖注入别名（供 routers 使用）
#########################
CreateCollectionDep = Annotated[CollectionDetail, Depends(create_collection_api)]
ListCollectionsDep = Annotated[ListDataResponse, Depends(list_collections_api)]
ListAllCollectionsDep = Annotated[ListDataResponse, Depends(list_all_collections_api)]
GetCollectionDep = Annotated[CollectionDetail, Depends(get_collection_api)]
PatchCollectionDep = Annotated[CollectionDetail, Depends(patch_collection_api)]
DeleteCollectionDep = Annotated[dict, Depends(delete_collection_api)]
AddMembersDep = Annotated[CollectionDetail, Depends(add_members_api)]
RemoveMembersDep = Annotated[dict, Depends(remove_members_api)]
ReorderMembersDep = Annotated[CollectionDetail, Depends(reorder_members_api)]
GetPublicCollectionDep = Annotated[CollectionDetail, Depends(get_public_collection_api)]
