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
    get_owned_collection,
    get_public_collection_by_public_id,
    list_user_collections,
    remove_members,
    reorder_members,
    update_collection,
)
from database.db import AsyncSessionDep
from models.filter_models import ListDataResponse, PaginationMeta
from models.collection_models import (
    CollectionCreate,
    CollectionDetail,
    CollectionMembersOperation,
    CollectionOwnedDetail,
    CollectionPatch,
    CollectionPublic,
)
from models.user_models import UserInDB

from tools import get_logger
logger = get_logger("api.collection_api")


#########################
# 管理端（需登录）
#########################
async def create_collection_api(
    data: CollectionCreate,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> CollectionOwnedDetail:
    """
    【创建集合接口】

    功能:创建集合并写入元数据与初始成员（file_ids 数组顺序即初始 position 1..n）

    权限:任何登录用户

    流程:
    1. 成员资格校验（存在 → completed → is_public → experiment_type 合法）
    2. 创建集合 + 成员链接行
    """
    collection = await create_collection(async_session, current_user, data)
    await async_session.commit()
    await async_session.refresh(collection)

    detail = await build_collection_detail(async_session, collection)
    logger.info(f"create collection user={current_user.id} collection_id={collection.id}")
    return detail


async def list_collections_api(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
) -> ListDataResponse:
    """
    【集合列表接口】

    功能:分页返回当前用户自己的集合（按 updated_at 倒序）
    用途:集合管理列表与"加入集合"选择器；公开发现页本期不做（靠链接分发）

    注意:
    - 分页参数 page≥1、1≤size≤100，越界由 FastAPI 返回 422
    - 响应结构与 list_user_files / list_files 一致（meta + data）
    """
    collections, total_records = await list_user_collections(
        async_session, current_user, page, size
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


async def get_collection_api(
    collection_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> CollectionOwnedDetail:
    """
    【集合详情接口】

    功能:集合元数据 + 成员文件列表（按 position 排序）+ 实时聚合统计

    权限:集合 owner 或 admin（两者权限相同）——免登录浏览走公开端 public_id 接口
    """
    collection = await get_owned_collection(async_session, collection_id, current_user)

    detail = await build_collection_detail(async_session, collection)
    logger.info(f"get collection user={current_user.id} collection_id={collection_id}")
    return detail


async def patch_collection_api(
    collection_id: int,
    patch: CollectionPatch,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> CollectionOwnedDetail:
    """
    【修改集合元数据接口】

    功能:部分更新元数据（只改请求中出现的字段）；文件成员不在 PATCH 中修改

    权限:集合 owner 或 admin（两者权限相同）
    """
    collection = await get_owned_collection(async_session, collection_id, current_user)

    await update_collection(async_session, collection, patch)
    await async_session.commit()
    await async_session.refresh(collection)

    detail = await build_collection_detail(async_session, collection)
    logger.info(f"patch collection user={current_user.id} collection_id={collection_id}")
    return detail


async def delete_collection_api(
    collection_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> dict:
    """
    【删除集合接口】

    功能:删除集合——只删 DataCollection + CollectionMember（外键 CASCADE），不动任何文件

    权限:集合 owner 或 admin（两者权限相同）
    """
    collection = await get_owned_collection(async_session, collection_id, current_user)

    await delete_collection(async_session, collection)
    await async_session.commit()
    await async_session.refresh(current_user)

    logger.info(f"delete collection user={current_user.id} collection_id={collection_id}")
    return {"collection_id": collection_id, "deleted": True}


async def add_members_api(
    collection_id: int,
    operation: CollectionMembersOperation,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> CollectionOwnedDetail:
    """
    【批量添加成员接口】

    功能:往集合追加成员，新成员固定追加到末尾（MAX(position)+1）

    权限:集合 owner 或 admin（两者权限相同）

    幂等:已在集合内的文件跳过、只加新文件，不报错
    """
    collection = await get_owned_collection(async_session, collection_id, current_user)

    await add_members(async_session, collection, current_user, operation.file_ids)
    await async_session.commit()
    await async_session.refresh(collection)

    detail = await build_collection_detail(async_session, collection)
    logger.info(f"add collection members user={current_user.id} collection_id={collection_id}")
    return detail


async def remove_members_api(
    collection_id: int,
    operation: CollectionMembersOperation,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> dict:
    """
    【批量移除成员接口】

    功能:批量移除集合成员——只删 CollectionMember 链接行，不动文件（单个移除即传长度为 1 的数组）

    权限:集合 owner 或 admin（两者权限相同）

    幂等:不在集合内的 file_id 跳过、不报错（与批量添加的幂等语义对称），
    响应返回 removed / skipped 两个列表供前端对账
    """
    collection = await get_owned_collection(async_session, collection_id, current_user)

    removed, skipped = await remove_members(async_session, collection, operation.file_ids)
    await async_session.commit()
    await async_session.refresh(current_user)

    logger.info(
        f"remove collection members user={current_user.id} collection_id={collection_id} "
        f"removed={len(removed)} skipped={len(skipped)}"
    )
    return {"collection_id": collection_id, "removed": removed, "skipped": skipped}


async def reorder_members_api(
    collection_id: int,
    operation: CollectionMembersOperation,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> CollectionOwnedDetail:
    """
    【调整成员顺序接口】

    功能:传全量 [file_id] 数组，按数组顺序重写 position = 1..n

    权限:集合 owner 或 admin（两者权限相同）

    校验:数组必须恰好等于集合当前成员全集（缺失/多出/重复 → 409，
    前端刷新成员列表后重试——乐观并发控制）
    """
    collection = await get_owned_collection(async_session, collection_id, current_user)

    await reorder_members(async_session, collection, operation.file_ids)
    await async_session.commit()
    await async_session.refresh(collection)

    detail = await build_collection_detail(async_session, collection)
    logger.info(f"reorder collection members user={current_user.id} collection_id={collection_id}")
    return detail


#########################
# 公开端（免登录）
#########################
async def get_public_collection_api(
    public_id: str,
    async_session: AsyncSessionDep,
) -> CollectionDetail:
    """
    【公开页集合详情接口（免登录）】

    功能:凭 URL 中的 public_id 返回集合详情 + 成员文件列表（按 position 排序）

    安全:
    - 非公开集合即使 public_id 正确也返回 404（不暴露存在性）
    - 成员仅返回 status == completed 的文件
    - 响应不包含集合数字 id：公开页禁止暴露自增 id，对外标识一律用 public_id
      （管理端操作要求 owner/admin 且走登录态接口，owner 从自己的集合列表拿数字 id，
      非 owner 拿到 id 也无权调用，故无暴露必要）
    """
    collection = await get_public_collection_by_public_id(async_session, public_id)
    owned_detail = await build_collection_detail(async_session, collection, only_completed=True)
    # 剥掉内部数字 id 后返回公开详情模型（id 为多余字段，model_validate 默认忽略）
    detail = CollectionDetail.model_validate(owned_detail)
    logger.info(f"get public collection public_id={public_id}")
    return detail


#########################
# 依赖注入别名（供 routers 使用）
#########################
CreateCollectionDep = Annotated[CollectionOwnedDetail, Depends(create_collection_api)]
ListCollectionsDep = Annotated[ListDataResponse, Depends(list_collections_api)]
GetCollectionDep = Annotated[CollectionOwnedDetail, Depends(get_collection_api)]
PatchCollectionDep = Annotated[CollectionOwnedDetail, Depends(patch_collection_api)]
DeleteCollectionDep = Annotated[dict, Depends(delete_collection_api)]
AddMembersDep = Annotated[CollectionOwnedDetail, Depends(add_members_api)]
RemoveMembersDep = Annotated[dict, Depends(remove_members_api)]
ReorderMembersDep = Annotated[CollectionOwnedDetail, Depends(reorder_members_api)]
GetPublicCollectionDep = Annotated[CollectionDetail, Depends(get_public_collection_api)]
