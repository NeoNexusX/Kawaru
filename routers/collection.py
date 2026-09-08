"""
文件集合（Collection）管理路由
"""
from fastapi import APIRouter

from models.collection_models import CollectionDetail, CollectionPublic
from api.collection_api import (
    CreateCollectionDep,
    ListCollectionsDep,
    GetCollectionDep,
    PatchCollectionDep,
    DeleteCollectionDep,
    AddMembersDep,
    RemoveMembersDep,
    ReorderMembersDep,
    GetPublicCollectionDep,
)

collection_router = APIRouter(prefix="/collections", tags=["collection manager"])

#########################
# 公开端（免登录）——先于 /{collection_id} 声明，避免路径被动态段吞掉 #TODO 这里考虑是否需要登陆
#########################

@collection_router.get("/public/{public_id}", response_model=CollectionDetail)
async def get_public_collection(detail: GetPublicCollectionDep):
    """公开页集合详情（免登录，凭 public_id；非公开集合返回 404）"""
    return detail


#########################
# 管理端（需登录）
#########################

@collection_router.post("", response_model=CollectionDetail)
async def create_collection(detail: CreateCollectionDep):
    """创建集合（可带初始成员 file_ids，数组顺序即初始顺序）"""
    return detail


@collection_router.get("", response_model=list[CollectionPublic])
async def list_collections(collections: ListCollectionsDep):
    """当前用户自己的集合列表（按 updated_at 倒序）"""
    return collections


@collection_router.get("/{collection_id}", response_model=CollectionDetail)
async def get_collection(detail: GetCollectionDep):
    """集合详情 + 成员列表（owner 或 admin，按 position 排序）"""
    return detail


@collection_router.patch("/{collection_id}", response_model=CollectionDetail)
async def patch_collection(detail: PatchCollectionDep):
    """修改集合元数据（owner 或 admin；部分更新）"""
    return detail


@collection_router.delete("/{collection_id}")
async def delete_collection(result: DeleteCollectionDep):
    """删除集合（owner 或 admin；只删集合与成员链接，不动文件）"""
    return result


@collection_router.post("/{collection_id}/members", response_model=CollectionDetail)
async def add_collection_members(detail: AddMembersDep):
    """批量添加成员（owner 或 admin；幂等跳过已在集合内的文件）"""
    return detail


@collection_router.patch("/{collection_id}/members/order", response_model=CollectionDetail)
async def reorder_collection_members(detail: ReorderMembersDep):
    """调整成员顺序（owner 或 admin；全量 [file_id] 数组重写 position）"""
    return detail


@collection_router.delete("/{collection_id}/members")
async def remove_collection_members(result: RemoveMembersDep):
    """批量移除成员（owner 或 admin；单个移除即传长度为 1 的数组；不在集合内的 file_id 跳过不报错）"""
    return result
