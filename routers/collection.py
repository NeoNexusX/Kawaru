"""
文件集合（Collection）管理路由
"""
from fastapi import APIRouter

from models.collection_models import (
    CollectionDetail,
    CollectionOwnedDetail,
)
from models.filter_models import ListDataResponse
from api.collection_api import (
    CreateCollectionDep,
    ListCollectionsDep,
    ListAllCollectionsDep,
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

@collection_router.post("", response_model=CollectionOwnedDetail)
async def create_collection(detail: CreateCollectionDep):
    """创建集合（可带初始成员 file_public_ids，数组顺序即初始顺序）"""
    return detail


@collection_router.post("/list", response_model=ListDataResponse)
async def list_collections(collections: ListCollectionsDep):
    """当前用户自己的集合列表（按 updated_at 倒序）

    - 筛选条件走 body（CollectionFilter，不筛选也要发 {}），page/size 走 query
    - 每个筛选字段可传单值或数组；同字段多值 OR、不同字段之间 AND
    """
    return collections


@collection_router.post("/list_all", response_model=ListDataResponse)
async def list_all_collections(collections: ListAllCollectionsDep):
    """全部集合列表（任何登录用户；按 updated_at 倒序）

    - 与 /list 的区别：不做 owner 过滤，返回全量集合（可按 owner_username 筛）
    - 筛选条件走 body（CollectionFilter，不筛选也要发 {}），page/size 走 query
    """
    return collections


@collection_router.get("/{collection_id}", response_model=CollectionOwnedDetail)
async def get_collection(detail: GetCollectionDep):
    """集合详情 + 成员列表（owner 或 admin，按 position 排序）"""
    return detail


@collection_router.patch("/{collection_id}", response_model=CollectionOwnedDetail)
async def patch_collection(detail: PatchCollectionDep):
    """修改集合元数据（owner 或 admin；部分更新）"""
    return detail


@collection_router.delete("/{collection_id}")
async def delete_collection(result: DeleteCollectionDep):
    """删除集合（owner 或 admin；只删集合与成员链接，不动文件）"""
    return result


@collection_router.post("/{collection_id}/members", response_model=CollectionOwnedDetail)
async def add_collection_members(detail: AddMembersDep):
    """批量添加成员（owner 或 admin；幂等跳过已在集合内的文件）"""
    return detail


@collection_router.patch("/{collection_id}/members/order", response_model=CollectionOwnedDetail)
async def reorder_collection_members(detail: ReorderMembersDep):
    """调整成员顺序（owner 或 admin；全量文件 public_id 数组重写 position）"""
    return detail


@collection_router.delete("/{collection_id}/members")
async def remove_collection_members(result: RemoveMembersDep):
    """批量移除成员（owner 或 admin；单个移除即传长度为 1 的数组；不在集合内的 public_id 跳过不报错）"""
    return result
