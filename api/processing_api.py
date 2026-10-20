"""
Processing API — API 入口层
"""
import json
from typing import Annotated

from fastapi import Depends, Query
from fastapi.exceptions import HTTPException

from api_services.composite_service import mark_run_for_deletion
from api_services.file_service import get_file_record_by_public_id, get_public_ids_by_file_ids
from api_services.processing_service import (
    assert_zarr_folder_access,
    create_process_run,
    create_raw_convert_run,
    get_origin_for_processing,
    get_process_run_by_id,
    get_user_runs,
    mark_clustering_run,
    rollback_process_run,
    validate_process_request,
    validate_clustering_request,
)
from api_helpers.processing_helper import assert_run_ownership
from api.user_api import token_authen
from database.db import AsyncSessionDep
from database.redis_client import redis_client
from exceptions.experiment_exceptions import invalid_process_request_exception
from exceptions.common_exceptions import internal_server_error_exception
from constants.file_constants import OSS_BUCKET_NAME, OSS_REGION_ID, OSS_DOWNLOAD_STS_TOKEN_EXPIRE_SECONDS, ZARR_STS_CACHE_TTL_SECONDS
from models.file_models import ZarrFolderAccess
from models.user_models import UserInDB
from models.filter_models import ListDataResponse, PaginationMeta, RunFilter
from models.processing_models import (
    ExperimentProcessRunCreate,
    ExperimentProcessRunPublic,
    ExperimentProcessRunRawConvert,
    ProcessingStatus,
)
from storage.oss.oss_storage import OssStorage

from background.processing_background import trigger_fc_algorithm_task, trigger_fc_raw_convert_task, trigger_fc_clustering_task
from background.file_background import delete_objects_task
from tools.logger import get_logger

logger = get_logger("api.processing_api")


async def create_algorithm_task(
    body: ExperimentProcessRunCreate,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> ExperimentProcessRunPublic:
    """创建算法处理任务 — 写 DB + 异步触发 FC"""
    run_record = None
    try:
        # 1. 纯校验（配额、算法配置）
        if not await validate_process_request(current_user, body.algorithms):
            raise invalid_process_request_exception

        # 2. API 层换算 public_id → file_id（缺失/删除流程 404 口径不变），
        #    再查文件归属 + 就绪 origin 和元数据
        file_record = await get_file_record_by_public_id(body.file_public_id, async_session)
        # commit 后 ORM 对象属性过期（expire_on_commit），异步会话下不能再惰性加载，先取出响应所需字段
        filename, file_public_id = file_record.filename, file_record.public_id
        file_id = file_record.file_id
        assert file_id is not None
        origin, _, metadata, _ = await get_origin_for_processing(
            async_session, file_id, current_user, allow_public=True,
        )

        assert current_user.id is not None
        run_record = await create_process_run(
            async_session, origin, body.algorithms, current_user.id,
        )

        # 构造 OSS 输出路径
        oss_output_path = OssStorage.prepare_run_output_path(run_record.id)  # type: ignore

        # 先提交数据库，让 run_id 生效
        await async_session.commit()
        await async_session.refresh(run_record)
        await async_session.refresh(origin)

        # 异步触发 FC 函数执行算法处理
        trigger_fc_algorithm_task.delay( # type: ignore
            run_id=run_record.id,
            imzml_path=origin.imzml_path,
            config_json=run_record.params_json,
            output_path=oss_output_path,
            metadata=metadata,
        )

        return ExperimentProcessRunPublic(
            **run_record.model_dump(), filename=filename, source_file_public_id=file_public_id,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("create algorithm task failed user=%s file_public_id=%s", current_user.id, body.file_public_id)
        await rollback_process_run(async_session, run_record)
        raise internal_server_error_exception from e


async def create_raw_convert_task(
    body: ExperimentProcessRunRawConvert,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> ExperimentProcessRunPublic:
    """创建 raw_convert 直转任务 — 写 DB + 异步触发 FC fc-raw-convert"""
    run_record = None
    try:
        # 1. API 层换算 public_id → file_id（缺失/删除流程 404 口径不变），
        #    再查文件归属 + 就绪 origin 和元数据
        #    allow_public=True：公开文件允许任何登录用户直转
        file_record = await get_file_record_by_public_id(body.file_public_id, async_session)
        # commit 后 ORM 对象属性过期（expire_on_commit），异步会话下不能再惰性加载，先取出所需字段
        filename, file_public_id = file_record.filename, file_record.public_id
        file_id, default_run_id = file_record.file_id, file_record.default_run_id
        assert file_id is not None
        origin, _, metadata, _ = await get_origin_for_processing(
            async_session, file_id, current_user,
            allow_public=True,
        )

        assert current_user.id is not None

        # 2. 已有完成的直转 Zarr，直接复用（default_run_id 由算法回调写入）
        if default_run_id is not None:
            run_record = await get_process_run_by_id(async_session, default_run_id)
            if run_record:
                return ExperimentProcessRunPublic(
                    **run_record.model_dump(), filename=filename,
                    source_file_public_id=file_public_id,
                )

        # 3. 创建 raw_convert run（含 PROCESSING 防重复）
        run_record, is_existing = await create_raw_convert_run(
            file_id=file_id,
            user_id=current_user.id,
            experiment_id=origin.id,  # type: ignore
            async_session=async_session,
        )

        # 若返回的是已有 processing 中的 run，直接返回
        if is_existing:
            await async_session.commit()
            await async_session.refresh(run_record)
            return ExperimentProcessRunPublic(
                **run_record.model_dump(), filename=filename,
                source_file_public_id=file_public_id,
            )

        # 构造 OSS 输出路径
        oss_output_path = OssStorage.prepare_run_output_path(run_record.id)  # type: ignore

        # 提交数据库，让 run_id 生效
        await async_session.commit()
        await async_session.refresh(run_record)
        await async_session.refresh(origin)

        # 3. 异步触发 FC raw_convert 函数
        trigger_fc_raw_convert_task.delay(  # type: ignore
            run_id=run_record.id,
            imzml_path=origin.imzml_path,
            ibd_path=origin.ibd_path,
            output_path=oss_output_path,
            metadata=metadata,
        )

        return ExperimentProcessRunPublic(
            **run_record.model_dump(), filename=filename,
            source_file_public_id=file_public_id,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("create raw_convert task failed user=%s file_public_id=%s",
                         current_user.id, body.file_public_id)
        await rollback_process_run(async_session, run_record)
        raise internal_server_error_exception from e


async def list_my_process_runs(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
    filters: RunFilter,
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
) -> ListDataResponse:
    """分页查询当前用户的所有运行记录（RunFilter 走 body，支持文件名/算法参数模糊搜索）"""
    try:
        assert current_user.id is not None
        runs, total_records = await get_user_runs(async_session, current_user.id, filters, page, size)

        # 响应需要 source_file_public_id：API 层按 source_file_id 批量反查 public_id（service 只认 file_id）
        source_file_ids = [row["source_file_id"] for row in runs]
        public_id_map = await get_public_ids_by_file_ids(async_session, source_file_ids)

        total_pages = (total_records + size - 1) // size if total_records > 0 else 0
        pagination_meta = PaginationMeta(
            current_page=page,
            current_records=len(runs),
            total_pages=total_pages,
            total_records=total_records,
        )
        return ListDataResponse(
            meta=pagination_meta,
            data=[
                ExperimentProcessRunPublic(
                    **row, source_file_public_id=public_id_map.get(row["source_file_id"], ""),
                )
                for row in runs
            ],
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("failed to list_my_process_runs: %s", e)
        raise internal_server_error_exception from e


async def delete_process_run_endpoint(
    run_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> dict[str, int | str]:
    """根据 run_id 删除算法处理运行记录

    删除状态位流程（OSS 先删、DB 后删、失败标记，方案场景 2）：
    - 登记事务内置 DELETING 后立即返回（不再同步等待 OSS 目录删除）
    - worker 删完 out_path 全部对象后 finalize：删行 + 退 processed 配额 +
      清 default_run_id（同一事务，天然幂等）；任一失败置 DELETE_FAILED，行保留
      等重投（本接口无 admin 旁路，重投仅限该 run 的 owner 本人重新调用）
    """
    # include_deleting=True: 对删除流程中的 run 重复发起删除时走幂等分支而非 404
    run_record = await get_process_run_by_id(async_session, run_id, include_deleting=True)
    if not run_record:
        raise invalid_process_request_exception

    if not assert_run_ownership(run_record.user_id, current_user.id):  # type: ignore
        raise invalid_process_request_exception

    # 登记删除:行锁抢占 → 置 DELETING → 收集路径快照（同一事务）
    snapshot = await mark_run_for_deletion(async_session, run_record)

    # 先 commit（登记对 worker 可见），再投递删除任务；投递失败只留下 DELETING 行
    # （owner 重复调用本接口即可重投，兜底任务上线后自动收敛）。顺序不能反：
    # 先投后 commit 的崩溃会留下「对象已删、行未标记」的不可自愈形态
    await async_session.commit()
    await async_session.refresh(current_user)
    delete_objects_task.delay( # type: ignore
        kind="run",
        resource_id=run_id,
        paths=list(snapshot.paths),
    )

    logger.info("marked run for deletion user=%s run_id=%s", current_user.id, run_id)
    return {"run_id": run_id, "status": "deleted"}


async def get_zarr_folder_access(
    run_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> ZarrFolderAccess:
    """
    【zarr 文件夹访问接口】

    返回文件夹路径和 OSS STS Token，前端凭此通过 OSS SDK 直连读取，
    无需逐个文件签名，支持 Range 请求和 zarr 随机分块读取。

    聚类结果写回算法处理结果 zarr 内部，统一返回 run_record.out_path。
    """
    run_record = await assert_zarr_folder_access(async_session, run_id, current_user)

    target_path = run_record.out_path

    # Redis 缓存：同一 run_id 复用同一个 STS Token
    cache_key = f"zarr_sts:{run_id}"
    cached = await redis_client.get(cache_key)  # type: ignore
    if cached:
        return ZarrFolderAccess(**json.loads(cached))

    expires = OSS_DOWNLOAD_STS_TOKEN_EXPIRE_SECONDS
    sts_token = await OssStorage.get_zarr_read_sts_token(target_path, expires)  # type: ignore
    if sts_token is None:
        raise internal_server_error_exception

    access = ZarrFolderAccess(
        folder_path=target_path,  # type: ignore
        bucket=OSS_BUCKET_NAME,           # type: ignore
        region=OSS_REGION_ID,             # type: ignore
        sts_token=sts_token,
        expires_in=expires,
    )

    await redis_client.set(cache_key, access.model_dump_json(), ex=ZARR_STS_CACHE_TTL_SECONDS)  # type: ignore

    return access


async def create_clustering_task(
    run_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> ExperimentProcessRunPublic:
    """创建聚类分析任务 — 对已完成的算法 run 执行 UMAP/KMeans 聚类，结果写回源 zarr"""
    try:
        run_record = await validate_clustering_request(async_session, run_id, current_user)
        # 响应需要 source_file_public_id：API 层按 source_file_id 反查（service 只认 file_id）
        source_file_public_id = (
            await get_public_ids_by_file_ids(async_session, [run_record.source_file_id])
        ).get(run_record.source_file_id, "")

        # 聚类已完成 / 处理中 / 已失败，直接返回（幂等；failed 不自动重试，
        # 如需重跑由运维将 clustering_status 置空后再触发）
        if run_record.clustering_status in (
            ProcessingStatus.COMPLETED,
            ProcessingStatus.PROCESSING,
            ProcessingStatus.FAILED,
        ):
            return ExperimentProcessRunPublic(
                **run_record.model_dump(), source_file_public_id=source_file_public_id,
            )

        # 标记聚类状态为 processing
        await mark_clustering_run(async_session, run_record, status=ProcessingStatus.PROCESSING)
        await async_session.commit()
        await async_session.refresh(run_record)

        # 异步触发 FC 聚类函数（聚类结果写回 run_record.out_path 内部）
        trigger_fc_clustering_task.delay(  # type: ignore
            run_id=run_id,
            source_zarr_path=run_record.out_path,
        )

        return ExperimentProcessRunPublic(
            **run_record.model_dump(), source_file_public_id=source_file_public_id,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("create clustering task failed run_id=%s", run_id)
        await async_session.rollback()
        raise internal_server_error_exception from e


# 依赖注入类型别名
CreateAlgorithmTaskDep = Annotated[ExperimentProcessRunPublic, Depends(create_algorithm_task)]
CreateRawConvertTaskDep = Annotated[ExperimentProcessRunPublic, Depends(create_raw_convert_task)]
CreateClusteringTaskDep = Annotated[ExperimentProcessRunPublic, Depends(create_clustering_task)]
ListMyRunsDep = Annotated[ListDataResponse, Depends(list_my_process_runs)]
DeleteProcessRunDep = Annotated[dict[str, int | str], Depends(delete_process_run_endpoint)]
ZarrFolderDep = Annotated[ZarrFolderAccess, Depends(get_zarr_folder_access)]
