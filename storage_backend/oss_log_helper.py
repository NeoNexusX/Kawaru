import os
import asyncio
from datetime import datetime, timedelta
from aliyun.log import LogClient, GetLogsResponse
from aliyun.log.queriedlog import QueriedLog
from aliyun.log.auth import AUTH_VERSION_4

from celery_runtime.state import get_celery_app
from celery_runtime.result import wait_for_result
from constants.file_constants import OSS_REGION_ID, FC_LOG_PROJECT_NAME, FC_LOGSTORE_NAME

from tools.logger import get_logger

logger = get_logger("storage.oss_log_helper")
celery_app = get_celery_app()


# 目前关于日志的api阿里云只提供了同步的接口
# 如需异步调用需要通过asyncio的run_in_executor包装或将父函数放入任务队列

def get_oss_log_client():
    """
    获取OSS日志客户端
    """
    # 从环境变量获取OSS相关配置
    access_key_id = os.getenv("OSS_ACCESS_KEY_ID")
    access_key_secret = os.getenv("OSS_ACCESS_KEY_SECRET")

    # 日志服务的服务接入点
    endpoint = f"{OSS_REGION_ID}.log.aliyuncs.com"

    # 创建 LogClient 实例，使用 V4 签名，根据实际情况填写 region
    assert OSS_REGION_ID is not None # bootstrap流程已检查过
    client = LogClient(endpoint, access_key_id, access_key_secret, auth_version=AUTH_VERSION_4, region=OSS_REGION_ID)

    return client


async def  get_fc_invoke_status(request_id: str, timeout: int = 20) -> bool | None:
    """
    根据request_id获取函数计算成功状态

    :param request_id: 函数调用ID
    :param timeout: 超时时间，默认20秒，大压缩文件可能需要更长的时间

    :return: 返回True表示成功，False表示失败
    """
    res = get_fc_invoke_status_task.delay(request_id) # type: ignore
    is_success: bool | None = await wait_for_result(res, poll_interval=0.1, timeout=timeout) # type: ignore
    if is_success is None:
        logger.error(f"get fc invoke status failed due to server timeout, request_id={request_id}")
        return None
    return is_success


async def get_fc_invoke_instance_id_by_request_id(request_id: str, timeout: int = 5) -> str | None:
    """
    根据request_id获取函数计算的instance_id

    这里超时可以设置大一些，经实验证明，函数计算完成后才会将日志写入日志服务
    如果报错 **get fc invoke instance id failed due to server timeout**，尝试将timeout设高一些
    """
    start_time = datetime.now()
    while True:
        await asyncio.sleep(5) # 提供一些延迟确保函数计算有时间触发
        res = get_fc_invoke_instance_id_by_request_id_task.delay(request_id) # type: ignore
        instance_id: str | None = await wait_for_result(res, poll_interval=0.1, timeout=5) # type: ignore
        if instance_id is not None:
            break
        if (datetime.now() - start_time).seconds > timeout:
            break

    if instance_id is None:
        logger.error(f"get fc invoke instance id failed due to server timeout, request_id={request_id}")
        return None

    return instance_id


async def get_fc_invoke_logs_by_request_id(request_id: str) -> list[str] | None:
    """
    根据request_id获取函数计算日志
    这里超时可以不用设太高，等待调用get_fc_invoke_status之后再获取日志，确保函数计算完整日志写入日志服务
    """
    instance_id = await get_fc_invoke_instance_id_by_request_id(request_id)
    res = get_fc_invoke_logs_by_instance_id_task.delay(instance_id) # type: ignore
    logs: list[str] | None = await wait_for_result(res, poll_interval=0.1, timeout=10) # type: ignore
    if logs is None:
        logger.error(f"get fc invoke logs failed due to server timeout, instance_id={instance_id}")
        return None

    # 过滤出包含request_id的日志
    logs = [log for log in logs if request_id in log]

    return logs


@celery_app.task()
async def get_fc_invoke_logs_by_request_id_in_background(request_id: str) -> None:
    """
    根据request_id获取函数计算日志，后台执行
    """
    await asyncio.sleep(0.5) # 等待0.5秒确保确保函数计算有时间触发
    logs = await get_fc_invoke_logs_by_request_id(request_id)
    if logs is None:
        logger.error(f"get fc invoke logs failed due to server timeout, request_id={request_id}")
        return
    # 打印日志
    for log in logs:
        logger.info(f"fc invoke log: {log}")


@celery_app.task()
async def get_fc_invoke_instance_id_by_request_id_task(request_id: str) -> str | None:
    """
    根据request_id获取函数计算日志的instanceID
    """
    # 构建查询语句，查询指定request_id的日志
    query = f"* | where requestId LIKE '{request_id}%'"

    # 执行查询
    logs = await fetch_fc_logs_async(query)
    if logs is None:
        return None

    # 解析查询结果
    for log in logs:
        content: dict = log.contents
        if "instanceID" not in content:
            continue
        if content["requestId"] == request_id:
            return content["instanceID"]
        continue

    return None


@celery_app.task()
async def get_fc_invoke_logs_by_instance_id_task(instance_id: str) -> list[str] | None:
    """
    根据instanceID获取函数计算日志
    """
    # 构建查询语句，查询指定instance_id且包含message字段的日志
    query = f"* and instanceID : {instance_id} and message:*"

    # 执行查询
    logs = await fetch_fc_logs_async(query)
    if logs is None:
        return None

    # 解析查询结果
    message_list: list[str] = []
    for log in logs:
        content: dict = log.contents
        message_list.append(content["message"])

    message_list.reverse()

    return message_list


@celery_app.task()
async def get_fc_invoke_status_task(request_id: str) -> bool | None:
    """
    根据request_id获取函数计算成功状态
    返回True表示成功，False表示失败
    """
    # 构建查询语句，查询指定request_id且包含message字段的日志
    query = f"* and requestId : {request_id} and hasFunctionError:*"

    # 执行查询
    logs = await fetch_fc_logs_async(query)
    if logs is None:
        return None

    # 解析查询结果
    for log in logs:
        content: dict = log.contents
        if content["hasFunctionError"] == "true":
            return False
        if content["hasFunctionError"] == "false":
            return True

    return None


async def fetch_fc_logs_async(query: str) -> list[QueriedLog] | None:
    """
    异步查询函数计算日志
    """
    def _fetch_fc_logs(query: str) -> list[QueriedLog] | None:
        """
        执行函数计算日志查询
        """
        # 获取oss日志客户端
        client = get_oss_log_client()
        res = None # type: ignore

        # 执行查询
        try:
            res = client.get_log_all_v2( # type: ignore
                project = FC_LOG_PROJECT_NAME,
                logstore = FC_LOGSTORE_NAME,
                from_time = ( datetime.now() - timedelta(days=1)).timestamp(), # 查询最近一天的日志
                to_time = datetime.now().timestamp(),
                reverse = True,
                query = query,
            )
            res: GetLogsResponse = next(res) # type: ignore
            # 解析查询结果
            logs: list[QueriedLog] = res.get_log_list()
            return logs
        except Exception as e: # pylint:disable = broad-exception-caught
            logger.error(f"cannot fetch fc logs: {e}")
            return None
    _loop = asyncio.get_event_loop()
    return await _loop.run_in_executor(None, _fetch_fc_logs, query)
