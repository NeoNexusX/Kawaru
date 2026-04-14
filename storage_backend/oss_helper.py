import os
import asyncio
from datetime import datetime, timedelta
from aliyun.log import LogClient, GetLogsResponse
from aliyun.log.queriedlog import QueriedLog
from aliyun.log.auth import AUTH_VERSION_4

from constants.file_constants import OSS_REGION_ID, FC_LOG_PROJECT_NAME, FC_LOGSTORE_NAME

from tools.logger import get_logger

logger = get_logger("storage.oss_helper")


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


async def get_fc_logs_instance_id_by_request_id(request_id: str) -> str | None:
    """
    根据request_id获取函数计算日志的instanceID
    """
    # 构建查询语句，查询指定request_id的日志
    query = f"* and requestId : {request_id}"

    # 执行查询
    logs = await fetch_fc_logs_async(query)
    if logs is None:
        return None

    # 解析查询结果
    for log in logs:
        content: dict = log.contents
        if not "instanceID" in content:
            continue
        if content["requestId"] == request_id:
            return content["instanceID"]
        continue

    return None


async def get_fc_invoke_logs_by_instance_id(instance_id: str) -> list[str] | None:
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

    return message_list


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
        res = None

        # 执行查询
        try:
            res = client.get_log_all_v2(
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
