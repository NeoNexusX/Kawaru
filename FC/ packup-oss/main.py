# -*- coding: utf-8 -*-

import os
import json
import time
import logging
import zipfile
import oss2
import httpx
from pathlib import Path
from oss2.models import PartInfo
from task_queue import TaskQueue
from helper import MemBuffer
from helper import StreamZipFile

from constants import (
    DOWNLOAD_EXPIRE_SECONDS, OSS_MOUNTED_STORAGE_PATH,
    OSS_ZIP_STORAGE_ROOT, OSS_PACKUP_ZIP_STORAGE_ROOT,
)

LOG = logging.getLogger()

# FC 回调验证 Token
FC_CALLBACK_TOKEN = os.environ["CALLBACK_TOKEN"]

# FC handler
# event: the config, see event.json for example
# return: the signed url of the zip file


def main_handler(environ, context):
    if isinstance(environ, bytes):
        environ = json.loads(environ.decode('utf-8'))
    LOG.info('event: %s', environ)
    evt = json.loads(environ['body'])


    region = context.region

    oss_client = get_oss_client(evt, region)
    ret = _main_handler(oss_client, evt)

    response = {
        "statusCode": 200,
        "isBase64Encoded": False,
    }

    return response


def _main_handler(oss_client, evt):
    # source_dir 和 source_files 只能二选一
    source_dir = evt.get('source-dir')
    dest_file = evt.get('dest-file') # 指定压缩后zip文件在oss中的临时存放路径
    filename = evt.get('filename')

    try:
        dest_file = zip_files(oss_client, source_dir, dest_file)
        packup_callback(filename, "completed", dest_file)
    except Exception as e:
        LOG.error(e)
        # 失败时把事件里原始的 dest-file 原样带回：服务端靠它反解 file_id 落 FAILED 状态
        packup_callback(filename, "failed", dest_file)
        raise e


def packup_callback(filename: str, status: str, file_path: str|None) -> None:
    callback_endpoint = os.getenv("PACKUP_CALLBACK_ENDPOINT")
    if not callback_endpoint:
        LOG.error("can not get callback endpoint, PACKUP_CALLBACK_ENDPOINT is not set")
        raise RuntimeError("can not get callback endpoint, PACKUP_CALLBACK_ENDPOINT is not set")
    with httpx.Client(timeout=10) as client:
        response = client.post(
            callback_endpoint,
            params={
                "filename": filename,
                "status": status,
                "zip_path": file_path
            },
            headers={"X-FC-Token": FC_CALLBACK_TOKEN},
        )


def get_oss_client(evt, region):
    bucket = evt['bucket']
    access_key_id = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID')
    access_key_secret = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET')
    security_token = os.getenv('ALIBABA_CLOUD_SECURITY_TOKEN')

    if not all([access_key_id, access_key_secret, security_token]):
        raise Exception('credentials must be specified')
    auth = oss2.StsAuth(access_key_id, access_key_secret, security_token)
    oss_client = oss2.Bucket(auth, 'oss-' + region +
                             '-internal.aliyuncs.com', bucket)
    return oss_client


# add the source files into the zip file
# streaming: the source files are added in stream, no local files are needed
# concurrency: the output zip file is buffer in memory, it buffers at
# most 8MB data and uploads to OSS concurrently by 8 threads


def zip_files(oss_client, source_dir, dest_file):
    LOG.info('create zip, source_dir: %s, dest_file: %s',
             source_dir, dest_file)

    start_time = time.time()

    # 直接写入本地文件
    local_dest_path = Path(OSS_MOUNTED_STORAGE_PATH) / dest_file
    base_path = Path(OSS_MOUNTED_STORAGE_PATH) / source_dir
    LOG.info('zip files, dest_file: %s, base_path: %s', str(dest_file), str(base_path))
    os.makedirs(str(local_dest_path.parent), exist_ok=True)

    with zipfile.ZipFile(str(local_dest_path), 'w', zipfile.ZIP_DEFLATED) as zip_file:
        if not base_path.exists():
            raise Exception(f'source_dir does not exist: {base_path}')
        # 遍历所有文件
        for local_file in base_path.rglob('*'):
            if local_file.is_dir():
                continue
            arcname = local_file.relative_to(base_path)
            zip_file.write(str(local_file), str(arcname))

    end_time = time.time()
    LOG.info('create zip, cost: %s secs', end_time - start_time)

    return dest_file


if __name__ == '__main__':
    pass