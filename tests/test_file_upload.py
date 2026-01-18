
"""
# To test this module, you should do: 
# 1.Copy .env.pytest.example as .env.pytest, configure your .env.pytest file.
3.Run "uv run main.py" to start FastAPI app.
4.Run "uv run pytest -m upload" in shell with venv activated.
"""
import os
import hashlib
from typing import Tuple
import pytest
import aiofiles
from httpx import AsyncClient
from .conftest import logger
from .conftest import BASE_URL, UPLOAD_FILE_DIR, CHUNK_BUFFER_SIZE


@pytest.fixture(scope="session", name="async_client")
async def _async_client():
    if not BASE_URL:
        pytest.fail("Environment variable BASE_URL is required")
    async with AsyncClient(base_url=BASE_URL) as client:
        yield client

@pytest.fixture(scope="session", name="admin_login")
async def _admin_login(async_client):
    client = async_client
    # ASGI login, and get the access token.
    username = os.getenv("TEST_USER")
    password = os.getenv("TEST_PWD")
    if not (username and password):
        pytest.skip("TEST_USER/TEST_PWD hasn't provided yet.")

    rs = await client.post(
        "/login",
        data={
            "grant_type": "password",
            "username": username,
            "password": password,
        },
    )
    assert rs.status_code == 200, rs.text
    rs.raise_for_status()
    token, token_type = rs.json()['access_token'], rs.json()['token_type']

    # Check if current user is corresponding to your username.
    rs = await client.get(
        "/user",
        headers={"Authorization": f"{token_type.title()} {token}"},
    )
    assert rs.status_code == 200, rs.text
    current_username = rs.json()['username']
    if current_username != username:
        pytest.fail(f"Current user is not the same as target user，"
                    f"username:{username}, current username:{current_username}")
    
    yield client, token, token_type

@pytest.mark.upload
@pytest.mark.asyncio
async def test_upload_file_chunks(login: Tuple[AsyncClient, str, str]):
    """
    test_upload_file_chunks
    """
    client, token, token_type = login # Get the token of current user.
    logger.info("Start testing upload_file_chunks...")
    if not UPLOAD_FILE_DIR:
        pytest.fail("Environment variable UPLOAD_FILE_DIR is required")
    for file_name in os.listdir(UPLOAD_FILE_DIR):
        file_path = os.path.join(UPLOAD_FILE_DIR, file_name)
        logger.info(f"Now testing: file_path:{file_path}")
        async with aiofiles.open(file=file_path, mode='rb') as upload_file:
            total_hash = hashlib.sha256()
            part_hashes = []
            total_parts = 0
            size = 0
            await upload_file.seek(0)
            while True:
                chunk = await upload_file.read(CHUNK_BUFFER_SIZE) # 分片来计算hash值，不分片会消耗很大的内存
                if not chunk:
                    break
                total_parts += 1
                total_hash.update(chunk) # 同步操作 但是轻量级 应该没啥问题
                part_hash = hashlib.sha256() # Get the hash of single chunk
                part_hash.update(chunk)
                part_hashes.append(part_hash.hexdigest())
                size += len(chunk)
            await upload_file.seek(0)

            # Preflight Chunked File
            rs = await client.post(
                "/files/chunked/preflight",
                headers={"Authorization": f"{token_type.title()} {token}"},
                data={
                    "storage_type": "local",
                    "filename": file_name,
                    "size": size,
                    "hash_sha256": total_hash.hexdigest(),
                    "total_parts": total_parts
                }
            )
            assert rs.status_code == 200, rs.text
            rs.raise_for_status()

            # Upload File Chunks
            await upload_file.seek(0)
            part_number = 1
            while True:
                chunk = await upload_file.read(CHUNK_BUFFER_SIZE)
                if not chunk:
                    break
                part_hash = part_hashes[part_number-1]
                chunkfile = {
                    "file_hash": part_hash,
                    "part_number": str(part_number), 
                    "part_hash": part_hash,
                    "chunk": (None, chunk, "application/octet-stream") 
                }
                rs = await client.post(
                    "/files/chunked/upload",
                    headers = {"Authorization": f"{token_type.title()} {token}"},
                    files = chunkfile
                )
                assert rs.status_code == 200, rs.text
                rs.raise_for_status()
                part_number += 1
            await upload_file.seek(0)

        logger.info(f"Testing pass: file_path:{file_path}, sha256:{total_hash.hexdigest()}")

        