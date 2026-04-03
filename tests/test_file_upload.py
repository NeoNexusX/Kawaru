
"""
# To test this module, you should do: 
# 1.Copy .env.pytest.example as .env.pytest, configure your .env.pytest file.
3.Run "uv run main.py" to start FastAPI app.
4.Run "uv run pytest -m upload" in shell with venv activated.
"""
import os
import hashlib
import json
from typing import Tuple
from io import BytesIO
from pathlib import Path
import pytest
import aiofiles
from httpx import AsyncClient
from .conftest import logger
from .conftest import UPLOAD_FILE_DIR, CHUNK_BUFFER_SIZE


class TestFileUpload:
    """Tests for file upload relevant functions."""
    @pytest.mark.upload
    @pytest.mark.asyncio
    async def test_upload_file_chunks(self, admin_login: Tuple[AsyncClient, str, str]):
        """
        test_upload_file_chunks
        """
        client, token, token_type = admin_login # Get the token of current user.
        logger.info("Start testing upload_file_chunks...")
        if not UPLOAD_FILE_DIR:
            pytest.fail("Environment variable UPLOAD_FILE_DIR is required")

        # pursue test for each file in UPLOAD_FILE_DIR
        for file_name in os.listdir(UPLOAD_FILE_DIR):
            file_path = os.path.join(UPLOAD_FILE_DIR, file_name)
            logger.info(f"Now testing: file_path:{file_path}")

            # open file asynchronously
            async with aiofiles.open(file=file_path, mode='rb') as upload_file:
                file_type = Path(file_path).suffix.lstrip('.')
                total_hash = hashlib.md5()
                part_hashes = []
                total_parts = 0
                size = 0
                await upload_file.seek(0)

                # calculate the hashes of all the chunks of current file
                # simultaneously calculate the total hash of current file
                while True:
                    chunk = await upload_file.read(CHUNK_BUFFER_SIZE) # 分片来计算hash值，不分片会消耗很大的内存
                    if not chunk:
                        break
                    total_parts += 1
                    total_hash.update(chunk)
                    part_hash = hashlib.md5() # get the hash of single chunk
                    part_hash.update(chunk)
                    part_hashes.append(part_hash.hexdigest())
                    size += len(chunk)
                await upload_file.seek(0)
                total_hash = total_hash.hexdigest() # uniformly transform total hash to hex as the part hashes be

                # Preflight Chunked File
                json_data = {
                        "storage_type": "local",
                        "filename": file_name,
                        "file_type": file_type,
                        "size": size,
                        "hash_md5": total_hash,
                        "total_parts": total_parts,
                        "experiment_type": "imzML",
                        "organism": "Citrus sinensis",
                        "organism_part": "ovary",
                        "condition": "normal condition",
                        "sample_growth_conditions": "sgc1",
                        "sample_stabilization": "ss1",
                        "tissue_modification": "tm1",
                        "maldi_matrix": "mm1",
                        "maldi_matrix_application": "mma1",
                        "solvent": "",
                }

                rs = await client.post(
                    "/files/preflight",
                    headers={"Authorization": f"{token_type.title()} {token}"},
                    json=json_data
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
                    logger.info(f"- uploading chunk part_num:{part_number}, part_hash:{part_hash}")

                    part_data = {
                        "file_hash": total_hash,
                        "part_number": str(part_number), 
                        "part_hash": part_hash,
                        "part_size": len(chunk),
                    }

                    chunk_file = {
                        "chunk": (
                            "chunk.bin",
                            BytesIO(chunk),
                            "application/octet-stream"
                        )
                    }

                    form_data = {
                        "part_data_json": json.dumps(part_data),
                    }

                    rs = await client.post(
                        "/files/chunked/upload",
                        headers = {"Authorization": f"{token_type.title()} {token}"},
                        data = form_data,
                        files = chunk_file
                    )
                    assert rs.status_code == 200, rs.text
                    rs.raise_for_status()
                    logger.info(f"- uploading chunk part_num:{part_number} completed.\n")
                    part_number += 1
                await upload_file.seek(0)

            logger.info(f"Testing pass: file_path:{file_path}, sha256:{total_hash}")
