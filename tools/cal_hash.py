
# file_hash.py
"""
计算文件SHA256哈希值的异步模块
"""

import argparse
import hashlib
from pathlib import Path
import aiofiles
import asyncio
import zipfile


CHUNK_SIZE = 1024 * 1024  # 1MB分片大小

async def compute_imzml_md5(file_path: str | Path) -> tuple[str, int]:
    """
    异步计算.imzML文件的MD5哈希值和大小
    计算方式：分别分块读取.imzML文件和.ibd文件，计算整体哈希值和大小

    参数:
        file_path: imzML实验ZIP文件路径

    返回:
        (md5哈希值, 文件大小)
    """
    def _extract_file(file_path: str | Path, extract_tmp_path: Path) -> tuple[Path|None, Path|None]:
        imzml_path = None
        ibd_path = None
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            for filename in zip_ref.namelist():
                if filename.endswith(".imzML"):
                    imzml_path = Path(filename)
                if filename.endswith(".ibd"):
                    ibd_path = Path(filename)
            zip_ref.extractall(extract_tmp_path)
            return imzml_path, ibd_path

    file_verify_code = hashlib.md5()
    imzml_path = None
    ibd_path = None
    imzml_size, ibd_size = 0, 0

    extract_tmp_path = Path(file_path).parent / "extract_tmp"

    _loop = asyncio.get_running_loop()
    imzml_path, ibd_path = await _loop.run_in_executor(None, _extract_file, file_path, extract_tmp_path)

    if not imzml_path or not ibd_path:
        raise FileNotFoundError("ZIP file must contain both .imzML and .ibd files")

    async with aiofiles.open(extract_tmp_path / imzml_path, mode='rb') as imzml_file:
        await imzml_file.seek(0)
        while True:
            chunk = await imzml_file.read(CHUNK_SIZE)
            if not chunk:
                break
            file_verify_code.update(chunk)
            imzml_size += len(chunk)

    async with aiofiles.open(extract_tmp_path / ibd_path, mode='rb') as ibd_file:
        await ibd_file.seek(0)
        while True:
            chunk = await ibd_file.read(CHUNK_SIZE)
            if not chunk:
                break
            file_verify_code.update(chunk)
            ibd_size += len(chunk)

    return file_verify_code.hexdigest(), imzml_size + ibd_size

async def compute_file_md5(file_path: str | Path) -> tuple[str, int]:
    """
    异步计算文件的MD5哈希值和大小
s
    参数:
        file_path: 文件路径

    返回:
        (md5哈希值, 文件大小)
    """
    md5 = hashlib.md5()
    size = 0

    if isinstance(file_path, Path):
        file_path = str(file_path.resolve())

    # 使用aiofiles异步打开文件
    async with aiofiles.open(file_path, 'rb') as file:
        while True:
            # 分片读取文件内容
            chunk = await file.read(CHUNK_SIZE)
            if not chunk:  # 读取完毕
                break

            # 更新哈希值
            md5.update(chunk)
            size += len(chunk)

    return md5.hexdigest(), size


async def file_exists(file_path: str) -> bool:
    """
    异步检查文件是否存在
    
    参数:
        file_path: 文件路径
        
    返回:
        文件是否存在
    """
    return Path(file_path).exists()


# 使用示例
async def main():
    """使用示例"""
    try:
        parser = argparse.ArgumentParser(description="计算文件哈希值")
        parser.add_argument("file_path", type=str, help="要计算哈希值的文件路径")
        args = parser.parse_args()
        hash_value, file_size = await compute_file_md5(args.file_path)
        print(f"文件大小: {file_size} bytes")
        print(f"MD5值: {hash_value}")

        # 检查文件是否存在
        exists = await file_exists(args.file_path)
        print(f"文件存在: {exists}")

    except FileNotFoundError as e:
        print(f"文件不存在: {e}")
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"计算MD5值时出错: {e}")


# 如果需要直接运行测试
if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
