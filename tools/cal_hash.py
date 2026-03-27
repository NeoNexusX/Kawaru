
# file_hash.py
"""
计算文件SHA256哈希值的异步模块
"""

import hashlib
import aiofiles
from pathlib import Path

CHUNK_SIZE = 1024 * 1024  # 1MB分片大小

async def compute_file_sha256(file_path: str) -> tuple[str, int]:
    """
    异步计算文件的SHA256哈希值和大小
    
    参数:
        file_path: 文件路径
        
    返回:
        (sha256哈希值, 文件大小)
    """
    sha256 = hashlib.sha256()
    size = 0
    
    # 使用aiofiles异步打开文件
    async with aiofiles.open(file_path, 'rb') as file:
        while True:
            # 分片读取文件内容
            chunk = await file.read(CHUNK_SIZE)
            if not chunk:  # 读取完毕
                break
            
            # 更新哈希值
            sha256.update(chunk)
            size += len(chunk)
    
    return sha256.hexdigest(), size


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
        # 计算文件哈希值
        hash_value, file_size = await compute_file_sha256("44.ibd")
        print(f"文件大小: {file_size} bytes")
        print(f"SHA256哈希值: {hash_value}")
        
        # 检查文件是否存在
        exists = await file_exists("44.ibd")
        print(f"文件存在: {exists}")
        
    except FileNotFoundError as e:
        print(f"文件不存在: {e}")
    except Exception as e:
        print(f"计算哈希值时出错: {e}")


# 如果需要直接运行测试
if __name__ == "__main__":
    import asyncio
    asyncio.run(main())