import os
import sys
import hashlib
import math

def calculate_sha256(data):
    sha256_hash = hashlib.sha256()
    sha256_hash.update(data)
    return sha256_hash.hexdigest()

def split_file(file_path, num_chunks):
    if not os.path.exists(file_path):
        print(f"错误: 文件 '{file_path}' 不存在。")
        return

    file_size = os.path.getsize(file_path)
    chunk_size = math.ceil(file_size / num_chunks)
    
    print(f"文件: {file_path}")
    print(f"总大小: {file_size} 字节")
    print(f"分片数量: {num_chunks}")
    print("-" * 60)
    print(f"{'分片文件名':<30} {'大小 (字节)':<15} {'SHA256 哈希'}")
    print("-" * 60)

    base_name = os.path.basename(file_path)
    file_dir = os.path.dirname(file_path)
    name, ext = os.path.splitext(base_name)

    with open(file_path, 'rb') as f:
        for i in range(num_chunks):
            chunk_data = f.read(chunk_size)
            if not chunk_data:
                break
            
            chunk_filename = f"{base_name}.part{i+1}"
            chunk_path = os.path.join(file_dir, chunk_filename)
            
            # 写入分片文件
            with open(chunk_path, 'wb') as chunk_file:
                chunk_file.write(chunk_data)
            
            chunk_hash = calculate_sha256(chunk_data)
            current_chunk_size = len(chunk_data)
            
            print(f"{chunk_filename:<30} {current_chunk_size:<15} {chunk_hash}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("用法: python file_split.py <文件路径> <分片数量>")
    else:
        input_file = sys.argv[1]
        try:
            chunks = int(sys.argv[2])
            if chunks <= 0:
                print("分片数量必须大于 0")
            else:
                split_file(input_file, chunks)
        except ValueError:
            print("分片数量必须是一个整数")