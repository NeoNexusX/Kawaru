# 文件管理与分片上传 API 使用说明

> 适用于 FastAPI + SQLModel 实现的文件上传系统，支持 **普通上传 / 分片上传 / 文件复用 / 预检 / 清理机制**。

------

## 1. 设计目标

- 支持小文件一次性上传、大文件分片上传
- 避免重复存储（基于 SHA256 的文件复用）
- 支持多用户共享同一物理文件
- 强一致性：**文件是否完成 = 是否写入 user.file_ids**
- 安全清理：不会误删仍被其他用户引用的文件

------

## 2. 核心设计概念

### 2.1 核心原则

- **所有上传必须先预检（Preflight）**
- **FileInDB 表示“物理文件”**
- **User.file_ids 表示“用户真正拥有的文件”**
- **owners 是弱归属，用于预检 / 复用 / 权限判断**
- **只有上传成功后才写 user.file_ids**

------

## 3. 数据模型说明

### 3.1 FileInDB（文件表）

| 字段           | 说明                   |
| -------------- | ---------------------- |
| file_id        | 文件唯一 ID            |
| filename       | 文件名                 |
| path           | 物理存储路径           |
| hash_sha256    | 文件内容 hash          |
| size           | 文件大小               |
| owners         | 关联的用户 ID 列表     |
| storage_type   | local                  |
| total_parts    | 分片总数（仅分片上传） |
| uploaded_at    | 创建时间               |
| download_count | 下载次数               |
| total_parts    | 分片的片数（默认为零） |

------

### 3.2 UserInDB（用户表）

| 字段            | 说明                       |
| --------------- | -------------------------- |
| username        | 用户名字                   |
| active          | 默认为ture                 |
| identity        | 身份（管理员或者普通用户） |
| id              | 主键                       |
| email           | 邮箱                       |
| hashed_password | hash加密的密码             |
| file_ids        | 用户拥有的文件 ID 列表     |
| file_count      | 文件数量                   |
| total_file_size | 已用存储空间               |
| download_used   | 文件的空间使用量           |

------

### 3.3 PartInDB（分片表）

| 字段            | 说明                  |
| --------------- | --------------------- |
| part_id         | 主键                  |
| file_hash       | 对应文件 hash         |
| part_number     | 分片序号（从 1 开始） |
| part_hash       | 分片 hash             |
| part_size       | 分片大小              |
| temp_path       | 临时分片路径          |
| owner_id        | 上传用户              |
| upload_at       | 上传时间              |
| file_id         | 所属文件的id          |
| total_file_size | 总文件的大小          |
| total_parts     | 文件的总片数          |

------

## 4. 普通文件上传流程（Simple Upload）

### 4.1 流程概览

```
预检 → 校验 → 写文件 → 更新 user.file_ids → 完成
```

------

### 4.2 预检接口

**接口**

```
POST /preflight_create_file
```

**参数**

| 参数         | 说明        |
| ------------ | ----------- |
| filename     | 文件名      |
| hash_sha256  | 文件 SHA256 |
| size         | 文件大小    |
| storage_type | local       |

**行为**

- 校验文件名 / 类型 / 配额
- 检查用户是否已有同名 / 同内容文件
- 如发现已有相同 hash 文件 → 直接复用（写 owners）
- 如果对应hash文件路径存在，会直接**不会写 user.file_ids**，该用户不用再上传
- 否则创建新的 FileInDB 记录
- 路径不存在，**不会写 user.file_ids**

------

### 4.3 上传接口

**接口**

```
POST /upload_simple_file
```

**参数**

| 参数        | 说明               |
| ----------- | ------------------ |
| pre_file_id | 预检返回的 file_id |
| file        | 上传文件           |

**关键校验**

- 文件名 / size / hash 必须与预检一致
- 防止预检与上传间状态变化
- 再次校验用户配额

**成功后行为**

- 写入物理文件
- 更新 user.file_ids
- 更新 file_count / total_file_size
- ![普通文件上传](C:\桌面\普通文件上传.jpg)

------

## 5. 分片上传流程（Chunked Upload）

### 5.1 总体流程

```
分片预检
 ↓
上传分片 (1..N)
 ↓
校验完整性
 ↓
合并分片
 ↓
写 user.file_ids
 ↓
完成
```

------

### 5.2 分片预检接口

**接口**

```
POST /preflight_create_chunked_file
```

**参数**

| 参数        | 说明          |
| ----------- | ------------- |
| filename    | 文件名        |
| hash_sha256 | 文件整体 hash |
| size        | 文件总大小    |
| total_parts | 分片总数      |

**行为**

- 校验 total_parts > 0
- 校验用户配额
- 清理过期预检 / 分片记录
- 如果发现已有相同 hash 文件 → 直接复用
- 如果路径存在则直接修改用户表file_ids字段，直接建立关联，不需要在重新上传。
- 否则创建 FileInDB（含 total_parts）
- 如果文件路径不存在，**不写 user.file_ids**
- 

------

### 5.3 上传单个分片

**接口**

```
POST /upload_file_chunk
```

**参数**

| 参数        | 说明                  |
| ----------- | --------------------- |
| file_hash   | 文件整体 hash         |
| part_number | 分片编号（从 1 开始） |
| part_hash   | 分片 hash             |
| chunk       | 分片文件              |

**处理流程**

1. 校验分片编号合法
2. 校验 file_hash 对应的 FileInDB 存在
3. 校验用户在 owners 中
4. 校验分片未重复上传
5. 保存分片到临时目录
6. 写入 PartInDB
7. 检查是否可以合并

------

### 5.4 分片完整性校验规则

- 已上传分片数量 == total_parts
- 分片编号必须是 `1..N` 连续序列
- 所有分片文件必须存在

------

### 5.5 分片合并逻辑

**合并步骤**

1. 按 part_number 排序
2. 顺序写入最终文件
3. 实时计算 hash
4. 校验 hash == FileInDB.hash_sha256

**成功后**

- 删除所有分片文件
- 删除 PartInDB 记录
- 写入 user.file_ids
- 更新 file_count / total_file_size

**失败处理**

- 删除已写文件

- 调用 safe_cleanup_preflight_record

  ![分片文件上传](C:\桌面\分片文件上传.jpg)

------

## 6. 文件复用机制

### 6.1 复用触发条件

- hash_sha256 完全一致
- 物理文件存在

### 6.2 复用行为

- 不重复写磁盘
- 仅更新 FileInDB.owners
- 上传完成后再写 user.file_ids

------

## 7. 清理机制

### 7.1 预检清理（cleanup_stale_preflights）

**清理条件**

- 文件路径不存在
- file_id 不在 user.file_ids
- 超过 TTL
- owners 包含当前用户

**安全点**

- 使用 safe_cleanup_preflight_record
- 多用户共享文件不会被误删

------

### 7.2 分片清理（cleanup_stale_chunks）

- 清理过期 PartInDB
- 删除临时分片文件
- 不影响已完成文件

------

## 8. 权限模型

### assert_file_owner 规则

- 管理员直接放行
- 用户在 FileInDB.owners 中
- 或 file_id 在 user.file_ids 中

------

## 9. Swagger / 调试说明

### Swagger 的限制

- Swagger UI **不支持自动分片**
- 分片测试需：
  - 手动切文件
  - 或使用脚本 / 前端代码

### 推荐测试方式

1. Python 分片脚本
2. 前端 Blob.slice()

------

## 10. 典型状态机总结

```
NEW (preflight)
 ↓
UPLOADING (chunks)
 ↓
MERGING
 ↓
COMPLETED (user.file_ids 写入)

失败 → CLEANUP
```

------

## 11. 总结

该文件系统具备：

- ✔ 强一致性
- ✔ 支持大文件
- ✔ 文件复用
- ✔ 多用户共享
- ✔ 安全清理
- ✔ 易扩展（可接入对象存储）

**核心判断一句话**：

> 文件是否真正存在，对用户而言，以 `user.file_ids` 为准。

