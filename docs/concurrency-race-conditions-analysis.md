# Kawaru 项目并发竞态条件分析报告

> 分析日期：2026-06-01 | 分支：fix_bear | 数据库：MariaDB (AWS RDS) | ORM：SQLModel (SQLAlchemy)

---

## 一、核心问题

### 数据库是否自带锁？

**MariaDB/InnoDB 确实有行级锁**，但需要使用 `SELECT ... FOR UPDATE` 显式获取。当前项目使用的是 ORM 的 **read-modify-write** 模式：

```
请求A: SELECT download_count FROM fileindb WHERE file_id=1   → 读到 5
请求B: SELECT download_count FROM fileindb WHERE file_id=1   → 读到 5
请求A: Python: 5 + 1 = 6  →  UPDATE SET download_count=6
请求B: Python: 5 + 1 = 6  →  UPDATE SET download_count=6
                                    ↑ 正确结果应为 7，丢失一次更新
```

**原因**：InnoDB 默认隔离级别 `REPEATABLE READ` 通过 MVCC 保证同一事务内的读一致性（快照读），但**不能防止两个事务基于相同的旧快照各自执行更新**。这是经典的 **Lost Update（丢失更新）** 问题。

### 解决方案对比

| 方案 | 原理 | 优点 | 缺点 |
|------|------|------|------|
| **A. 原子 SQL UPDATE** | `UPDATE SET count = count + 1` | 最高效，DB 原生保证，无锁开销 | 需修改 ORM 代码风格 |
| **B. SELECT ... FOR UPDATE** | 悲观行锁，阻塞并发读 | 改动最小，ORM 友好 | 可能死锁，高并发下性能差 |
| **C. Redis 分布式锁** | 应用层互斥 | 跨服务可用 | 每次操作需 Redis 往返，需处理超时/死锁 |

**推荐：A（原子 SQL）+ C（Redis 锁）混合策略**

- **计数器增减** → 方案 A（原子 SQL UPDATE）
- **复杂多步互斥操作** → 方案 C（Redis `SET NX EX`）

---

## 二、已发现的 6 处竞态条件漏洞

### 漏洞 1：文件下载计数 `update_download_stats`

**文件**：[api/file_api_business.py:564-578](api/file_api_business.py#L564-L578)

```python
async def update_download_stats(session, file_record, current_user):
    now = utc_time()
    file_record.download_count += 1                           # ← 读-改-写
    file_record.last_downloaded_at = now
    current_user.download_used = (current_user.download_used or 0) + 1  # ← 读-改-写
    session.add(current_user)
    session.add(file_record)
```

**触发场景**：两个用户同时下载同一个文件，或同一用户并发下载多个文件。
**影响**：`FileInDB.download_count` 和 `UserInDB.download_used` 计数不准确。

---

### 漏洞 2：用户文件计数增加 `link_file_to_user`

**文件**：[api/file_api_business.py:132-134](api/file_api_business.py#L132-L134)

```python
user.file_count = (user.file_count or 0) + 1                 # ← 读-改-写
user.total_file_size = (user.total_file_size or 0) + file_size  # ← 读-改-写
```

**触发场景**：用户同时上传两个文件（或一个上传 + 一个秒传复用）。
**影响**：`UserInDB.file_count` 和 `UserInDB.total_file_size` 计数不准确。

---

### 漏洞 3：用户文件计数减少 `unlink_user_from_file`

**文件**：[api/file_api_business.py:684-685](api/file_api_business.py#L684-L685)

```python
user.file_count = max(0, (user.file_count or 0) - 1)                      # ← 读-改-写
user.total_file_size = max(0, (user.total_file_size or 0) - file_record.size)  # ← 读-改-写
```

> 代码中已有 `#TODO 风险，后面加锁` 注释，说明开发者已意识到此风险。

**触发场景**：用户同时删除两个文件。
**影响**：`UserInDB.file_count` 和 `UserInDB.total_file_size` 计数不准确。

---

### 漏洞 4：算法回调累加已处理大小 `algorithm_callback`

**文件**：[api/callback_api.py:252-258](api/callback_api.py#L252-L258)

```python
user = (await async_session.exec(
    select(UserInDB).where(UserInDB.id == run_record.user_id)
)).first()
if user is not None:
    user.processed_file_size += output_size  # ← 读-改-写
    async_session.add(user)
```

**触发场景**：同一用户提交的两个算法任务几乎同时完成，FC（函数计算）回调几乎同时到达服务器。
**影响**：`UserInDB.processed_file_size` 计数不准确。

---

### 漏洞 5：删除运行记录扣减已处理大小 `delete_process_run`

**文件**：[api/processing_api_business.py:97-103](api/processing_api_business.py#L97-L103)

```python
user = (await async_session.exec(
    select(UserInDB).where(UserInDB.id == run_record.user_id))).first()
if user is not None:
    user.processed_file_size = max(0, user.processed_file_size - run_record.output_size)  # ← 读-改-写
    async_session.add(user)
```

**触发场景**：用户同时删除多个算法运行记录。
**影响**：`UserInDB.processed_file_size` 计数不准确。

---

### 漏洞 6：OSS 打包 Redis 锁本身有竞态条件

**文件**：[api/file_api_business.py:355-363](api/file_api_business.py#L355-L363)

```python
lock = await redis_client.get(oss_packup_lock_key)     # ← 检查
if lock:
    zip_download_url = "<PACKING>"
else:
    await redis_client.set(oss_packup_lock_key, "1", ex=10)  # ← 设置（非原子！）
```

这是经典的 **check-then-act** 反模式：

```
请求A: GET lock_key → None → 准备 SET
请求B: GET lock_key → None → 准备 SET
请求A: SET lock_key = "1"
请求B: SET lock_key = "1"   ← 两个请求都认为自己获得了锁！
```

**两个并发的下载请求可能同时触发 OSS 打包**，造成资源浪费甚至数据不一致。

---

## 三、修改方案

### 3.1 新建原子操作工具模块

**新文件**：`database/atomic_ops.py`

```python
"""
原子数据库操作辅助模块

提供基于 SQLAlchemy update() 的原子计数器增减操作，
避免 ORM read-modify-write 模式在高并发下的丢失更新（Lost Update）问题。

使用方式：
    from database.atomic_ops import atomic_increment, atomic_decrement

    await atomic_increment(session, UserInDB, user.id, "file_count", 1)
    await atomic_decrement(session, UserInDB, user.id, "total_file_size", file_size)

设计原则：
    - 所有操作在调用方事务内执行，不自动 commit
    - 更新后调用方需要 refresh 以获取 ORM 对象的最新值
    - decrement 时自动使用 GREATEST 保护，值不会低于 0
"""

from __future__ import annotations

from typing import Any
from sqlmodel import update, func
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel.main import SQLModel


async def _atomic_update(
    session: AsyncSession,
    model: type[SQLModel],
    pk_column: str,
    pk_value: Any,
    field: str,
    expression: Any,
) -> None:
    """执行原子字段更新（内部函数）"""
    pk_col = getattr(model, pk_column)
    stmt = (
        update(model)
        .where(pk_col == pk_value)
        .values(**{field: expression})
    )
    await session.exec(stmt)


async def atomic_increment(
    session: AsyncSession,
    model: type[SQLModel],
    pk_value: Any,
    field: str,
    amount: int | float = 1,
    *,
    pk_column: str = "id",
) -> None:
    """
    原子地将指定记录的字段值增加 amount。

    Example:
        await atomic_increment(session, FileInDB, file_id, "download_count")
        await atomic_increment(session, UserInDB, user.id, "download_used", 5)
    """
    col = getattr(model, field)
    await _atomic_update(session, model, pk_column, pk_value, field, col + amount)


async def atomic_decrement(
    session: AsyncSession,
    model: type[SQLModel],
    pk_value: Any,
    field: str,
    amount: int | float = 1,
    *,
    pk_column: str = "id",
) -> None:
    """
    原子地将指定记录的字段值减少 amount，结果不低于 0。
    """
    col = getattr(model, field)
    await _atomic_update(
        session, model, pk_column, pk_value, field,
        func.greatest(col - amount, 0),
    )


async def atomic_set(
    session: AsyncSession,
    model: type[SQLModel],
    pk_value: Any,
    field: str,
    value: Any,
    *,
    pk_column: str = "id",
) -> None:
    """原子地设置指定记录的字段值"""
    await _atomic_update(session, model, pk_column, pk_value, field, value)
```

---

### 3.2 修复漏洞 1：`update_download_stats`

**文件**：[api/file_api_business.py](api/file_api_business.py)

**顶部新增导入**：

```python
from database.atomic_ops import atomic_increment, atomic_set
```

**函数替换**：

```python
async def update_download_stats(
    session: AsyncSession,
    file_record: FileInDB,
    current_user: UserInDB,
) -> None:
    """
    更新下载统计（原子操作，防止并发丢失更新）

    使用数据库级别的 UPDATE SET field = field + 1，
    避免 ORM read-modify-write 模式在高并发下的竞态条件。
    """
    now = utc_time()
    assert file_record.file_id is not None and current_user.id is not None

    # 原子递增文件下载次数 + 更新最近下载时间
    await atomic_increment(
        session, FileInDB, file_record.file_id,
        "download_count", pk_column="file_id",
    )
    await atomic_set(
        session, FileInDB, file_record.file_id,
        "last_downloaded_at", now, pk_column="file_id",
    )

    # 原子递增用户已用下载次数
    await atomic_increment(
        session, UserInDB, current_user.id,
        "download_used",
    )
```

> **注意**：原子更新后，调用方 `get_download_file_metadata` 中的 `session.refresh()` 可以确保 ORM 对象同步到最新值。

---

### 3.3 修复漏洞 2：`link_file_to_user`

**文件**：[api/file_api_business.py](api/file_api_business.py)

**函数替换**：

```python
async def link_file_to_user(
    async_session: AsyncSession,
    user: UserInDB,
    file: FileInDB,
    file_size: int,
) -> None:
    # 1. 将文件添加到用户的文件列表
    new_linker = await append_file_link(async_session, user, file)

    # 2. 更新用户的文件统计信息（原子操作，防止并发丢失更新）
    if new_linker:
        assert user.id is not None
        await atomic_increment(async_session, UserInDB, user.id, "file_count")
        await atomic_increment(async_session, UserInDB, user.id, "total_file_size", file_size)

    # 3. 将链接器对象添加到会话
    if new_linker:
        async_session.add(new_linker)
```

> **注意**：无需再 `async_session.add(user)`，因为统计信息已通过原子 SQL 直接更新到数据库。

---

### 3.4 修复漏洞 3：`unlink_user_from_file`

**文件**：[api/file_api_business.py](api/file_api_business.py)

**顶部新增导入**（与漏洞 1 共用）：

```python
from database.atomic_ops import atomic_increment, atomic_decrement, atomic_set
```

**函数替换**：

```python
async def unlink_user_from_file(
    session: AsyncSession,
    user: UserInDB,
    file_record: FileInDB,
) -> bool:
    # 1. 更新当前用户的文件关联
    assert file_record.file_id is not None and user.id is not None
    await remove_file_link(session, user, file_record)
    session.add(file_record)

    # 2. 更新统计信息（原子操作，防止并发丢失更新）
    await atomic_decrement(session, UserInDB, user.id, "file_count")
    await atomic_decrement(session, UserInDB, user.id, "total_file_size", file_record.size)

    # 3. 刷新事务
    await session.flush()
    await session.refresh(file_record)
    # 4. 返回是否还有其他用户持有该文件
    owner_counts = (await session.exec(
        select(func.count()).where(UserFileLink.file_id == file_record.file_id)
    )).one()

    return bool(owner_counts)
```

> 可以移除原来的 `#TODO 风险，后面加锁` 注释。

---

### 3.5 修复漏洞 4：`algorithm_callback`

**文件**：[api/callback_api.py](api/callback_api.py)

**顶部新增导入**：

```python
from database.atomic_ops import atomic_increment
```

**替换累加用户处理配额的代码块**：

```python
# 原代码（第 252-258 行）：
# user = (await async_session.exec(
#     select(UserInDB).where(UserInDB.id == run_record.user_id)
# )).first()
# if user is not None:
#     user.processed_file_size += output_size
#     async_session.add(user)

# 替换为：
# 累加用户处理配额（原子操作，防止并发丢失更新）
if run_record.user_id is not None:
    await atomic_increment(
        async_session, UserInDB, run_record.user_id,
        "processed_file_size", output_size,
    )
```

---

### 3.6 修复漏洞 5：`delete_process_run`

**文件**：[api/processing_api_business.py](api/processing_api_business.py)

**顶部新增导入**：

```python
from database.atomic_ops import atomic_decrement
```

**替换扣除用户已处理配额的代码块**：

```python
# 原代码（第 97-103 行）：
# user = (
#     await async_session.exec(
#         select(UserInDB).where(UserInDB.id == run_record.user_id))).first()
# if user is not None:
#     user.processed_file_size = max(0, user.processed_file_size - run_record.output_size)
#     async_session.add(user)

# 替换为：
# 扣除用户已处理配额（原子操作，防止并发丢失更新）
if run_record.output_size and run_record.output_size > 0 and run_record.user_id is not None:
    await atomic_decrement(
        async_session, UserInDB, run_record.user_id,
        "processed_file_size", run_record.output_size,
    )
```

---

### 3.7 修复漏洞 6：OSS 打包 Redis 锁

**文件**：[api/file_api_business.py](api/file_api_business.py) 的 `prepare_download_file_oss` 函数

**替换 check-then-act 为非原子 SET NX**：

```python
# 原代码（第 355-363 行）：
# should_pack = True
# lock = await redis_client.get(oss_packup_lock_key)
# if lock:
#     logger.warning("...")
#     zip_download_url = "<PACKING>"
# else:
#     await redis_client.set(oss_packup_lock_key, "1", ex=10)

# 替换为：
# 原子加锁（SET NX），防止多个进程同时触发打包
lock_acquired = await redis_client.set(oss_packup_lock_key, "1", nx=True, ex=10)
if not lock_acquired:
    logger.warning(
        "packup is in progress by other user, now wait for it to finish, filename=%s",
        file_record.filename
    )
    zip_download_url = "<PACKING>"
else:
    should_pack = True
```

**原理说明**：Redis `SET key value NX EX seconds` 是原子命令：
- `NX`（Not eXists）：仅当 key 不存在时才设置成功
- `EX`：设置过期时间，防止死锁

这样 check 和 set 在 Redis 服务端原子执行，彻底消除竞态窗口。

---

## 四、为什么不全部用 Redis 锁？

| 维度 | 原子 SQL UPDATE | Redis 分布式锁 |
|------|:--:|:--:|
| 性能 | ✅ 无额外网络开销 | ❌ 每次操作需 Redis 往返 |
| 可靠性 | ✅ 与业务数据在同一事务 | ❌ Redis 不可用时计数器无法更新 |
| 复杂度 | ✅ 一行 SQL | ❌ 需要处理锁超时、死锁、锁续期 |
| 跨服务协调 | ❌ 仅限同一 DB | ✅ 跨多个应用实例 |
| 适合场景 | 计数器增减 | 多步互斥操作（如 OSS 打包） |

**结论**：计数器操作优先使用原子 SQL UPDATE，这是最高效且最可靠的方案。Redis 锁仅在需要跨多步操作互斥时使用（当前项目只有 OSS 打包属于此场景）。

---

## 五、修改清单汇总

| 文件 | 修改内容 |
|------|---------|
| `database/atomic_ops.py` | **新建**：原子操作工具模块 |
| `api/file_api_business.py` | 新增 import；修复 `update_download_stats`、`link_file_to_user`、`unlink_user_from_file`、OSS 打包锁 |
| `api/callback_api.py` | 新增 import；修复 `algorithm_callback` |
| `api/processing_api_business.py` | 新增 import；修复 `delete_process_run` |

> **无需修改数据库 Schema**，所有变更仅涉及 Python 代码层面的 SQL 生成方式。

---

## 六、验证建议

修改完成后，建议通过以下方式验证：

1. **单元测试**：模拟并发场景，使用 `asyncio.gather()` 并发调用下载/上传/删除接口，验证计数器最终一致性
2. **压力测试**：使用已有的 `tests/test_perf_concurrent_login.py` 和 `tests/test_oss_concurrent.py` 的测试框架，扩展针对下载计数的并发测试
3. **数据库直接验证**：并发测试后直接查询数据库 `SELECT download_count FROM fileindb WHERE file_id=X`，验证是否等于实际并发请求数
