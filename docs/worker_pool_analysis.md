# Celery Worker Pool 选型分析与优化方案

## 一、背景

Kawaru 后端原本配置了两个 Celery worker：

| Worker | 队列 | Pool | Concurrency | 用途 |
|--------|------|------|-------------|------|
| default | `default` | prefork | 10 | I/O 任务（OSS、邮件、FC 调用等） |
| compute | `compute` | prefork | 4 | 重计算任务（算法处理） |

经过分析发现：

1. **compute worker 已空转** — 重计算任务已全部迁移到阿里云 FC 函数计算，`background/compute_background.py` 仅剩一个 docstring。`task_routes` 为空，所有 `.delay()` 调用都走 default 队列，compute worker 启动后无任何任务路由到它，白白消耗约 320-600MB 内存。

2. **default worker 使用 prefork 模型不合理** — 所有 Celery task 都是 `async def`，由 `CeleryAsyncTask.__call__` 内部调用 `asyncio.run()` 执行，任务 100% 是 I/O 密集型（网络请求、磁盘读写、数据库查询），CPU 利用率极低。prefork 模型下 10 个子进程约占用 800MB-1.5GB 内存，对 I/O 任务来说性价比很差。

---

## 二、Celery Worker Pool 类型对比

### 1. prefork（默认）

**原理**：主进程 fork 出 N 个子进程，每个子进程独立加载 Python 解释器 + 应用代码，进程间完全隔离。

**特点**：
- ✅ 真正的并行，能利用多核 CPU
- ✅ 一个子进程崩溃不影响其他
- ❌ 每个子进程独立占用内存（约 80-150MB/个），N=10 就是 800MB-1.5GB
- ❌ fork 开销大，启动慢
- ❌ 进程间上下文切换成本高

**适合场景**：CPU 密集型任务（数值计算、图像处理、加密解密）

---

### 2. gevent

**原理**：基于协程的并发模型。单进程内通过 monkey-patch 将标准库的 I/O 操作替换为非阻塞版本，一个进程内可以同时运行成百上千个协程。

**特点**：
- ✅ 内存占用极低（单进程，约 100-200MB 总计）
- ✅ I/O 并发能力极强（协程切换开销约 1μs vs 进程切换约 100μs）
- ✅ 启动快
- ❌ 不能利用多核 CPU（单进程单线程）
- ❌ 需要安装 `gevent` 依赖
- ❌ monkey-patch 可能与某些库不兼容
- ❌ CPU 密集型任务会阻塞整个进程

**适合场景**：I/O 密集型任务（网络请求、文件读写、数据库查询、API 调用）

---

### 3. eventlet

**原理**：与 gevent 类似，也是协程模型，基于 `greenlet` + `epoll`。区别在于底层实现和 monkey-patch 方式不同。

**特点**：
- ✅ 内存占用低，与 gevent 相当
- ✅ I/O 并发能力强
- ❌ 社区活跃度不如 gevent，近年来维护较少
- ❌ 需要安装 `eventlet` 依赖
- ❌ 兼容性问题比 gevent 更多

**适合场景**：同 gevent，但生态和稳定性稍逊

---

### 4. solo

**原理**：单进程单线程，不使用任何并发池，任务串行执行。

**特点**：
- ✅ 内存最低
- ❌ 完全串行，吞吐量最低
- 适合极低流量或调试场景

---

## 三、对比总结

| 特性 | prefork | gevent | eventlet | solo |
|------|---------|--------|----------|------|
| 并发模型 | 多进程 | 协程 | 协程 | 单线程 |
| 内存占用（10并发） | 800MB-1.5GB | 100-200MB | 100-200MB | 50-100MB |
| CPU 并行 | ✅ 多核 | ❌ 单核 | ❌ 单核 | ❌ 单核 |
| I/O 并发能力 | 低（进程数限制） | 极高（协程数） | 极高（协程数） | 无 |
| 启动速度 | 慢（fork） | 快 | 快 | 最快 |
| 依赖 | 无 | gevent | eventlet | 无 |
| 兼容性风险 | 无 | 低（monkey-patch） | 中 | 无 |
| 适合任务类型 | CPU 密集 | I/O 密集 | I/O 密集 | 开发/调试 |

---

## 四、Kawaru 项目 Task 分析

遍历所有 Celery task 后，得出以下结论：

| 任务模块 | 任务函数 | 类型 | I/O 特征 |
|----------|----------|------|----------|
| file_background | heartbeat | I/O | 无操作 keepalive |
| file_background | cleanup_old_download_zips | I/O | 数据库查询 + 文件删除 |
| file_background | cleanup_stale_preflights_all_users | I/O | 数据库查询 + 删除 |
| file_background | cleanup_failed_experiments | I/O | 数据库查询 + 删除 |
| file_background | build_download_zip | I/O | OSS 下载 + 磁盘写入 |
| file_background_helper | delete_oss_files | I/O | OSS HTTP 调用 |
| api_background | send_email_async | I/O | SMTP 连接 |
| api_background | validate_orcid_async | I/O | HTTP 请求 |
| experiment_background | process_experiment_task | I/O | FC HTTP 触发 + DB 写入 |
| processing_background | trigger_fc_algorithm | I/O | FC HTTP 触发 |
| processing_background | trigger_fc_tic | I/O | FC HTTP 触发 |
| oss_log_helper | poll_fc_log_task | I/O | HTTP 轮询 FC 日志 |

**结论：所有 task 100% 是 I/O 密集型**，没有任何 CPU 密集型计算。所有 task 都是 `async def`，通过 `asyncio.run()` 在 worker 进程内执行异步 I/O。

---

## 五、最终方案

### 选择：gevent + concurrency=30

```yaml
celery:
  enable_embedded_celery: true
  worker_concurrency: 30
  worker_log_level: "INFO"
  worker_log_file: "kawaru_logs/celery.log"
  worker_disable_rate_limits: True
  worker_pool: "gevent"
  large_file_poll_time: 1000
```

### 选择理由

1. **任务特性完美匹配** — 所有 task 都是 I/O 等待（OSS、HTTP、SMTP、DB），gevent 协程模型下 I/O 阻塞时自动切换到其他协程，不浪费 CPU 时间片。

2. **内存大幅降低** — 单进程 + 30 个协程，总计约 100-200MB；对比原来 prefork×10 的 800MB-1.5GB，降低 5-10 倍。

3. **并发能力提升** — gevent 的 30 个协程可以同时处理 30 个 I/O 操作；原来 prefork×10 只能同时处理 10 个。对于 OSS 大文件上传、FC 函数调用等场景，并发能力提升 3 倍。

4. **依赖已就绪** — `pyproject.toml` 中已有 `celery[gevent]>=5.6.2` 和 `gevent>=26.4.0`，无需额外安装。

5. **兼容性良好** — 项目使用的 `redis`、`sqlalchemy`、`httpx`、`aiohttp` 都是 gevent 兼容的。gevent 的 monkey-patch 在 Celery worker 启动时自动完成，不影响 FastAPI 主进程。

### 移除 compute worker

同时移除了已空转的 compute worker，涉及以下文件：

| 文件 | 变更 |
|------|------|
| `background/compute_background.py` | 删除 |
| `models/config_models.py` | 删除 `ComputeCelerySettings` 类和 `Config.compute_celery` 字段 |
| `celery_runtime/launcher.py` | 删除 compute worker 启动逻辑，更新注释 |
| `config_example.yaml` | 删除 `compute_celery:` 配置段 |
| `config.yaml` | 删除 `compute_celery:` 配置段 |

### 预期收益

| 指标 | 优化前 | 优化后 |
|------|--------|--------|
| Worker 进程数 | 2 个（default + compute） | 1 个 |
| 内存占用 | ~1.2-2.0GB（prefork×10 + prefork×4） | ~100-200MB（gevent×30） |
| I/O 并发数 | 10（default）+ 4（compute 空转） | 30 |
| 空转浪费 | compute worker 无任务但占内存 | 无 |
