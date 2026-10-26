# FC 回调历史改造方案（鉴权 + 幂等 + 对象存在性）

> **文档性质：历史设计方案，不代表当前代码已经实现以下全部保证。** 当前行为请以 [异步任务与回调架构](异步任务与回调架构.md) 和 `api/callback_api.py` 为准；尤其是 algorithm 与 clustering 回调仍存在并发重复处理窗口，preview 对象缺失时仅记录日志后正常返回。

> 起草日期：2026-08-25（2026-08-26 修订：每个回调各自直白实现三项保证）
> 范围：algorithm_callback、tic_callback、feature_callback(umap)、
> clustering_callback。
> 相关背景见 [异步任务与回调架构](异步任务与回调架构.md)，oss_upload_callback 本身的可重入改造另行处理。

## 1. 原方案目标

原方案要求五个回调**各自直白实现**以下三项保证，互不依赖；下列内容是目标，不是当前实现说明：

1. **鉴权**：路由级 Token 验证（已有，保持不变）
2. **幂等**：条件 UPDATE（一条 SQL，看 rowcount）
3. **对象存在性**：落库前调 `OssStorage.check_file_exist()`（后端已有）

## 2. 现状问题（改这些的原因）

| 问题 | 位置 | 后果 |
| --- | --- | --- |
| algorithm 回调无状态检查，重复成功回调重复累加配额 | `callback_api.py` atomic_increment | 用户 processed_file_size 双计，实际资损 |
| tic/umap 失败分支吞掉：仅打日志即返回 200 | `callback_api.py` | 预览失败不可见、无状态、无重派，image_path 永远为 NULL |
| 落库前不验证 OSS 对象存在 | 全部回调 | COMPLETED 但下载 404（信 FC 说什么是什么） |

## 3. 原方案拟采用的实现方式

### 3.1 鉴权（已有，不动）

`callback_router` 挂 `verify_fc_callback_token` 依赖，比对 `X-FC-Token`
与环境变量 `FC_CALLBACK_TOKEN`。可选加固：比对改用
`secrets.compare_digest` 防时序攻击。

### 3.2 幂等：先查状态，再更新

回调到达后先查目标记录当前状态，非终态才更新并执行副作用；
已是终态（重复/迟到回调）直接返回 200，副作用不再执行：

- 成功回调：状态非 COMPLETED 才写 COMPLETED（超时 FAILED 后的迟到成功
  回调也因此能翻转），同事务执行副作用（配额累加、default_run_id 等）
- 失败回调：状态为 PROCESSING 才落 FAILED，终态不被覆盖

重复回调主要来自 FC 失败重试（重跑整个函数），与首次回调在时间上
基本串行，先查再写即可拦截。

并发窗口说明：两个重复回调同时到达时（FC 超时击杀后立刻重试、
二次执行又较快），查和写之间的窗口可能漏过。当前接受此残余风险
（其余回调漏过也只是同值重复写入，无害）；如需一行加固，可把判断
并入 UPDATE 的 WHERE 子句（`AND status != 'completed'`，以 rowcount
判断是否生效），建议至少在 algorithm 的配额累加处采用（唯一的资损项）。

### 3.3 对象存在性：`check_file_exist`

成功落库前调已有的 `OssStorage.check_file_exist(path)` 验证对象真实存在
（zarr 目录用前缀探测）。不存在 -> 返回 5xx 触发 FC 自带重试
（可能是时序问题）；持续失败由超时兜底任务判死。

## 4. 原方案中的失败处理设想：后端零重试

原方案不在后端做回调重试（无重试队列、无定时重发），而是设想由
阿里云 FC 的异步调用策略负责重试。具体重试次数和对 HTTP 状态码的
处理属于仓库外配置，不能仅凭本仓库保证；下表是方案期望的协议：

| 返回 | 场景 | FC 侧行为 |
| --- | --- | --- |
| 200 | 落库成功 / 幂等跳过 / 永久失败（is_success=false 落库后） | 停止重试 |
| 404 | 目标记录不存在 | 期望停止重试并告警 |
| 5xx | DB 抖动、OSS 探测失败（瞬时） | 期望 FC 重试；若完成 3.2 的改造，重复回调不应重复生效 |

注意：FC 的重试是**重跑整个函数**而非只重发回调，因此重复回调必然发生，
幂等（3.2）不可省。FC 重试覆盖不了的失败（FC 崩溃、消息超 TTL 丢弃、
重试耗尽）由 `cleanup_stale_process_runs` 超时兜底标 FAILED。

## 5. 原方案记录的各回调改动点

| 回调 | 改动 |
| --- | --- |
| algorithm_callback（历史记录称已完成，当前未完整实现） | 计划先查状态幂等跳过重复回调；成功迁移条件 UPDATE（判断并入 WHERE，rowcount 门控配额副作用，修配额双计）；落库前 check_file_exist(out_path)；失败仅 PROCESSING 在途时落 FAILED |
| clustering_callback（历史记录称已完成，当前未完整实现） | 计划先查 clustering_status：成功非 COMPLETED 才写（迟到成功可从 FAILED 翻转）、失败仅 PROCESSING 在途时才写；无路径不做存在性检查 |
| tic_callback / umap_callback | 合并为参数化 handler；幂等靠先查 image_path 是否已有值（**不加状态列**）；成功落库前 check_file_exist(image_path)；失败维持日志记录 |

表结构变更：无（tic/umap 不加状态列，幂等靠 image_path 已有值判断）。
`experiment_process_run` 的 error_code 列为可选，暂不做。

## 6. 实施顺序

1. algorithm_callback（已完成，配额双计是资损，最优先）
2. clustering_callback（已完成，最简单）
3. tic/umap 合并（无 DDL，幂等靠 image_path 已有值）

## 7. 历史改动记录（2026-08-26，不能作为当前实现依据）

以下文字保留当时的设计与实施记录。当前仓库已与该记录发生偏离：algorithm 并未使用条件 UPDATE 门控配额副作用，clustering 也没有终态保护。因此，下面的“已完成”和行为对照仅表示历史记录，不表示现状。

### 7.1 algorithm_callback / clustering_callback（已完成）

涉及文件：`api/callback_api.py`、`exceptions/callback_exceptions.py`。

- 新增异常 `fc_callback_target_not_found_exception`（404）：两个回调的
  「run 不存在」从 400（invalid_process_request）改为 404，按第 4 节协议
  表示永久失败、FC 停止重试
- **algorithm_callback**：
  - 幂等：已 COMPLETED 的重复成功回调直接返回；成功迁移改条件 UPDATE
    （`WHERE status != 'completed'`），**rowcount=1 才执行配额累加与
    default_run_id 写入**（3.2 节的一行加固，防并发重复回调双计配额）；
    超时 FAILED 的迟到成功回调仍可翻转，且只计一次配额
  - 失败分支：仅 status=PROCESSING 在途时落 FAILED，终态不被
    重复/迟到失败回调覆盖
  - 对象存在性：成功落库前 `check_file_exist(out_path)`，zarr 目录
    （尾 `/`）自动走前缀探测；缺失返回 500 交 FC 自带重试
- **clustering_callback**：纯先查再写--成功非 COMPLETED 才写（迟到成功
  可从 FAILED 翻转）、失败仅 PROCESSING 在途才写；无路径字段，不做出
  存在性检查

行为变化对照：

| 场景 | 旧 | 新 |
| --- | --- | --- |
| 重复成功回调（FC 重试重跑） | 配额双计、副作用重放 | 幂等 200，配额只计一次 |
| COMPLETED 后到达迟到失败回调 | 覆盖成 FAILED | 跳过，终态保持 |
| out_path 在 OSS 不存在 | 照样标 COMPLETED | 500 -> FC 重试 -> 超时兜底判死 |
| run 不存在 | 400 | 404（FC 停止重试） |

### 7.2 后续可选（当前不做）

后续可选（当前不做）：回调路径前缀白名单（防错误路径进入删除链路）、
鉴权改 compare_digest。
