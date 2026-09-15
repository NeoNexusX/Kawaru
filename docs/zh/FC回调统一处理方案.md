# FC 回调处理方案（鉴权 + 幂等 + 对象存在性）

> 起草日期：2026-08-25（2026-08-26 修订：每个回调各自直白实现三项保证）
> 范围：algorithm_callback、tic_callback、feature_callback(umap)、
> clustering_callback、oss_packup_callback。
> 相关背景见 [上传回调流程.md](上传回调流程.md)，oss_upload_callback 本身的可重入改造另行处理。

## 1. 方案

五个回调**各自直白实现**以下三项保证，互不依赖：

1. **鉴权**：路由级 Token 验证（已有，保持不变）
2. **幂等**：条件 UPDATE（一条 SQL，看 rowcount）
3. **对象存在性**：落库前调 `OssStorage.check_file_exist()`（后端已有）

## 2. 现状问题（改这些的原因）

| 问题 | 位置 | 后果 |
| --- | --- | --- |
| algorithm 回调无状态检查，重复成功回调重复累加配额 | `callback_api.py` atomic_increment | 用户 processed_file_size 双计，实际资损 |
| tic/umap 失败分支吞掉：仅打日志即返回 200 | `callback_api.py` | 预览失败不可见、无状态、无重派，image_path 永远为 NULL |
| 落库前不验证 OSS 对象存在 | 全部回调 | COMPLETED 但下载 404（信 FC 说什么是什么） |
| packup 旧回调可覆盖新一轮打包状态 | Redis hash | 状态指向已被 TTL 清理的 zip（死链） |

## 3. 三项保证的实现方式

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

## 4. 失败处理：后端零重试

后端不做任何回调重试（无重试队列、无定时重发），重试全部交给
阿里云 FC 异步调用自带机制（默认重试 3 次，可配 0~8 次）。
后端唯一职责是用返回码告知 FC 该不该重试：

| 返回 | 场景 | FC 侧行为 |
| --- | --- | --- |
| 200 | 落库成功 / 幂等跳过 / 永久失败（is_success=false 落库后） | 停止重试 |
| 404 | 目标记录不存在 | 停止重试 + 告警 |
| 5xx | DB 抖动、OSS 探测失败（瞬时） | FC 自带重试；3.2 的幂等保证重复回调不重复生效 |

注意：FC 的重试是**重跑整个函数**而非只重发回调，因此重复回调必然发生，
幂等（3.2）不可省。FC 重试覆盖不了的失败（FC 崩溃、消息超 TTL 丢弃、
重试耗尽）由 `cleanup_stale_process_runs` 超时兜底标 FAILED。

## 5. 各回调改动点

| 回调 | 改动 |
| --- | --- |
| algorithm_callback（已完成） | 先查状态幂等跳过重复回调；成功迁移条件 UPDATE（判断并入 WHERE，rowcount 门控配额副作用，修配额双计）；落库前 check_file_exist(out_path)；失败仅 PROCESSING 在途时落 FAILED |
| clustering_callback（已完成） | 先查 clustering_status：成功非 COMPLETED 才写（迟到成功可从 FAILED 翻转）、失败仅 PROCESSING 在途才写；无路径不做出存在性检查 |
| tic_callback / umap_callback | 合并为参数化 handler；幂等靠先查 image_path 是否已有值（**不加状态列**）；成功落库前 check_file_exist(image_path)；失败维持日志记录 |
| oss_packup_callback | 待定（方案 A 纯后端 / 方案 B packup_id 需 FC 配合，分析后定） |

表结构变更：无（tic/umap 不加状态列，幂等靠 image_path 已有值判断）。
`experiment_process_run` 的 error_code 列为可选，暂不做。

## 6. 实施顺序

1. algorithm_callback（已完成，配额双计是资损，最优先）
2. clustering_callback（已完成，最简单）
3. tic/umap 合并（无 DDL，幂等靠 image_path 已有值）
4. oss_packup（方案待定；若用 packup_id 需 FC 侧配合透传）

## 7. 已完成改动记录（2026-08-26）

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

### 7.2 oss_packup_callback 分析（未改代码，方案待定）

三方协作现状：

```
发起侧 _execute_packup_flow          FC 打包函数                 回调 oss_packup_callback
─────────────────────────          ──────────────              ──────────────────────
invoke_fc({source-dir,             打包 -> dest-file           hset {status, file_path}
  dest-file: {file_id}.zip})                                   + TTL（无任何校验）
hset {status: PACKING} + TTL
                                   读侧 _handle_packup_status_check 轮询：
                                   COMPLETED -> 验 zip 存在，缺失自动重新打包（已有自愈）
```

问题（按真实严重程度排序）：

1. **无轮次标识**：新一轮打包进行中，旧一轮的迟到回调直接覆盖。最有害
   组合是「旧轮迟到 FAILED 覆盖新轮 COMPLETED」-> 用户看到失败，读侧
   还会删 key 报错，触发一次无谓的重打包
2. **COMPLETED 不验 zip 存在**：会写入死链；但读侧已有自愈
   （`_handle_packup_status_check` 的 COMPLETED 分支会 check_file_exist，
   缺失则重新打包），属加固项而非漏洞
3. 顺带发现的两个小问题：成功/失败两个分支代码完全相同（都是裸 hset）；
   Redis key 用 **filename** 而 zip 路径用 **file_id**，filename 无唯一
   约束，同名文件会共享打包状态（已修复：状态键与触发锁改为按 file_id，
   回调从 zip_path 反解 file_id，FC 无需改动）

关键观察：zip 路径是确定性的（`{file_id}.zip`，每轮相同），所以「旧轮
回调写旧 zip 造成死链」不成立；真正残留的坏场景只有「旧轮迟到 FAILED
污染新轮状态」，且读侧自愈能兜住。严重性远低于 algorithm 的配额双计。

两个候选方案：

| | 方案 A：状态前置检查（纯后端） | 方案 B：packup_id 轮次标识 |
| --- | --- | --- |
| 做法 | 回调侧仅接受在途状态：key 不存在（TTL 已过）或 status != PACKING 的回调一律丢弃；COMPLETED 写入前 check_file_exist(zip_path)，缺失改写 FAILED | 发起侧生成 uuid 存 Redis hash 并随 FC 事件下发，回调比对，不匹配丢弃 |
| 挡住 | 迟到 FAILED 覆盖终态、key 过期后回调凭空重建 hash、重复 COMPLETED | 全部，含两轮 PACKING 窗口交错的场景 |
| 挡不住 | 两轮都在 PACKING 窗口内交错（无法区分轮次；路径相同 + 读侧自愈，基本无害） | 无 |
| 依赖 | 无（约 15 行，顺手合并现有重复分支） | FC 侧配合透传 packup_id（回调目前是 query 参数，需 FC 事件带回） |

倾向：先做方案 A（零外部依赖，覆盖绝大部分风险），方案 B 等下次
动 FC 代码时再叠加。

后续可选（当前不做）：回调路径前缀白名单（防错误路径进入删除链路）、
鉴权改 compare_digest。
