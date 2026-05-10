# `concurrent_login_<timestamp>.json` 字段说明

由 `uv run pytest -m perf_concurrent` 生成，对应测试 [tests/test_perf_concurrent_login.py](../test_perf_concurrent_login.py)。

测试场景：100 个并发任务（每个任务独立 `httpx.AsyncClient`）同时执行
`POST /login` → `POST /files/list_files?page=1&size=10`，记录每个任务从
登录开始到 file_list 响应到达的端到端延迟。

文件名时间戳格式：`concurrent_login_YYYYMMDD_HHMMSS.json`，由生成时刻决定。

---

## 顶层字段

| 字段              | 类型      | 单位 | 含义                                              |
|-------------------|-----------|------|--------------------------------------------------|
| `concurrency`     | int       | —    | 并发任务数（默认 100）                            |
| `endpoint_login`  | string    | —    | 登录接口路径，固定 `"/login"`                      |
| `endpoint_list`   | string    | —    | 列文件接口路径（含 page/size）                    |
| `wall_elapsed_s`  | float     | 秒   | 整批 100 任务从开始到全部完成的墙钟时间            |
| `success_count`   | int       | —    | 成功任务数                                        |
| `failure_count`   | int       | —    | 失败任务数（任意一步抛异常或非 200 都算）          |
| `stats`           | object    | —    | 仅对成功任务 `duration_ms` 的聚合统计，见下        |
| `latencies_ms`    | float[]   | 毫秒 | 仅成功任务的延迟列表，长度 = `success_count`，论文画分布图直接用 |
| `results_full`    | object[]  | —    | 全量逐任务结果（含失败），长度始终 = `concurrency`  |

---

## `stats` 子字段

对 `latencies_ms` 计算（线性插值法分位数，与 numpy 默认 `linear` 一致）：

| 字段       | 单位 | 含义                                |
|-----------|------|-------------------------------------|
| `count`   | —    | 样本数（= `success_count`）         |
| `min_ms`  | 毫秒 | 最小延迟                             |
| `avg_ms`  | 毫秒 | 平均延迟                             |
| `p50_ms`  | 毫秒 | 中位数（50 百分位）                  |
| `p95_ms`  | 毫秒 | 95 百分位（论文常用）                |
| `p99_ms`  | 毫秒 | 99 百分位（尾延迟）                  |
| `max_ms`  | 毫秒 | 最大延迟                             |

---

## `results_full[i]` 子字段（每个任务）

| 字段             | 类型              | 含义                                                       |
|------------------|-------------------|-----------------------------------------------------------|
| `index`          | int               | 任务序号（0 ~ concurrency-1）                              |
| `login_status`   | int / null        | `/login` 响应码；网络异常未拿到响应时为 null                |
| `list_status`    | int / null        | `/files/list_files` 响应码；登录失败/未到达此步时为 null    |
| `logout_status`  | int / null        | `/logout` 响应码（仅供观察，不计入延迟）；异常时为 null     |
| `duration_ms`    | float / null      | 该任务 login 到 list_files 响应的端到端延迟；失败时为 null  |
| `error`          | string / null     | 异常文本（来自 `repr(e)`）；成功为 null                    |

---

## 论文使用建议

- **延迟分布图**：直接读 `latencies_ms`，画直方图 / CDF 即可
- **关键数值**：`stats` 里的 `avg_ms` / `p50_ms` / `p95_ms` / `p99_ms` 可直接写入正文
- **并发吞吐**：`concurrency / wall_elapsed_s` ≈ 整批近似 QPS（粗略指标，仅参考）
- **失败定位**：扫 `results_full` 里 `error != null` 的项

> 单位提示：JSON 里所有延迟相关字段都以**毫秒（ms）** 表示；只有 `wall_elapsed_s`
> 一个字段是**秒（s）**。画图前注意单位换算。
