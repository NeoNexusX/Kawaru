# Kawaru

SpatialXomics 后端服务器

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

**[English](README.md)** | **[中文](README_zh.md)**

---

Kawaru 是一个基于 FastAPI 的后端服务器项目，为 SpatialXomics 提供后端支持。项目采用 Python 开发，支持文件管理、实验数据处理、用户认证等功能。

## 环境要求

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) 包管理器
- 可连接的 MariaDB 与 Redis 实例（本地安装或 Docker 均可）
- 阿里云 OSS Bucket、可签发受限 STS 凭证的 RAM 角色，以及配置中对应的函数计算（FC）函数

## macOS 额外依赖

项目依赖 `m2crypto`，它需要在 macOS 上从源码编译 C 扩展，因此需要额外安装以下工具：

```bash
# 安装 SWIG 和 OpenSSL
brew install swig openssl@3
```

安装完成后，编译 `m2crypto` 时需要设置以下环境变量（Apple Silicon 必需）：

```bash
export LDFLAGS="-L$(brew --prefix openssl@3)/lib"
export CFLAGS="-I$(brew --prefix openssl@3)/include -D__aarch64__"
export SWIG_FEATURES="-I$(brew --prefix openssl@3)/include -cpperraswarn"
```

> **说明：**
> - `-cpperraswarn` — 让 SWIG 将系统头文件中的 `#error "Unsupported architecture"` 降级为警告。
> - 以上变量仅需在运行 `uv sync` 时临时设置，无需写入 shell 配置文件。
> - Intel Mac 用户无需 `-D__aarch64__`。

## 快速开始开发

### 1. 克隆项目

```bash
git clone https://github.com/NeoNexusX/Kawaru.git
cd Kawaru
```

### 2. 安装依赖

项目使用 `uv` 进行依赖管理。首先安装 `uv`（如果尚未安装）：

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

然后同步依赖环境：

```bash
uv sync
```

激活虚拟环境：

```bash
# macOS/Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### 3. 准备 MariaDB 与 Redis

应用启动时需要连接 MariaDB 和 Redis。本地没有实例的话，可以用 Docker 起临时实例：

```bash
docker run -d --name kawaru-mariadb -p 13306:3306 \
  -e MARIADB_ROOT_PASSWORD=your_db_password \
  -e MARIADB_DATABASE=kawaru mariadb:11.4

docker run -d --name kawaru-redis -p 16379:6379 \
  redis:8.4-alpine redis-server --requirepass your_redis_password
```

连接参数（host / port / 库名）在 `config.yaml` 的 `database:` / `redis:` 段配置，密码在 `.env` 中（见下一步）。

### 4. 配置环境变量

```bash
# 复制示例文件
cp .env_example .env
```

启动时会校验核心运行变量，OSS 初始化与回调路由还依赖各自凭证。各业务 FC 的令牌在对应任务执行时才会使用，因此服务健康启动不代表每个 FC 集成都已配置完成。关键项：

| 变量 | 用途 |
|------|------|
| `DATABASE_PASSWORD` | MariaDB 密码（以 root 连接） |
| `REDIS_PASSWORD` | Redis 密码 |
| `SECRET_KEY` | 固定的 JWT 签名密钥。除临时测试栈外应显式配置；否则每次进程启动都会随机生成，重启后已有令牌会失效。 |
| `SMTP_PASSWORD` | SMTP 登录密码（服务器 / 端口 / 账号 / 发件人在 `config.yaml` 的 `email:` 段配置） |
| `OSS_ACCESS_KEY_ID` / `OSS_ACCESS_KEY_SECRET` / `OSS_ROLE_ARN` | 阿里云 OSS 凭证，以及用于签发受限上传/下载 STS 凭证的 RAM 角色 |
| `OSS_FC_PACKUP_TOKEN` / `OSS_FC_ALGORITHM_TOKEN` / `OSS_FC_TIC_TOKEN` / `OSS_FC_FEATURE_TOKEN` / `OSS_FC_RAW_CONVERT_TOKEN` / `OSS_FC_CLUSTERING_TOKEN` | worker 调用配置中各 FC 函数时使用的令牌 |
| `FC_CALLBACK_TOKEN` | 所有 `/callback/*` 请求必须通过 `X-FC-Token` 携带的统一回调令牌 |
| `CONFIG_PATH` / `REGION_PATH` | 应用配置文件和国家/地区校验数据集路径 |
| `LOG_DIR` / `LOG_LEVEL` | 日志目录与级别 |
| `BASE_URL` / `TEST_USER` / `TEST_PWD` | 仅本地直接跑 pytest 时使用（见 [贡献指南](docs/zh/贡献指南.md)） |

### 5. 配置服务器参数

```bash
cp config_example.yaml config.yaml
```

编辑 `config.yaml`，重点核对：

- `database:` / `redis:` — 指向第 3 步准备好的实例。按上面的命令，本地应使用数据库 `kawaru`、主机 `127.0.0.1`，端口分别为 `13306` / `16379`。
- `email:` — SMTP 服务器、端口、账号、发件人
- `oss:` — 必填的 bucket、region、存储目录和 FC 端点；文件存储仅支持 OSS

应用使用的 OSS 身份必须能扮演配置的 RAM 角色并完成所需对象操作，RAM 角色需支持向客户端签发的受限上传/下载策略。还需为上传 ZIP 的解包函数配置 OSS 事件触发器，并配置生命周期规则：回收未带 `persistent=true` 标签的遗留临时对象，保留已标记的源文件和解包文件。

### 6. 初始化/升级数据库结构

应用启动时会通过 Schema Guard 严格校验数据库版本。在初次运行或代码更新后，先执行数据库迁移：

```bash
uv run python -m database.migration upgrade
```

### 7. 运行项目

```bash
uv run main.py
```

开发时开启代码变更自动重载：

```bash
UVICORN_RELOAD=true uv run main.py
```

或使用 uvicorn 直接运行：

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

服务启动后，访问以下地址：
- API 文档 (Swagger): `http://localhost:8000/docs`
- API 文档 (ReDoc): `http://localhost:8000/redoc`

### 8. 后台任务（Celery）

邮件发送、文件处理、定时清理等都作为 Celery 任务执行，只启动 API 进程不会运行它们。本地开发有两种方式：

**方式 A — 内嵌模式（最简单）**：在 `config.yaml` 中设置 `celery.enable_embedded_celery: true`，`uv run main.py` 会由 API 进程托管 worker + beat 子进程，随应用一起启停。

**方式 B — 独立进程**（与生产部署一致，保持 `enable_embedded_celery: false`）：

```bash
# worker（消费 default 队列）
uv run celery -A celery_runtime.entrypoint:celery_app worker \
  -Q default --pool threads --concurrency 10 --without-gossip --loglevel INFO

# beat（定时任务调度）
uv run celery -A celery_runtime.beat_entrypoint:beat_app beat --loglevel info
```

> 生产环境 worker / beat 运行在独立容器中（分离模式），详见 [部署指南](docs/zh/部署指南.md)。

## 当前 OSS 业务流程

Kawaru 目前**仅支持 OSS 存储**。原有本地存储和后端分片上传链路已移除；客户端通过短期 STS 凭证把 ZIP 文件直传 OSS。

1. 调用 `POST /files/preflight` 提交文件元数据、MD5、`storage_type=oss`、`spectrum_mode` 与 `storage_mode`。响应返回文件的 `public_id`（16 位 base62 随机串，不返回内部 `file_id`）；`is_reuse=true` 表示同哈希的已有记录已关联给当前用户，不需要再次上传。
2. 新对象调用 `POST /files/upload` 或 `POST /files/upload_simple`，传入 `pre_public_id`（即预检返回的 `public_id`）与 `filename`。后端会检查预检记录归属和上传状态，再返回限定对象路径的 STS 凭证。ZIP 对象键为 `{oss_zip_storage_root}/{file_verify_code}/{file_id}.zip`（哈希前置可将并发写入打散到 OSS 不同分区），因此 `oss_path` 中仍含内部 `file_id`。
3. 客户端将 ZIP 直传 OSS。解包 FC 从对象键解析 `file_id`，把 imZML/ibd 写到 `{oss_origin_storage_root}/{file_verify_code}/{file_id}/` 下，并携带 `X-FC-Token` 和 `file_id` 回调 `POST /callback/oss_upload_callback`。
4. 成功回调后，后端先保存三个对象键并提交事务，再给 ZIP、imZML 和 ibd 添加 `persistent=true` 标签。`storage_mode=continuous` 分发 feature FC 任务并由 `/callback/umap_callback` 接收结果，其他模式分发 TIC 并回调 `/callback/tic_callback`；两者都保存以 `/` 结尾的预览图目录键，通常为 `images/{file_id%1000:03d}/file_{file_id}/`；客户端在文件响应返回的 `image_path` 后拼接 `preview.jpg`。
5. 算法处理（`POST /processes`）和 RAW 直转（`POST /processes/raw-convert`）以 `file_public_id` 指定源文件、以 `source_file_public_id` 返回源文件，输出 `{oss_processed_storage_root}/run_{run_id}.zarr/`。聚类（`POST /processes/{run_id}/clustering`）直接更新同一个 Zarr，不再创建独立聚类输出路径；基础 run 完成后，`GET /processes/{run_id}/zarr` 返回只读 STS 凭证。
6. `GET /files/{public_id}/download_raw` 返回 imZML 与 ibd 的签名 URL。`GET /files/{public_id}/download` 启动或查询异步 ZIP 打包；打包对象键为 `{oss_packup_zip_storage_root}/{file_id}.zip`，在 `/callback/oss_packup_callback` 到达前可能保持 `<PACKING>`。

配置中的对象根目录为 `oss_zip_storage_root`、`oss_packup_zip_storage_root`、`oss_origin_storage_root` 和 `oss_processed_storage_root`。Celery beat 会调度过期预检清理、下载清理、失败实验清理、超时处理 run 标记和心跳任务；完全未进入回调、因而从未获得 `persistent=true` 标签的对象仍由 OSS 生命周期规则回收。

## 主要 API 范围

| 范围 | 主要路由 |
|------|----------|
| 文件上传与访问 | `/files/preflight`、`/files/upload`、`/files/upload_simple`、`/files/{public_id}/metadata`、`/files/{public_id}/download_raw`、`/files/{public_id}/download_raw_noauth`、`/files/{public_id}/download` |
| 处理任务 | `POST /processes`、`POST /processes/raw-convert`、`POST /processes/{run_id}/clustering`、`GET /processes/{run_id}/zarr`、`GET /processes/mine`、`DELETE /processes/{run_id}` |
| FC 回调 | `/callback/oss_upload_callback`、`/callback/algorithm_callback`、`/callback/tic_callback`、`/callback/umap_callback`、`/callback/clustering_callback`、`/callback/oss_packup_callback` |

## 项目结构

分层调用方向：`routers/` → `api/` → `api_services/` / `api_helpers/` → `database/` / `models/`；后台任务在 `background/`。

| 目录 | 职责 |
|------|------|
| `routers/` | FastAPI 路由定义 |
| `api/` | 接口处理逻辑 |
| `api_services/` | 业务服务层 |
| `api_helpers/` | api 层共享辅助函数 |
| `background/` | Celery 任务定义 |
| `celery_runtime/` | Celery 应用构建、worker / beat 入口、内嵌启动器 |
| `boot/` | 启动引导：env / config / logging / celery / OSS 初始化 |
| `models/` | Pydantic / SQLModel 模型 |
| `database/` | 数据库引擎、建表、Redis 客户端 |
| `storage/` | OSS 存储实现 |
| `constants/`、`exceptions/`、`tools/` | 常量、异常定义、日志等工具 |
| `docker/` | Dockerfile 与 dev / prod / test compose 栈 |
| `tests/` | 端到端 pytest 套件（需连接运行中的服务，见贡献指南） |
| `.github/` | CI / CD GitHub Actions 工作流 |
| `docs/` | 贡献指南与部署指南 |

顶层文件：

| 文件 | 用途 |
|------|------|
| `main.py` | 应用入口：FastAPI 实例、路由注册、生命周期（建表、创建管理员、内嵌 celery 启停） |
| `config_example.yaml` | 应用配置模板，复制为 `config.yaml` 后使用 |
| `.env_example` | 环境变量模板，复制为 `.env` 后使用 |
| `secrets.example.yaml` | CI 与 dev/prod CD 使用的 GitHub Actions Secrets 配置清单 |
| `pyproject.toml` / `uv.lock` | 依赖声明与锁定（uv） |
| `pytest.ini` | pytest 配置与 marker 定义 |
| `.pylintrc` | pylint 配置（仅本地使用，不在 CI 中；行宽 150） |
| `.python-version` | Python 版本固定（3.12） |
| `.gitignore` / `.dockerignore` | git / Docker 构建忽略规则 |
| `LICENSE` | GPLv3 许可证 |

## 文档

详细文档请参考 `docs/` 目录：

| 文档 | 说明 |
|------|------|
| [贡献指南](docs/zh/贡献指南.md) | 协作流程与规范 |
| [部署指南](docs/zh/部署指南.md) | dev/prod 部署指南 |
| [核心业务 API 架构：文件上传与 OSS 生命周期](docs/zh/核心业务API架构-文件上传.md) | 文件上传分层、API 契约、Mermaid 交互流程、状态机与 OSS 生命周期 |
| [OSS 上传回调流程](docs/zh/上传回调流程.md) | 上传回调事务与失败回收说明 |

## 许可证

本项目采用 GNU General Public License v3.0 开源许可证。详见 [LICENSE](LICENSE) 文件。
