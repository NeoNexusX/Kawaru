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
  -e MARIADB_ROOT_PASSWORD=your_db_password mariadb:11.4

docker run -d --name kawaru-redis -p 16379:6379 \
  redis:8.4-alpine redis-server --requirepass your_redis_password
```

连接参数（host / port / 库名）在 `config.yaml` 的 `database:` / `redis:` 段配置，密码在 `.env` 中（见下一步）。

### 4. 配置环境变量

```bash
# 复制示例文件
cp .env_example .env
```

启动时会校验必需环境变量（缺失会直接报错退出）。关键项：

| 变量 | 用途 |
|------|------|
| `DATABASE_PASSWORD` | MariaDB 密码（以 root 连接） |
| `REDIS_PASSWORD` | Redis 密码 |
| `SMTP_PASSWORD` | SMTP 登录密码（服务器 / 端口 / 账号 / 发件人在 `config.yaml` 的 `email:` 段配置） |
| `OSS_ACCESS_KEY_ID` / `OSS_ACCESS_KEY_SECRET` / `OSS_ROLE_ARN` | 阿里云 OSS 凭证（`storage.allowed_storage_types` 含 `oss` 时必需） |
| `OSS_FC_*_TOKEN` / `FC_CALLBACK_TOKEN` | 函数计算（FC）触发与回调令牌 |
| `CONFIG_PATH` | 应用配置文件路径（默认 `config.yaml`） |
| `LOG_DIR` / `LOG_LEVEL` | 日志目录与级别 |
| `BASE_URL` / `TEST_USER` / `TEST_PWD` | 仅本地直接跑 pytest 时使用（见 [贡献指南](docs/zh/贡献指南.md)） |

### 5. 配置服务器参数

```bash
cp config_example.yaml config.yaml
```

编辑 `config.yaml`，重点核对：

- `database:` / `redis:` — 指向第 3 步准备好的实例
- `email:` — SMTP 服务器、端口、账号、发件人
- `oss:` — bucket、region、FC 端点；不使用 OSS 时把 `storage.allowed_storage_types` 改为 `["local"]`

### 6. 运行项目

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

### 7. 后台任务（Celery）

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
| `storage_backend/` | 本地 / OSS 存储抽象 |
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
| `secrets.example.yaml` | GitHub Secrets 配置清单（fork 仓库跑 CI 前对照填写） |
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

## 许可证

本项目采用 GNU General Public License v3.0 开源许可证。详见 [LICENSE](LICENSE) 文件。
