# Kawaru
Backend  Server for Massflow
## 项目简介

Kawaru 是一个基于 FastAPI 的后端服务器项目,为 Massflow 提供后端支持。项目采用 Python 开发。

## 环境要求

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) 包管理器

## 快速开始开发

### 1. 克隆项目

```bash
git clone https://github.com/NeoNexusX/Kawaru.git
cd Kawaru
```

### 2. 安装依赖

项目使用 `uv` 进行依赖管理。首先安装 `uv` (如果尚未安装):

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

然后同步依赖环境:

```bash
uv sync
```

激活虚拟环境:

```bash
# macOS/Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

主要依赖包括:
- `fastapi==0.115.11` - Web 框架
- `uvicorn==0.34.0` - ASGI 服务器
- `sqlmodel==0.0.24` - ORM 数据库操作
- `pydantic==2.10.6` - 数据验证
- `PyJWT==2.10.1` - JWT 认证
- `passlib==1.7.4` - 密码加密
- `pyotp==2.9.0` - 验证码生成
- 其他依赖详见 `pyproject.toml`

### 3. 配置环境变量

创建 `.env` 文件并配置以下内容:

```bash
# 复制示例文件 (如果存在)
cp .env_example .env
```

编辑 `.env` 文件,配置以下内容:

#### SMTP 邮件服务配置 (推荐)

```env
# SMTP 配置
LAST_SMTP_PORT=465              # 或 587
LAST_SMTP_USERNAME=your_email@example.com
LAST_SMTP_PASSWORD=your_password
LAST_SMTP_FROM_EMAIL=your_email@example.com
LAST_SMTP_SERVER=smtp.example.com
```

**常用邮箱服务器配置示例:**

- **Gmail**
  ```env
  LAST_SMTP_SERVER=smtp.gmail.com
  LAST_SMTP_PORT=587
  LAST_SMTP_USERNAME=your_email@gmail.com
  LAST_SMTP_PASSWORD=your_16_char_app_password
  ```
  > 注意: Gmail 需要开启"两步验证"并生成"应用专用密码"

- **QQ 邮箱**
  ```env
  LAST_SMTP_SERVER=smtp.qq.com
  LAST_SMTP_PORT=465
  LAST_SMTP_USERNAME=your_email@qq.com
  LAST_SMTP_PASSWORD=your_authorization_code
  ```
  > 注意: QQ 邮箱需要在设置中获取授权码

#### Mailgun 服务配置 (可选)

```env
# Mailgun 配置
MAILGUN_API_KEY=your_mailgun_api_key
MAILGUN_DOMAIN=your_mailgun_domain
```

### 4. 配置服务器参数

复制 `config_example.yaml` 为 `config.yaml`:

```bash
cp config_example.yaml config.yaml
```

编辑 `config.yaml` 文件,配置服务器基本信息:

```yaml
server:
  name: bionet              # 服务名称
  port: 8000                # 服务端口
  host: 0.0.0.0            # 监听地址
  log_level: debug          # 日志级别: debug/info/warning/error/critical
  log_dir: logs             # 日志目录
  admin_email: admin@example.com  # 管理员邮箱

database:
  name: bionet              # 数据库名称
  path: database/           # 数据库文件路径
  thread: False             # SQLite 线程检查

storage:
  root: ./                  # 存储根目录
  local_root: data/         # 本地存储目录
  max_file_size: 2000000000 # 最大文件大小 (bytes)
```

配置模型定义在 `models/config_models.py` 中。

### 5. 运行项目

使用 `uv` 运行:

```bash
uv run main.py
```

或使用 uvicorn 直接运行:

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

服务启动后,访问以下地址:
- API 文档 (Swagger): `http://localhost:8000/docs`
- API 文档 (ReDoc): `http://localhost:8000/redoc`

## 项目结构说明

```bash
Kawaru/
├── api/                    # API 路由模块
│   ├── __init__.py
│   ├── auth_api.py        # 认证相关 API
│   ├── email_api.py       # 邮件发送 API
│   └── user_api.py        # 用户管理 API
├── database/              # 数据库模块
│   └── db.py             # 数据库连接和会话管理
├── models/                # 数据模型
│   ├── __init__.py
│   ├── auth.py           # 认证模型
│   ├── config_models.py  # 配置模型
│   ├── email_models.py   # 邮件模型
│   ├── tasks_models.py   # 任务模型
│   └── user_models.py    # 用户模型
├── envset/               # 环境配置模块
│   ├── __init__.py
│   ├── config.py         # 配置文件加载
│   └── envset.py         # 环境变量加载
├── docker/               # Docker 配置
│   └── DEV              # 开发环境 Dockerfile
├── logs/                 # 日志目录 (自动生成)
├── docs/                 # 文档目录
├── .env                  # 环境变量配置 (需自行创建)
├── config.yaml           # 服务器配置文件
├── logger.py             # 日志管理器
├── main.py              # 应用入口
├── pyproject.toml       # 项目配置与依赖
├── uv.lock              # 依赖锁定文件
├── LICENSE              # 许可证文件
└── README.md            # 项目说明表
├── LICENSE              # 许可证文件
└── README.md            # 项目说明
```

学习文档参考：

docs目录下文档部分
