# Kawaru 开发环境配置文档

## 项目简介

Kawaru 是一个基于 FastAPI 的后端服务器项目,为 Massflow 提供后端支持。项目采用 Python 开发。

## 环境要求

- Python 3.12+
- pip 包管理器

## 快速开始

### 1. 克隆项目

```bash
git clone https://github.com/NeoNexusX/Kawaru.git
cd Kawaru
```

### 2. 安装依赖

项目依赖已在 `requirements.txt` 中定义,执行以下命令安装:

```bash
pip install -r requirements.txt
```

主要依赖包括:
- `fastapi==0.115.11` - Web 框架
- `uvicorn==0.34.0` - ASGI 服务器
- `sqlmodel==0.0.24` - ORM 数据库操作
- `pydantic==2.10.6` - 数据验证
- `PyJWT==2.10.1` - JWT 认证
- `passlib==1.7.4` - 密码加密
- `pyotp==2.9.0` - 验证码生成
- 其他依赖详见 `requirements.txt`

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
```

配置模型定义在 `models/config_models.py` 中。


### 5. 运行项目

```bash
python main.py
```

或使用 uvicorn 直接运行:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

服务启动后,访问以下地址:
- API 文档 (Swagger): `http://localhost:8000/docs`
- API 文档 (ReDoc): `http://localhost:8000/redoc`

## 项目结构说明

```
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
├── .gitignore            # Git 忽略文件
├── .pylintrc             # Pylint 配置
├── config.yaml           # 服务器配置文件
├── logger.py             # 日志管理器
├── main.py              # 应用入口
├── requirements.txt      # 依赖列表
├── LICENSE              # 许可证文件
└── README.md            # 项目说明
```

## 核心功能模块

### 1. 日志系统

日志系统在 `logger.py` 中实现,支持:
- 控制台彩色输出
- 文件日志记录 (保存在 `logs/` 目录)
- 多级别日志 (DEBUG/INFO/WARNING/ERROR/CRITICAL)
- 自动日志轮转 (10MB/文件,保留 5 个备份)

使用示例:
```python
from logger import get_logger

logger = get_logger("module_name")
logger.info("Information message")
logger.error("Error message")
logger.debug("Debug message")
```

### 2. 用户认证

认证系统基于 JWT Token (`models/auth.py`):
- 密码加密存储 (bcrypt 算法)
- OAuth2 密码流程
- Token 过期时间: 30 分钟
- 支持用户注册、登录、信息更新

认证流程:
1. 用户提供用户名和密码
2. 服务器验证并返回 JWT Token
3. 后续请求在 Header 中携带 Token: `Authorization: Bearer <token>`

### 3. 邮件服务

支持两种邮件发送方式 (`api/email_api.py`):

#### SMTP 方式 (推荐)
- 端点: `POST /emailsmtprequest`
- 支持任意 SMTP 服务器
- 配置灵活,适用于大多数场景

#### Mailgun API 方式
- 端点: `POST /emailrequest`
- 需要 Mailgun 账户
- 适用于大规模邮件发送

邮件验证码功能:
- 使用 TOTP 算法生成 6 位数字验证码
- 验证码有效期: 5 分钟
- 自动发送 HTML 格式邮件

### 4. 数据库

数据库模块 (`database/db.py`) 特点:
- 使用 SQLModel ORM (基于 SQLAlchemy)
- SQLite 数据库 (适合中小型应用)
- 自动创建数据表
- 依赖注入式会话管理

数据库会话使用示例:
```python
from fastapi import Depends
from sqlmodel import Session
from database.db import get_session

@app.get("/example")
def example_endpoint(session: Session = Depends(get_session)):
    # 使用 session 进行数据库操作
    pass
```

## API 端点文档

主要 API 端点定义在 `main.py`:

### 认证相关
- `POST /login` - 用户登录
  - 请求体: `username`, `password`
  - 返回: JWT Token

### 用户管理
- `GET /user` - 获取当前用户信息 (需认证)
- `POST /user_signup` - 用户注册
- `POST /user_change` - 更新用户信息 (需认证)
- `DELETE /user_delete` - 删除用户 (需认证)

### 邮件服务
- `POST /emailrequest` - 发送邮件 (Mailgun)
- `POST /emailsmtprequest` - 发送邮件 (SMTP)

详细 API 文档请访问 `/docs` 端点查看 Swagger UI。

## 开发注意事项

### 1. 代码规范

项目使用 Pylint 进行代码检查,配置文件为 `.pylintrc`:
- 最大行长度: 130 字符
- 跳过模块文档字符串检查
- 最小公共方法数: 1

运行代码检查:
```bash
pylint api/ models/ database/ envset/
```


### 2. 管理员账户

应用首次启动时会自动创建管理员账户 (`api/user_api.py` 中的 `create_admin_user()` 函数):
- 用户名: 配置文件中的 `server.name`
- 密码: 随机生成的 8 位数字
- 密码会在日志中输出,请及时保存并修改

查看管理员密码:
```bash
# 查看最新日志
tail -n 50 logs/app.log | grep "Admin password"
```

### 3. 开发建议

- 使用 `--reload` 模式开发,代码修改后自动重启:
  ```bash
  uvicorn main:app --reload
  ```

- 开启 DEBUG 日志级别便于调试:
  ```yaml
  # config.yaml
  server:
    log_level: debug
  ```

## 贡献指南

欢迎提交 Pull Request 或 Issue!

提交代码前请确保:
1. 代码符合 PEP 8 规范
2. 通过 Pylint 检查
3. 添加必要的注释
4. 更新相关文档

## 许可证

本项目采用 GNU General Public License v3.0 开源许可证。

详见 [LICENSE](../LICENSE) 文件。

## 技术支持

- 问题反馈: 提交 GitHub Issue
- 文档更新: 提交 Pull Request
- 日志查看: `logs/app.log`

## 更新日志

项目使用 Git 管理版本,查看提交历史:
```bash
git log --oneline
```

---

**最后更新**: 2025-11-09
**文档版本**: 1.0.0
