# Kawaru

SpatialXomics 后端服务器

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

**[English](README.md)** | **[中文](README_zh.md)**

---

Kawaru 是一个基于 FastAPI 的后端服务器项目，为 SpatialXomics 提供后端支持。项目采用 Python 开发，支持文件管理、实验数据处理、用户认证等功能。

## 环境要求

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) 包管理器

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

### 3. 配置环境变量

创建 `.env` 文件并配置以下内容：

```bash
# 复制示例文件
cp .env_example .env
```

编辑 `.env` 文件，配置 SMTP 邮件服务等：

```env
# SMTP 配置
LAST_SMTP_PORT=465
LAST_SMTP_USERNAME=your_email@example.com
LAST_SMTP_PASSWORD=your_password
LAST_SMTP_FROM_EMAIL=your_email@example.com
LAST_SMTP_SERVER=smtp.example.com
```

### 4. 配置服务器参数

复制 `config_example.yaml` 为 `config.yaml`：

```bash
cp config_example.yaml config.yaml
```

编辑 `config.yaml` 文件，配置服务器基本信息。

### 5. 运行项目

```bash
uv run main.py
```

或使用 uvicorn 直接运行：

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

服务启动后，访问以下地址：
- API 文档 (Swagger): `http://localhost:8000/docs`
- API 文档 (ReDoc): `http://localhost:8000/redoc`

## 文档

详细文档请参考 `docs/` 目录：

| 文档 | 说明 |
|------|------|
| [贡献指南](docs/zh/贡献指南.md) | 协作流程与规范 |
| [部署指南](docs/zh/部署指南.md) | dev/prod 部署指南 |

## 许可证

本项目采用 GNU General Public License v3.0 开源许可证。详见 [LICENSE](LICENSE) 文件。
