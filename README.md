# Kawaru

Backend Server for SpatialXomics

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

**[English](README.md)** | **[中文](README_zh.md)**

---

Kawaru is a FastAPI-based backend server project that provides backend support for SpatialXomics. Built with Python, it supports file management, experiment data processing, user authentication, and more.

## Requirements

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager

## macOS Additional Dependencies

The project depends on `m2crypto`, which requires compiling C extensions from source on macOS. Additional tools are needed:

```bash
# Install SWIG and OpenSSL
brew install swig openssl@3
```

After installation, set the following environment variables when compiling `m2crypto` (required for Apple Silicon):

```bash
export LDFLAGS="-L$(brew --prefix openssl@3)/lib"
export CFLAGS="-I$(brew --prefix openssl@3)/include -D__aarch64__"
export SWIG_FEATURES="-I$(brew --prefix openssl@3)/include -cpperraswarn"
```

> **Notes:**
> - `-cpperraswarn` — Allows SWIG to downgrade `#error "Unsupported architecture"` in system headers to a warning.
> - These variables only need to be set temporarily when running `uv sync`; no need to add them to shell config files.
> - Intel Mac users do not need `-D__aarch64__`.

## Quick Start

### 1. Clone the Project

```bash
git clone https://github.com/NeoNexusX/Kawaru.git
cd Kawaru
```

### 2. Install Dependencies

The project uses `uv` for dependency management. First install `uv` (if not already installed):

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Then sync the dependency environment:

```bash
uv sync
```

Activate the virtual environment:

```bash
# macOS/Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### 3. Configure Environment Variables

Create a `.env` file with the following content:

```bash
# Copy the example file
cp .env_example .env
```

Edit the `.env` file to configure SMTP email service, etc.:

```env
# SMTP Configuration
LAST_SMTP_PORT=465
LAST_SMTP_USERNAME=your_email@example.com
LAST_SMTP_PASSWORD=your_password
LAST_SMTP_FROM_EMAIL=your_email@example.com
LAST_SMTP_SERVER=smtp.example.com
```

### 4. Configure Server Parameters

Copy `config_example.yaml` to `config.yaml`:

```bash
cp config_example.yaml config.yaml
```

Edit `config.yaml` to configure server settings.

### 5. Run the Project

```bash
uv run main.py
```

Or run directly with uvicorn:

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

After the server starts, visit:
- API Documentation (Swagger): `http://localhost:8000/docs`
- API Documentation (ReDoc): `http://localhost:8000/redoc`

## Documentation

For detailed documentation, refer to the `docs/` directory:

| Document | Description |
|----------|-------------|
| [Contributing](docs/en/CONTRIBUTING.md) | Collaboration workflow |
| [Deployment](docs/en/DEPLOYMENT.md) | dev/prod deployment guide |

## License

This project is licensed under the GNU General Public License v3.0. See [LICENSE](LICENSE) for details.
