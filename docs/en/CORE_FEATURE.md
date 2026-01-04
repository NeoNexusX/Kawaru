# Kawaru Development Documentation

**Last Updated**: 2025-12-04

**Document Version**: 1.1.011-09

**Document Version**: 1.0.0

## Core Function Modules

### 1. Logging System

The logging system is implemented in `logger.py`, supporting:
- Console color output
- File logging (saved in `logs/` directory)
- Multi-level logging (DEBUG/INFO/WARNING/ERROR/CRITICAL)
- Automatic log rotation (10MB/file, keeping 5 backups)

Usage Example:
```python
from logger import get_logger

logger = get_logger("module_name")
logger.info("Information message")
logger.error("Error message")
logger.debug("Debug message")
```

### 2. User Authentication

The authentication system is based on JWT Token (`models/auth.py`):
- Password encryption storage (bcrypt algorithm)
- OAuth2 password flow
- Token expiration time: 30 minutes
- Supports user registration, login, and information update

Authentication Flow:
1. User provides username and password
2. Server verifies and returns JWT Token
3. Subsequent requests carry Token in Header: `Authorization: Bearer <token>`

### 3. Email Service

Supports two email sending methods (`api/email_api.py`):

#### SMTP Method (Recommended)
- Endpoint: `POST /emailsmtprequest`
- Supports any SMTP server
- Flexible configuration, suitable for most scenarios

#### Mailgun API Method
- Endpoint: `POST /emailrequest`
- Requires Mailgun account
- Suitable for large-scale email sending

Email Verification Code Feature:
- Uses TOTP algorithm to generate 6-digit verification codes
- Verification code validity: 5 minutes
- Automatically sends HTML format emails

### 4. Database

Database module (`database/db.py`) features:
- Uses SQLModel ORM (based on SQLAlchemy)
- MariaDB with the asyncmy async driver (satisfies distributed requirements)
- Automatic table creation
- Dependency injection session management

Database Session Usage Example:
- Synchronous version
```python
from fastapi import Depends
from sqlmodel import Session
from database.db import get_sync_session

@app.get("/example")
def example_endpoint(session: Session = Depends(get_sync_session)):
    # use session for database operations
    pass
```
- Asynchronous version
```python
from fastapi import Depends
from sqlmodel import Session
from database.db import get_session

@app.get("/example")
async def example_endpoint(session: Session = Depends(get_session)):
    # use await session for database operations
    pass
```


## API Endpoint Documentation

Main API endpoints are defined in `main.py`:

### Authentication Related
- `POST /login` - User Login
  - Request Body: `username`, `password`
  - Response: JWT Token

### User Management
- `GET /user` - Get current user info (Requires Authentication)
- `POST /user_signup` - User Registration
- `POST /user_change` - Update user info (Requires Authentication)
- `DELETE /user_delete` - Delete user (Requires Authentication)

### Email Service
- `POST /emailrequest` - Send Email (Mailgun)
- `POST /emailsmtprequest` - Send Email (SMTP)

For detailed API documentation, please visit the `/docs` endpoint to view Swagger UI.

## Development Notes

### 1. Code Standards

The project uses Pylint for code checking, configuration file is `.pylintrc`:
- Max line length: 130 characters

Run code check:
```bash
uv run pylint api/ models/ database/ envset/
```
Code check:
```bash
pylint api/ models/ database/ envset/
```

### 2. Administrator Account

The application automatically creates an administrator account when started for the first time (`create_admin_user()` function in `api/user_api.py`):
- Username: `server.name` in configuration file
- Password: Randomly generated 8-digit number
- The password will be output in the log, please save and change it in time

View Administrator Password:
```bash
# View latest logs
tail -n 50 logs/app.log | grep "Admin password"
```

### 3. Development Suggestions

- Use `--reload` mode for development, automatically restart after code modification:
  ```bash
  uvicorn main:app --reload
  ```

- Enable DEBUG log level for easier debugging:
  ```yaml
  # config.yaml
  server:
    log_level: debug
  ```

## License

This project is licensed under the GNU General Public License v3.0.

See [LICENSE](../LICENSE) file for details.

## Technical Support

- Issue Feedback: Submit GitHub Issue
- Documentation Update: Submit Pull Request
- Log Viewing: `logs/app.log`

## Changelog

The project uses Git for version management, view commit history:
```bash
git log --oneline
```
