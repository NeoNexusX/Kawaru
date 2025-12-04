# Kawaru Naming Conventions and Standards

## General Principles
- Use "clear, readable, and unambiguous" naming consistently; avoid ambiguity and excessive abbreviation.
- Maintain consistent style: `snake_case` for files/functions/variables, `PascalCase` for classes, `UPPER_CASE` for constants.
- Follow PEP 8 standards for code style.
- Mandatory use of Type Hints.

## Files and Directories
- Use lowercase words for directories, separated by underscores if necessary: `api/`, `models/`, `database/`.
- Use `snake_case` for Python files:
  - API route files: `auth_api.py`, `user_api.py`.
  - Data model files: `user_models.py`, `auth.py`.
  - Configuration files: `config.py`.
  - Core entry point: `main.py`.

## Modules and Packages
- Module imports should remain clear to avoid circular imports.
- Relative imports should only be used within a package; absolute imports are recommended for cross-module usage.

## Class Naming (PascalCase)
- **SQLModel/Pydantic Models**:
  - Base models: `UserBase` (usually containing shared fields).
  - Database models: `UserInDB` (containing database-specific fields, e.g., `table=True`).
  - Public/Response models: `UserPublic` (for API responses, filtering sensitive fields).
  - Creation/Update models: `UserCreate`, `UserUpdate`.
- **Ordinary Classes**: Use nouns or noun phrases, e.g., `Config`.

## Methods and Functions (snake_case)
- **API Handler Functions**: Usually serve as route handlers; names should clearly express the action and resource, e.g., `create_user`, `get_current_user`, `login_for_access_token`.
- **Utility Functions**: Start with a verb, e.g., `hashed_password`, `verify_password`, `create_access_token`.
- **Dependency Injection Functions**: Usually start with `get_`, e.g., `get_db`, `get_current_active_user`.

## Variable Naming (snake_case)
- Ordinary variables: `user`, `db`, `token`, `hashed_pwd`.
- Boolean variables: Recommended to use `is_`, `has_` prefixes, e.g., `is_active` (but nouns like `active` can be used directly in Pydantic model fields).
- Exception instances: `credentials_exception`, `token_expired_exception`.

## Constants (UPPER_CASE)
- Use full uppercase with underscores for global configurations, environment variables, and fixed values.
- Examples: `SECRET_KEY`, `ALGORITHM`, `PWD_CONTEXT`.

## Type Hinting
- All function parameters and return values must have type annotations.
- Use the Python standard library `typing` or new syntax in Python 3.10+ (e.g., `|` instead of `Union`).
- Example:
  ```python
  def verify_password(plain_pwd: str, hashed_pwd: str) -> bool:
      ...
  
  def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
      ...
  ```

## Database and Model Standards
- **SQLModel**:
  - Use `Field()` for field definitions.
  - Set default values to `None` or use `default_factory` for optional fields.
  - Database table models must set `table=True`.
  - Sensitive fields (like passwords) should not be returned in response models like `UserPublic`.

## Naming Anti-patterns (Avoid)
- Mixed styles: e.g., `userData` (should be `user_data`).
- Meaningless abbreviations: e.g., `u`, `t` (unless in short loops).
- Using verbs for class names: e.g., `CreateUser` (if it's an action class, it should be `UserCreator`; if it's a data model, it should be `UserCreate`).

## Implementation Suggestions
- When adding a new API module, create `*_api.py` in the `api/` directory.
- When adding a new data model, create `*_models.py` in the `models/` directory.
- Keep `main.py` concise, mainly for application initialization and route registration.
