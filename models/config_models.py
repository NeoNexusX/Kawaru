from pydantic import BaseModel, ValidationError


class ServerSettings(BaseModel):
    name: str
    port: int
    host: str
    log_level: str
    log_dir: str
    admin_email: str


# Database settings
class DatabaseSettings(BaseModel):
    name: str
    path: str
    thread: bool


# file storage model
class StorageSettings(BaseModel):
    root: str
    local_root: str
    max_file_size: int
    chunk_buffer_size: int
    allowed_storage_types: list[str]
    allowed_file_types: list[str]
    max_filename_length: int


class Config(BaseModel):
    server: ServerSettings
    database: DatabaseSettings
    storage: StorageSettings