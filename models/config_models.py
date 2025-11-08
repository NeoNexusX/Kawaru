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


# main model
class Config(BaseModel):
    server: ServerSettings
    database: DatabaseSettings