from pydantic import BaseModel, Field


class ServerSettings(BaseModel):
    """服务器配置模型"""
    name: str
    port: int
    host: str


class TokenSettings(BaseModel):
    """令牌配置模型"""
    token_expire_minutes: int
    login_expire_minutes: int
    algorithm: str


class DatabaseSettings(BaseModel):
    """数据库配置模型"""
    db_username: str
    db_name: str
    db_type: str
    pool_size: int
    pool_recycle: int
    max_overflow: int
    echo_pool: bool
    host: str
    port: int


class RedisSettings(BaseModel):
    """Redis服务器配置模型"""
    username: str
    host: str
    port: int
    max_connection: int
    default_db_index: int
    celery_broker_db_index: int
    celery_backend_db_index: int
    celery_redbeat_db_index: int


class CelerySettings(BaseModel):
    """Celery配置模型"""
    worker_concurrency: int
    worker_log_level: str
    worker_log_file: str
    worker_disable_rate_limits: bool
    worker_pool: str


class EmailSettings(BaseModel):
    """邮件配置模型"""
    # Support for MailGun
    mailgun_domain: str
    # Support for SMTP
    smtp_port: int
    smtp_username: str
    smtp_from_email: str
    smtp_server: str
    totp_window: int


class StorageSettings(BaseModel):
    """文件存储配置模型"""
    root: str
    local_root: str
    max_file_size_gb: int
    chunk_buffer_size_mb: int
    max_chunks_per_upload: int
    allowed_storage_types: list[str]
    allowed_file_types: list[str]
    allowed_experiment_types: list[str]
    max_path_length: int
    max_filename_length: int
    max_file_type_length: int
    max_downloads_per_user: int
    max_files_per_user: int
    preflight_ttl_minutes: int
    cleanup_preflights_interval_minutes: int
    cleanup_download_zip_interval_minutes: int
    download_zip_retention_days: int
    chunk_ttl_minutes: int


class OSSSettings(BaseModel):
    """OSS配置模型"""
    region_id: str | None  = None
    bucket_name: str | None = None
    oss_upload_sts_token_expire_minutes: int | None = Field(default=None, ge=15, le=60)
    oss_download_sts_token_expire_minutes: int | None = Field(default=None, ge=15, le=60)
    oss_zip_storage_root: str | None = None
    oss_experiment_storage_root: str | None = None
    oss_default_timeout_seconds: int | None = Field(default=20, ge=5, description="默认超时时间，单位秒")
    fc_logstore_name: str | None = Field(default=None, description="函数计算SLS日志存储名称")
    fc_log_project_name: str | None = Field(default=None, description="函数计算SLS日志项目名称")
    fc_unzip_timeout_seconds: int | None = Field(default=20, description="函数调用超时时间，单位秒")
    fc_packup_endpoint: str | None = Field(default=None, description="函数计算自动打包公网地址")
    fc_packup_timeout_seconds: int | None = Field(default=20, description="函数计算自动打包调用时超时时间，单位秒")


class UserSettings(BaseModel):
    """用户信息配置模型"""
    allowed_position_types: list[str]
    max_identity_length: int
    max_username_length: int
    max_useremail_length: int
    max_institution_length: int
    max_position_length: int
    max_research_field_length: int
    max_region_length: int
    max_orcid_length: int
    max_homepage_length: int


class SampleInfoSettings(BaseModel):
    """样品信息配置模型"""
    max_experiment_name_length: int
    max_experiment_type_length: int
    max_organism_length: int
    max_organism_part_length: int
    max_condition_length: int
    max_sample_growth_conditions_length: int
    max_sample_stabilization_length: int
    max_tissue_modification_length: int
    max_maldi_matrix_length: int
    max_maldi_matrix_application_length: int
    max_solvent_length: int


# Initial admin user's information 初始管理员信息
class AdminInfo(BaseModel):
    """初始管理员信息配置模型"""
    username: str
    institution: str
    position: str
    research_field: str
    region: str
    orcid: str
    homepage: str
    email: str


class Config(BaseModel):
    """全局配置模型"""
    server: ServerSettings
    email: EmailSettings
    database: DatabaseSettings
    redis: RedisSettings
    celery: CelerySettings
    storage: StorageSettings
    oss: OSSSettings
    user: UserSettings
    token: TokenSettings
    admin_info: AdminInfo
    sample_info: SampleInfoSettings
