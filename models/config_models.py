from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ServerSettings(BaseModel):
    """服务器配置模型"""
    model_config = ConfigDict(extra="forbid")
    name: str = "SpatialXomics"  # 项目名，用于邮件文案等对外展示
    port: int
    host: str
    # 对外 API 公网前缀，用于拼接中转下载 URL（如 https://api.example.com）；
    # 未配置时回退用请求的 request.base_url 推导（要求反向代理透传 Host）
    public_base_url: str | None = None


class TokenSettings(BaseModel):
    """令牌配置模型"""
    model_config = ConfigDict(extra="forbid")
    token_expire_minutes: int
    login_expire_minutes: int
    algorithm: str


class DatabaseSettings(BaseModel):
    """数据库配置模型"""
    model_config = ConfigDict(extra="forbid")
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
    model_config = ConfigDict(extra="forbid")
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
    model_config = ConfigDict(extra="forbid")
    enable_embedded_celery: bool = True
    worker_concurrency: int
    worker_log_level: str
    worker_log_file: str
    worker_disable_rate_limits: bool
    worker_pool: str
    large_file_poll_time: int


class EmailSettings(BaseModel):
    """邮件配置模型"""
    model_config = ConfigDict(extra="forbid")
    # Support for SMTP
    smtp_port: int
    smtp_username: str
    smtp_from_email: str
    smtp_server: str
    # 已废弃：邮箱验证码改为按用途 + 邮箱存 Redis（见 api_services/user_service.py），该项不再生效；
    # 仅为兼容仍包含此项的旧配置文件而保留（extra="forbid" 下直接删除字段会让旧配置校验失败）
    totp_window: int | None = None
    reset_password_max_attempts: int = Field(
        default=5,
        ge=1,
        description="单个邮箱验证码（注册/改邮箱/重置密码）允许的最大校验失败次数，超过则作废该验证码以防暴力破解",
    )
    email_send_cooldown_seconds: int = Field(
        default=60,
        ge=1,
        description="同一邮箱两次发送验证码邮件之间的最小间隔（秒），不区分用途，防邮件轰炸",
    )


class StorageSettings(BaseModel):
    """文件存储配置模型"""
    model_config = ConfigDict(extra="forbid")
    max_chunks_per_upload: int
    allowed_storage_types: list[Literal["oss"]] = Field(min_length=1)
    allowed_file_types: list[str]
    allowed_experiment_types: list[str]
    max_path_length: int
    max_filename_length: int
    max_file_type_length: int
    cleanup_preflights_interval_minutes: int
    cleanup_download_zip_interval_minutes: int
    cleanup_failed_experiments_interval_minutes: int
    preflight_ttl_minutes: int = Field(description="预检记录 TTL 下限（分钟）；与带宽公式计算值取较大者")
    max_file_size_gb: int | None = Field(default=None, gt=0, description="允许的最大单文件大小（GB），用于预检校验和 TTL 公式；不配置则不启用")
    min_upload_bandwidth_mbps: float | None = Field(default=None, gt=0, description="最低上传带宽假设（Mbps，运营下限），用于 TTL 公式；不配置则不启用公式")
    upload_ttl_safety_factor: float = Field(default=1.5, gt=0, description="预检 TTL 安全余量系数（乘式），随上传耗时等比放大")
    download_zip_ttl_minutes: int
    signed_url_ttl_minutes: int = Field(
        ge=6,
        description="OSS 签名 URL 过期时间（分钟），必须 >= 6，以保证 Redis 缓存能比签名 URL 早 5 分钟过期",
    )
    failed_experiment_ttl_minutes: int
    stale_process_run_timeout_minutes: int = Field(default=60, description="算法/转换 run 超过此时间无回调则标记失败")
    stale_clustering_run_timeout_minutes: int = Field(default=120, description="聚类 run 超过此时间无回调则标记失败")
    cleanup_stale_process_runs_interval_minutes: int = Field(default=15, description="清理僵尸 process_run 的定时间隔")


class OSSSettings(BaseModel):
    """OSS配置模型"""
    model_config = ConfigDict(extra="forbid")
    region_id: str | None  = None
    bucket_name: str | None = None
    # 上限需与阿里云 RAM 角色「最大会话时长」设置对齐（AssumeRole 不能超过该值）
    # 上传：480 分钟 = 8 小时（慢速大文件上传留足余量；RAM 角色最大会话时长上限 12 小时）
    oss_upload_sts_token_expire_minutes: int | None = Field(default=None, ge=15, le=480)
    # 下载（zarr 只读）：240 分钟 = 4 小时
    oss_download_sts_token_expire_minutes: int | None = Field(default=None, ge=15, le=240)
    oss_zip_storage_root: str | None = None
    oss_packup_zip_storage_root: str | None = None
    oss_origin_storage_root: str | None = None
    oss_processed_storage_root: str | None = Field(default=None, description="OSS算法处理结果存储根目录")
    oss_default_timeout_seconds: int | None = Field(default=20, ge=5, description="默认超时时间，单位秒")
    oss_delete_object_timeout_seconds: int | None = Field(default=30, ge=5, description="OSS 删除单个对象轮询超时时间，单位秒")
    oss_delete_prefix_timeout_seconds: int | None = Field(default=300, ge=5, description="OSS 删除目录前缀轮询超时时间，单位秒")
    fc_packup_endpoint: str | None = Field(default=None, description="函数计算自动打包HTTP触发地址")
    fc_packup_timeout_seconds: int | None = Field(default=20, description="函数计算自动打包调用时超时时间，单位秒")
    fc_algorithm_endpoint: str | None = Field(default=None, description="FC算法处理函数HTTP触发地址")
    fc_algorithm_timeout_seconds: int | None = Field(default=60, description="FC算法处理调用超时时间，单位秒")
    fc_tic_endpoint: str | None = Field(default=None, description="FC TIC 处理函数HTTP触发地址")
    fc_tic_timeout_seconds: int | None = Field(default=60, description="FC TIC 处理调用超时时间，单位秒")
    fc_feature_endpoint: str | None = Field(default=None, description="FC UMAP预览图函数HTTP触发地址")
    fc_feature_timeout_seconds: int | None = Field(default=60, description="FC UMAP预览图调用超时时间，单位秒")
    fc_raw_convert_endpoint: str | None = Field(default=None, description="FC raw_convert 直转函数HTTP触发地址")
    fc_raw_convert_timeout_seconds: int | None = Field(default=120, description="FC raw_convert 调用超时时间，单位秒")
    fc_clustering_endpoint: str | None = Field(default=None, description="FC 聚类分析函数HTTP触发地址")
    fc_clustering_timeout_seconds: int | None = Field(default=180, description="FC 聚类分析调用超时时间，单位秒")
    # 中转下载（relay）专用内网 endpoint：仅当服务器与 OSS 同地域时可达，OSS→服务器这跳免费。
    # 未配置时中转下载不可用（create_internal_oss_client 返回 None，relay 接口报错）。
    oss_internal_endpoint: str | None = Field(default=None, description="OSS 内网 endpoint（中转下载专用，如 https://oss-cn-hangzhou-internal.aliyuncs.com）")
    relay_max_concurrency: int = Field(default=5, ge=1, description="中转下载最大并发数（保护服务器带宽）")
    relay_acquire_timeout_seconds: float = Field(default=10, gt=0, description="中转下载排队等待上限（秒），超时返回 429")
    relay_idle_timeout_seconds: float = Field(default=30, gt=0, description="中转下载空闲超时（秒）：客户端 X 秒不消费数据则中断传输")


class UserSettings(BaseModel):
    """用户信息配置模型"""
    model_config = ConfigDict(extra="forbid")
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
    model_config = ConfigDict(extra="forbid")
    max_experiment_name_length: int
    max_experiment_type_length: int
    max_instrument_types_length: int
    max_organism_length: int
    max_organism_part_length: int
    max_condition_length: int
    max_sample_growth_conditions_length: int
    max_sample_stabilization_length: int
    max_tissue_modification_length: int
    max_maldi_matrix_length: int
    max_maldi_matrix_application_length: int
    max_solvent_length: int
    max_polarity_length: int
    max_ionisation_source_length: int
    max_analyzer_length: int


# Initial admin user's information 初始管理员信息
class AdminInfo(BaseModel):
    """初始管理员信息配置模型"""
    model_config = ConfigDict(extra="forbid")
    username: str
    institution: str
    position: str
    research_field: str
    region: str
    orcid: str
    homepage: str
    email: str
    max_total_file_size: int
    max_file_count: int
    max_processing_size: int
    max_download_count: int


class CORSSettings(BaseModel):
    """CORS 配置模型"""
    model_config = ConfigDict(extra="forbid")
    allow_origins: list[str]
    allow_credentials: bool
    allow_methods: list[str]
    allow_headers: list[str]
    expose_headers: list[str]


class Config(BaseModel):
    """全局配置模型"""
    model_config = ConfigDict(extra="forbid")
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
    cors: CORSSettings
