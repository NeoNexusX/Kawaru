"""统计相关的常量定义"""

# 允许前端查询的文件分类字段白名单
ALLOWED_FILE_CLASSIFY_FIELDS = {
    "organism",
    "organism_part",
    "condition",
    "sample_stabilization",
    "tissue_modification",
    "maldi_matrix",
    "solvent",
    "polarity",
    "ionisation_source",
    "analyzer",
    "spectrum_mode",
    "storage_mode",
    "experiment_type",
}

# 允许前端查询的用户分类字段白名单
ALLOWED_USER_CLASSIFY_FIELDS = {
    "institution",
    "position",
    "research_field",
    "region",
    "identity",
}

# 访问统计 Redis Key 前缀
VISIT_STATS_KEY_PREFIX = "stats"

# 访问统计 TTL（秒）
VISIT_DAILY_TTL = 90 * 86400       # 90 天
VISIT_MONTHLY_TTL = 365 * 86400    # 365 天
