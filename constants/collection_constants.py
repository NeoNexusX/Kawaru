"""文件集合（Collection）相关的常量定义"""

# 集合成员数量上限（防灌水护栏）
MAX_COLLECTION_MEMBERS = 300

# 允许收录为集合成员的文件实验类型
# 注意：与上传侧 ALLOWED_EXPERIMENT_TYPES 不同——"ibd" 是 zip 内的组成文件类型，
# 集合成员必须是完整的实验数据文件（当前即 imzML）
ALLOWED_MEMBER_EXPERIMENT_TYPES = ("imzML",)

# 集合筛选字段分组（供 build_collection_filters 生成谓词，新增筛选字段时同步维护）
# 模糊匹配（LIKE %v%）：集合自身的文本字段
COLLECTION_FUZZY_FILTER_FIELDS = ("name", "title", "journal_name")
# 精确匹配（IN）：单值枚举字段
COLLECTION_EXACT_FILTER_FIELDS = ("member_type", "collection_type")
# JSON 数组字段（成员文件取值汇总）：元素成员判断（集合内任一成员含该值），大小写不敏感
# 注意：doi / access 为标识符类字段，不参与筛选
COLLECTION_JSON_FILTER_FIELDS = (
    "organism",
    "organism_part",
    "sample_stabilization",
    "sample_growth_conditions",
    "tissue_modification",
    "polarity",
    "ionisation_source",
    "analyzer",
)
