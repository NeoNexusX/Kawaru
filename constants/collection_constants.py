"""文件集合（Collection）相关的常量定义"""

# 集合成员数量上限（防灌水护栏）
MAX_COLLECTION_MEMBERS = 300

# 允许收录为集合成员的文件实验类型
# 注意：与上传侧 ALLOWED_EXPERIMENT_TYPES 不同——"ibd" 是 zip 内的组成文件类型，
# 集合成员必须是完整的实验数据文件（当前即 imzML）
ALLOWED_MEMBER_EXPERIMENT_TYPES = ("imzML",)
