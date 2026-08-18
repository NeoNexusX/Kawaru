from datetime import datetime
from enum import Enum
from pathlib import Path
from sqlmodel import Field, SQLModel, Relationship
from sqlalchemy import Column, Integer, BigInteger, Float
from tools import utc_time
from boot.config import get_config
from models.link_models import UserFileLink
from models.user_models import UserInDB
from models.auth import OssStsToken



# 模型字段配置来自 config
config = get_config()
storage_config = config.storage
sample_config = config.sample_info
user_config = config.user


class FileStatus(str, Enum):
    """文件处理状态枚举"""
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"


class PackupStatus(str, Enum):
    """文件打包状态"""
    PACKING = "packing"
    FAILED = "failed"
    COMPLETED = "completed"


class FileBase(SQLModel):
    """公共字段,供数据库与返回模型复用"""

    filename: str = Field(max_length=storage_config.max_filename_length)
    file_type: str = Field(default="", max_length=storage_config.max_file_type_length)
    # 【实验类型字段】用于定位对应的实验表(如 imzML/raw 等),由前端上传时指定
    experiment_type: str = Field(default="",
                                 description="实验类型(imzml/raw等),用于定位实验表",
                                 max_length=sample_config.max_experiment_type_length,)
    size: int = Field(default=0,sa_column=Column(BigInteger, nullable=False, default=0))
    storage_type: str = Field(default="oss", max_length=64)
    file_verify_code: str | None = Field(default=None, max_length=32)

    # 样本信息字段
    organism: str = Field(default="",description="样本所属物种",max_length=sample_config.max_organism_length,)
    organism_part: str = Field(default="",
                               description="样本来源组织",
                               max_length=sample_config.max_organism_part_length,)
    condition: str = Field(default="",description="样本状态",max_length=sample_config.max_condition_length,)
    sample_growth_conditions: str = Field(default="",
                                          description="样本生长或处理条件(选填)",
                                          max_length=sample_config.max_sample_growth_conditions_length,)
    sample_stabilization: str = Field(default="",description="样本稳定方式",max_length=sample_config.max_sample_stabilization_length,)
    tissue_modification: str = Field(default="",description="组织修饰方式",max_length=sample_config.max_tissue_modification_length,)
    maldi_matrix: str = Field(default="",description="MALDI 基质",max_length=sample_config.max_maldi_matrix_length,)
    maldi_matrix_application: str = Field(default="",
                                          description="基质施加方式",
                                          max_length=sample_config.max_maldi_matrix_application_length,)
    solvent: str = Field(default="",description="使用溶剂(选填)",max_length=sample_config.max_solvent_length,)
    is_public: bool = Field(default=False, description="是否公开文件")
    spectrum_mode: str = Field(default="profile", max_length=32, description="谱图模式: profile | centroid")
    storage_mode: str = Field(default="continuous", max_length=32, description="存储模式: continuous | processed")

    #质谱相关系数
    polarity: str = Field(default="",description="极性 (Polarity), 如 positive, negative",max_length=sample_config.max_polarity_length,)
    ionisation_source: str = Field(default="",description="电离源, 如 MALDI, ESI, DESI",max_length=sample_config.max_ionisation_source_length,)
    analyzer: str = Field(default="",description="质量分析器 (Analyzer), 如 TOF, Orbitrap, FTICR",max_length=sample_config.max_analyzer_length,)
    pixel_size_horizontal: float = Field(default=0.0,description="像素尺寸 - 水平方向 (Pixel Size, Horizontal)，单位通常为微米 (µm)",
                                         sa_column=Column(Float, nullable=False, default=0.0),)
    pixel_size_vertical: float = Field(default=0.0,description="像素尺寸 - 垂直方向 (Pixel Size, Vertical)，单位通常为微米 (µm)",
                                       sa_column=Column(Float, nullable=False, default=0.0),)
    resolving_power: float = Field(default=0.0,description="分辨率 (Resolving Power)，如 100000（无单位，或指 FWHM）",
                                   sa_column=Column(Float, nullable=False, default=0.0),)
    mz: float = Field(default=0.0,description="质荷比 (m/z, Mass-to-Charge Ratio)",
                      sa_column=Column(Float, nullable=False, default=0.0),)


class FileCreate(FileBase):
    """创建文件时的请求模型"""
    total_parts: int | None = Field(default=None, description="兼容保留的总分片数字段")


class FilePublic(FileBase):
    """对外返回的文件信息（通用元数据，不含 OSS 上传临时凭证）"""

    file_id: int
    status: str = Field(default="", max_length=32, description="文件处理状态: uploading, completed, failed")
    uploaded_at: datetime
    download_count: int
    last_downloaded_at: datetime
    first_uploaded_by: str | None = Field(default=None, description="首次上传该文件的用户名")
    is_reuse: bool = Field(default=False, description="是否为复用已有文件（秒传）")
    default_run_id: int | None = Field(default=None, description="指向默认展示的 raw_convert/algo run")


class FileOssUploadResponse(FilePublic):
    """OSS 上传接口专用响应模型，附加 STS 临时凭证及写入路径"""

    oss_sts_token: OssStsToken | None = Field(default=None)
    oss_bucket: str | None = Field(default=None, description="OSS存储桶(仅用于OSS存储类型)")
    oss_path: str | None = Field(default=None, description="OSS上传路径(仅用于OSS存储类型)")
    oss_region_id: str | None = Field(default=None, description="OSS区域ID(仅用于OSS存储类型)")


class FileInDB(FileBase, table=True):
    """数据库模型"""

    file_id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})  # 文件编号(自增主键)
    is_public: bool = Field(default=False, description="是否公开文件")
    path: str = Field(max_length=getattr(storage_config, "max_path_length", 128))  # 存储路径(长度限制取ext4文件系统限制路径最大长度255字符)

    status: str = Field(default="", max_length=32, description="文件处理状态: uploading, completed, failed")
    first_uploaded_by: str | None = Field(default=None, max_length=user_config.max_username_length, description="首次上传该文件的用户名")
    # 与 UserInDB 的多对多关系
    owners: list[UserInDB] = Relationship(back_populates="files", link_model=UserFileLink)
    uploaded_at: datetime = Field(default_factory=utc_time, index=True)  # 上传时间
    last_downloaded_at: datetime = Field(default_factory=utc_time, index=True)  # 最近下载时间
    download_count: int = Field(default=0)  # 下载次数
    total_parts: int | None = Field(default=1, description="兼容保留的总分片数字段")
    image_path: str | None = Field(default=None, max_length=255, description="预览图像(preview.jpg)在 OSS 上的完整对象键，由 FC 回调写入")
    default_run_id: int | None = Field(default=None, sa_column=Column(Integer, nullable=True), description="指向默认展示的 raw_convert/algo run")
    # 压缩率 = 上传 zip 实际大小 / 压缩前大小（size 字段），由 FC 回调写入；
    # 值越小压缩收益越大，供下载分流（打包 zip vs RAW 直链）参考；NULL 表示回调未完成
    compression_ratio: float | None = Field(
        default=None,
        sa_column=Column(Float, nullable=True),
        description="压缩率 = zip实际大小 / 压缩前大小，FC 回调时写入",
    )

class FileDownloadMeta(FileBase):
    """下载所需的文件元数据"""

    path: Path
    oss_download_url: str | None = None
    file_id: int | None = None
    uploaded_at: datetime
    download_count: int
    last_downloaded_at: datetime

    @classmethod
    def from_record(cls, record: FileInDB) -> "FileDownloadMeta":
        """从文件记录创建下载元数据"""
        return cls(
            file_id=record.file_id,
            filename=record.filename,
            file_type=record.file_type,
            size=record.size,
            storage_type=record.storage_type,
            file_verify_code=record.file_verify_code,
            path=Path(record.path),
            uploaded_at=record.uploaded_at,
            download_count=record.download_count,
            last_downloaded_at=record.last_downloaded_at,
        )


class OssDownloadFile(SQLModel):
    """RAW 直链下载的单个文件"""

    filename: str   # "Brain01.imzML" / "Brain01.ibd"
    url: str        # 预签名下载 URL
    size: int       # 字节


class RawDownloadResponse(SQLModel):
    """RAW 直链下载接口的完整响应"""

    files: list[OssDownloadFile]


class ZarrFolderAccess(SQLModel):
    """zarr 文件夹访问凭证 — 前端拿到后通过 OSS SDK 直连读取"""
    folder_path: str = Field(description="OSS 上的 zarr 文件夹路径")
    bucket: str = Field(description="OSS bucket 名称")
    region: str = Field(description="OSS region")
    sts_token: "OssStsToken" = Field(description="OSS 临时访问凭证")
    expires_in: int = Field(description="STS Token 有效时间（秒）")
