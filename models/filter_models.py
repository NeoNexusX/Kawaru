from typing import Any

from pydantic import model_validator
from sqlmodel import Field, SQLModel


class PaginationMeta(SQLModel):
    """分页元数据模型"""
    current_page: int = Field(description="当前页码")
    current_records: int = Field(description="当前页记录数")
    total_pages: int = Field(description="总页数")
    total_records: int = Field(description="总记录数")


class UserFilter(SQLModel):
    """用户筛选模型（管理端用户列表；暂不支持多值）"""
    username: str | None = Field(default=None, description="用户名")
    active: bool | None = Field(default=None, description="是否激活")
    institution: str | None = Field(default=None, description="所属机构")
    region: str | None = Field(default=None, description="所属地区")


class MultiValueFilter(SQLModel):
    """多值筛选模型基类：把每个属性的输入统一归一化为去重后的字符串列表

    一个属性支持多选（同字段多值 OR、不同字段之间 AND），同时兼容旧前端的单值写法：
    - "mouse"                    → ["mouse"]（旧前端单值，不破坏现有调用方）
    - ["Mouse", " human ", ""]   → ["Mouse", "human"]（去空白、去空串、保序去重）
    - "" / None / []             → None（视为不筛选，避免生成 IN () 这类恒假条件）
    """

    @model_validator(mode="before")
    @classmethod
    def _normalize_multi_values(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        normalized: dict[str, Any] = {}
        for key, value in data.items():
            if value is None or (isinstance(value, str) and not value.strip()):
                normalized[key] = None
                continue

            values = [value] if isinstance(value, str) else value
            if isinstance(values, (list, tuple, set)):
                cleaned = [
                    str(item).strip()
                    for item in values
                    if item is not None and str(item).strip()
                ]
                normalized[key] = list(dict.fromkeys(cleaned)) or None
            else:
                normalized[key] = value
        return normalized


class FileFilter(MultiValueFilter):
    """文件/实验筛选模型（每个字段可传单值或数组；同字段多值 OR，不同字段之间 AND）

    匹配方式分两类：filename / first_uploaded_by 为模糊匹配（LIKE %v%），
    其余为精确匹配（IN）；大小写由列排序规则（utf8mb4_unicode_ci）保证不敏感。
    """
    filename: list[str] | None = Field(default=None, description="文件名（模糊匹配）")
    experiment_type: list[str] | None = Field(default=None, description="实验类型")
    username: list[str] | None = Field(default=None, description="所属用户名（模糊匹配，走用户-文件关联）")
    first_uploaded_by: list[str] | None = Field(default=None, description="首次上传用户名（模糊匹配）")
    organism: list[str] | None = Field(default=None, description="样本所属物种")
    organism_part: list[str] | None = Field(default=None, description="样本来源组织")
    condition: list[str] | None = Field(default=None, description="样本生长或处理条件")
    sample_stabilization: list[str] | None = Field(default=None, description="样本稳定方式")
    sample_growth_conditions: list[str] | None = Field(default=None, description="样本生长条件")
    tissue_modification: list[str] | None = Field(default=None, description="组织修饰方式")
    maldi_matrix: list[str] | None = Field(default=None, description="MALDI 基质")
    maldi_matrix_application: list[str] | None = Field(default=None, description="基质施加方式")
    solvent: list[str] | None = Field(default=None, description="使用溶剂")
    polarity: list[str] | None = Field(default=None, description="极性")
    ionisation_source: list[str] | None = Field(default=None, description="电离源")
    analyzer: list[str] | None = Field(default=None, description="质量分析器")


class RunFilter(MultiValueFilter):
    """运行记录筛选模型（每个字段可传单值或数组；同字段多值 OR，不同字段之间 AND）

    两个字段均为模糊匹配（LIKE %v%）。
    """
    filename: list[str] | None = Field(default=None, description="源文件名(模糊匹配)")
    params: list[str] | None = Field(default=None, description="算法参数关键词(模糊匹配 params_json)")


class CollectionFilter(MultiValueFilter):
    """集合筛选模型（每个字段可传单值或数组；同字段多值 OR，不同字段之间 AND）

    匹配方式分三类：
    - name / title / journal_name / owner_username：模糊匹配（LIKE %v%）
    - member_type / collection_type：精确匹配（IN）
    - organism 等 JSON 数组字段：元素成员判断（集合内任一成员含该值），大小写不敏感

    不含 doi / access（标识符类字段，不参与筛选）。
    """
    # 标量-模糊
    name: list[str] | None = Field(default=None, description="集合名称（模糊匹配）")
    title: list[str] | None = Field(default=None, description="数据集标题（模糊匹配）")
    journal_name: list[str] | None = Field(default=None, description="期刊名（模糊匹配）")
    owner_username: list[str] | None = Field(default=None, description="集合 owner 用户名（模糊匹配）")
    # 标量-精确
    member_type: list[str] | None = Field(default=None, description="集合包含的数据类型")
    collection_type: list[str] | None = Field(default=None, description="集合类型")
    # JSON 数组（成员文件取值汇总，元素成员判断）
    organism: list[str] | None = Field(default=None, description="样本所属物种")
    organism_part: list[str] | None = Field(default=None, description="样本来源组织")
    sample_stabilization: list[str] | None = Field(default=None, description="样本稳定方式")
    sample_growth_conditions: list[str] | None = Field(default=None, description="样本生长条件")
    tissue_modification: list[str] | None = Field(default=None, description="组织修饰方式")
    polarity: list[str] | None = Field(default=None, description="极性")
    ionisation_source: list[str] | None = Field(default=None, description="电离源")
    analyzer: list[str] | None = Field(default=None, description="质量分析器")


class ListDataResponse(SQLModel):
    """数据列表响应模型"""
    meta: PaginationMeta = Field(description="分页元数据")
    data: list[Any] = Field(description="数据列表")
