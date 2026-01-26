# 实验管理 API 指南

## 概述

Kawaru 后端支持自动文件解压和实验管理功能。用户上传 ZIP 文件后，系统自动在后台解压验证，将实验数据结构化存储到对应的实验表中。

## 核心特性

- **自动解压**：ZIP 文件上传成功后自动触发后台解压
- **多实验类型支持**：通过 `experiment_type` 字段区分不同实验类型（imzML、raw 等）
- **按类型查询**：根据文件的 `experiment_type` 字段定位对应实验表，避免遍历多表
- **失败自动清理**：过期的失败实验记录会被自动清理

## 工作流程

```
1. 文件预检 → 前端指定 experiment_type（如 imzML）
2. 文件上传 → 普通上传或分片上传
3. 自动解压 → 检测到 ZIP 文件后触发后台任务
4. 文件验证 → 验证 .imzML 和 .ibd 文件
5. 移动文件 → 验证通过后移动到 experiments 目录
6. 清理资源 → 删除原始 ZIP 和临时文件
7. 状态更新 → 更新实验状态（completed/failed）
```

## API 接口

### 文件上传预检

**POST /files/preflight**

预检时需指定 `experiment_type` 字段，用于后续查询时定位对应实验表。

```bash
curl -X POST "http://localhost:8000/files/preflight" \
  -H "Authorization: Bearer <token>" \
  -F "filename=sample.zip" \
  -F "hash_sha256=<file_hash>" \
  -F "size=<file_size>" \
  -F "experiment_type=imzML" \
  -F "organism=Human" \
  -F "organism_part=Brain" \
  -F "condition=Healthy" \
  -F "sample_stabilization=Fresh frozen" \
  -F "tissue_modification=Sectioned" \
  -F "maldi_matrix=DHB" \
  -F "maldi_matrix_application=Spray coating"
```

**关键参数：**
| 参数 | 必填 | 说明 |
|------|------|------|
| `experiment_type` | 是 | 实验类型，用于定位实验表（如 `imzML`、`raw`） |
| `organism` | 是 | 样本所属物种 |
| `organism_part` | 是 | 样本来源组织 |
| `condition` | 是 | 样本状态 |
| `sample_growth_conditions` | 否 | 样本生长或处理条件 |
| `sample_stabilization` | 是 | 样本稳定方式 |
| `tissue_modification` | 是 | 组织修饰方式 |
| `maldi_matrix` | 是 | MALDI 基质 |
| `maldi_matrix_application` | 是 | 基质施加方式 |
| `solvent` | 否 | 使用溶剂 |

### 文件上传

**POST /files/upload_simple**

上传成功后，ZIP 文件会自动触发解压处理。

```bash
curl -X POST "http://localhost:8000/files/upload_simple" \
  -H "Authorization: Bearer <token>" \
  -F "pre_file_id=<preflight_id>" \
  -F "file=@sample.zip"
```

### 查询实验

**GET /experiments/imzml/{file_id}**

根据源文件 ID 查询实验记录。系统会根据文件的 `experiment_type` 字段自动定位对应实验表。

```bash
curl -X GET "http://localhost:8000/experiments/imzml/123" \
  -H "Authorization: Bearer <token>"
```

**响应示例：**
```json
{
  "id": 1,
  "experiment_name": "sample",
  "user_id": 1,
  "source_file_id": 123,
  "imzml_path": "/storage/experiments/user_1/exp_1/sample.imzML",
  "imzml_size": 2048576,
  "ibd_path": "/storage/experiments/user_1/exp_1/sample.ibd",
  "ibd_size": 8192000,
  "created_at": "2025-12-27T15:30:00",
  "status": "completed",
  "error_message": ""
}
```

## 实验状态

| 状态 | 说明 |
|------|------|
| `processing` | 正在解压和验证 |
| `completed` | 解压验证成功 |
| `failed` | 解压失败或验证不通过 |

## 存储结构

```
storage/
├── local/                    # 原始 ZIP 文件（处理后删除）
├── experiments/
│   ├── temp/                 # 解压临时目录
│   └── user_{id}/
│       └── exp_{id}/
│           ├── sample.imzML
│           └── sample.ibd
```

## 扩展新实验类型

添加新实验类型（如 RAW）只需：

1. **创建实验模型** (`models/experiment_models.py`)
```python
class ExperimentRawInDB(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    user_id: int
    source_file_id: int
    # ... RAW 特有字段
```

2. **添加查询函数** (`api/experiment_api.py`)
```python
async def get_raw_experiment(file_id: int, user_id: int, session: SessionDep):
    return session.exec(
        select(ExperimentRawInDB).where(
            ExperimentRawInDB.source_file_id == file_id,
            ExperimentRawInDB.user_id == user_id
        )
    ).first()
```

3. **注册到映射表**
```python
EXPERIMENT_QUERY_MAP = {
    "imzML": get_imzml_experiment,
    "raw": get_raw_experiment,  # 新增
}
```

4. **添加后台处理函数**（参考 `process_imzml_experiment_background`）

## 配置项

`config.yaml` 中的相关配置：

```yaml
storage:
  failed_experiment_ttl_minutes: 30  # 失败实验记录过期时间（分钟）
```

## 注意事项

1. **前端必须指定 experiment_type**：预检时必须传入，否则无法查询实验
2. **解压处理**：解压是后台自动触发，后续应该还要扩展
3. **自动清理**：处理完成后自动删除原始 ZIP 文件
4. **权限控制**：用户只能查看自己的实验
5. **临时目录**：解压临时文件存放在 `experiments/temp/` 下
