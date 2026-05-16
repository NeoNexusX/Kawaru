"""
重计算 Celery 任务模块。

此模块的任务由独立的 compute worker 消费（队列名: compute），
适用于 CPU 密集型、同步（非协程）的数据处理任务。

Worker 不进行任何数据库操作 —— 所有输入通过参数传入，结果通过返回值传出。
"""
from pathlib import Path

from celery_runtime.state import get_celery_app
from models.processing_models import ProcessingAlgorithmsConfig
from api.processing_api_helper import validate_algorithms_json
from massflow.data_manager import MSDataManagerImzML
from massflow.preprocess import Preprocessor

celery_app = get_celery_app()


@celery_app.task()  # type: ignore
def execute_algorithm_pipeline_task(
    imzml_path: str,
    config_json: str,
    output_dir: str,
) -> str:
    """
    算法管线执行任务（纯 CPU，零 DB）

    Args:
        imzml_path: 输入 imzML 文件路径
        config_json: 算法配置 JSON 字符串
        output_dir: 输出目录

    Returns:
        output_path  zarr 输出目录路径
    """
    config = validate_algorithms_json(config_json)
    if config is None:
        raise ValueError("invalid algorithms JSON")
    return build_and_run_pipeline(imzml_path, config, output_dir)


def build_and_run_pipeline(
    imzml_path: str,
    config: ProcessingAlgorithmsConfig,
    output_dir: str,
    batch_size: int = 256,
) -> str:
    """
    构建 MassFlow Preprocessor 链并执行

    1. 加载 imzML 数据管理器，读取头数据
    2. 按 config 注册选中的算法（顺序不影响执行顺序）
    3. 启动管线，zarr 格式直接写入 output_dir
    4. 关闭数据管理器，返回输出路径

    Returns:
        output_path
    """

    dm = MSDataManagerImzML(filepath=imzml_path)
    dm.load_head_data()

    preprocessor = Preprocessor(
        dm,
        batch_size=batch_size,
        output_format="ion_image_zarr",
        output_path=output_dir,
    )

    # 注册算法（顺序不影响 Preprocessor 内部执行顺序）
    if config.baseline_correction is not None:
        preprocessor = preprocessor.baseline_correction(**config.baseline_correction.model_dump())

    if config.noise_reduction is not None:
        preprocessor = preprocessor.noise_reduction(**config.noise_reduction.model_dump())

    if config.normalization is not None:
        preprocessor = preprocessor.normalization(**config.normalization.model_dump())

    if config.peak_pick is not None:
        preprocessor = preprocessor.peak_pick(**config.peak_pick.model_dump())

    if config.peak_align is not None:
        preprocessor = preprocessor.peak_align(**config.peak_align.model_dump())

    # 执行管线（zarr 模式直接写入 output_path）
    result_dm = preprocessor.start()

    dm.close()
    result_dm.close()

    return str(Path(output_dir))
