"""
重计算 Celery 任务模块（已迁移至 FC 函数计算）。

原有的 execute_algorithm_pipeline_task / build_and_run_pipeline 已迁移到阿里云 FC 函数中执行。
FC 函数通过 HTTP 触发（见 background/processing_background.py），通过回调接口返回结果（见 api/callback_api.py）。
"""
