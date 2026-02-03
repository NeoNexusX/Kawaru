"""
异常格式化工具模块
提供统一的异常格式转换函数
"""


def format_exception_detail(msg: str) -> dict:
    """
    将错误消息格式化为 FastAPI 标准格式
        
    Returns:
        符合 FastAPI 标准的 detail 字典
    """
    return {
        "detail": [
            {
                "type": "",
                "loc": [],
                "msg": msg,
                "input": {},
                "ctx": {}
            }
        ]
    }
