"""
通用工具函数。

提供跨模块使用的纯工具函数，无副作用、无网络请求。
限流和 HTTP 请求逻辑已迁移到 rate_limiter.py 和 http_client.py。
"""

import base64
import signal
from functools import wraps
from datetime import datetime, timezone, timedelta
from typing import Optional


# ==================== 时间与日志 ====================

# 北京时区（UTC+8），用于日志输出
_BEIJING_TZ = timezone(timedelta(hours=8))


def now_str() -> str:
    """返回北京时间字符串，格式：YYYY-MM-DD HH:MM:SS。

    用于所有日志的标准化时间戳。

    Returns:
        北京时间格式化字符串，例如 "2026-06-04 14:30:00"
    """
    return datetime.now(_BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')


def utc_now_iso_z() -> str:
    """返回 UTC 时间的 ISO 8601 格式字符串（含 Z 后缀）。

    用于 GitHub API 搜索查询中的时间参数。

    Returns:
        ISO 8601 格式字符串，例如 "2026-06-04T06:30:00Z"
    """
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


# ==================== 编码工具 ====================

def safe_base64_decode(s: str) -> Optional[str]:
    """安全 Base64 解码，自动补全 padding，容错非标准字符。

    处理常见的 Base64 变体：
      - URL-safe 字符集（- → +, _ → /）
      - 缺失 padding（自动补全）
      - 非标准尾部字符（使用 validate=False）

    Args:
        s: Base64 编码的字符串

    Returns:
        解码后的 UTF-8 字符串，解码失败返回 None
    """
    s = s.strip().replace('-', '+').replace('_', '/')
    if not s:
        return None
    padding = len(s) % 4
    if padding:
        s += '=' * (4 - padding)
    try:
        return base64.b64decode(s, validate=False).decode('utf-8', errors='replace')
    except Exception:
        return None


# ==================== 超时装饰器 ====================

def timeout_decorator(seconds: int):
    """函数超时装饰器（基于 signal.alarm）。

    ⚠️ 重要限制：
      - 仅在 Linux/macOS 上可用（Windows 无 SIGALRM）
      - 不能嵌套使用（signal 不可重入）
      - 不能在 ThreadPoolExecutor 线程中使用
      - 如果 seconds 为 None 或 <= 0，则不应用超时

    推荐替代方案：对于需要跨平台超时的场景，使用 ThreadPoolExecutor + future.result(timeout)。

    Args:
        seconds: 超时秒数。None 或 <= 0 时不应用超时。

    Returns:
        装饰后的函数
    """
    if seconds is None or seconds <= 0:
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            def handler(signum, frame):
                raise TimeoutError(f"函数 {func.__name__} 执行超过 {seconds} 秒")

            old = signal.signal(signal.SIGALRM, handler)
            signal.alarm(seconds)
            try:
                return func(*args, **kwargs)
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old)

        return wrapper
    return decorator
