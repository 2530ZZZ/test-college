"""
通用工具函数：网络请求、超时保护、Base64 解码、北京时间格式化等。

限流控制参数 MAX_TOTAL_RATE_LIMIT_WAIT 统一由 config.py 管理，
本模块通过 import 引用，不再硬编码。
"""

import requests
import time
import base64
import signal
from functools import wraps
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Dict

# 引用 config 中的限流阈值（统一配置源）
from config import MAX_TOTAL_RATE_LIMIT_WAIT

beijing_tz = timezone(timedelta(hours=8))

# ==================== 限流控制状态 ====================

# 全局累计限流等待秒数（实际已经等待的总时间）
# 由 safe_get 在每次遇到 403 限流后累加
total_rate_limit_wait = 0.0

# 限流超限标志：一旦为 True，所有后续请求应立即放弃
# 由 safe_get 在检测到累计等待将超过阈值时设置
rate_limit_exceeded = False


# ==================== Session 管理 ====================

def create_session() -> requests.Session:
    """
    创建带重试策略和连接池的 requests Session。
    重试策略：
      - 最多重试 2 次
      - 只对 429/500/502/503/504 状态码触发重试
      - 退避因子为 1（重试间隔递增：1s, 2s, 4s...）
    连接池：
      - 最多同时保持 10 个连接
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=2,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# ==================== 网络请求 ====================

def safe_get(url: str, headers: Dict[str, str], timeout=(8, 15), max_retries=2,
             operation_name="请求") -> Optional[requests.Response]:
    """
    安全 HTTP GET 请求，带智能限流处理。

    限流处理逻辑（config.MAX_TOTAL_RATE_LIMIT_WAIT 集中管理阈值）：
      1. 遇到 403 限流时，解析 X-RateLimit-Reset 头获取精确等待时间。
      2. 如果预计等待时间 + 已累计等待 > 阈值 → 设置超限标志，立即放弃。
      3. 否则等待到限流结束，累加等待时间后重试。
      4. 等待结束后再次检查是否超限（兜底保护）。
      5. 累计等待超过阈值后，所有后续请求将直接放弃。

    返回值：
      成功时返回 Response 对象，失败时返回 None。
    """
    global total_rate_limit_wait, rate_limit_exceeded
    session = create_session()

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=headers, timeout=timeout)

            # 成功
            if resp.status_code == 200:
                return resp

            # 404/409：快速失败，不重试
            if resp.status_code == 404:
                print(f"[{now_str()}] {operation_name} 404，跳过", flush=True)
                return None
            if resp.status_code == 409:
                print(f"[{now_str()}] {operation_name} 409，跳过", flush=True)
                return None

            print(f"[{now_str()}] {operation_name} 返回 {resp.status_code} "
                  f"(尝试 {attempt}/{max_retries})", flush=True)

            # 403 限流处理
            if resp.status_code == 403:
                # 尝试解析精确等待时间
                reset_time = resp.headers.get('X-RateLimit-Reset')
                wait_seconds = 0
                if reset_time:
                    try:
                        wait_seconds = int(reset_time) - int(time.time()) + 5
                        if wait_seconds < 0:
                            wait_seconds = 0
                    except Exception:
                        wait_seconds = 0

                # 检查是否会导致超限（config.MAX_TOTAL_RATE_LIMIT_WAIT）
                if total_rate_limit_wait + wait_seconds > MAX_TOTAL_RATE_LIMIT_WAIT:
                    print(f"[{now_str()}] 限流等待 {wait_seconds}s 后将超过阈值 "
                          f"（累计 {total_rate_limit_wait:.0f}s），放弃后续请求", flush=True)
                    rate_limit_exceeded = True
                    return None

                # 正常等待
                if wait_seconds > 0:
                    print(f"[{now_str()}] 触发限流，等待 {wait_seconds}s ...", flush=True)
                    time.sleep(wait_seconds)
                    total_rate_limit_wait += wait_seconds
                    # 兜底检查
                    if total_rate_limit_wait >= MAX_TOTAL_RATE_LIMIT_WAIT:
                        rate_limit_exceeded = True
                        return None
                else:
                    # 无法解析等待时间，保守等待 30s
                    if total_rate_limit_wait + 30 > MAX_TOTAL_RATE_LIMIT_WAIT:
                        print(f"[{now_str()}] 保守等待 30s 后将超过阈值，放弃后续请求", flush=True)
                        rate_limit_exceeded = True
                        return None
                    print(f"[{now_str()}] 无法解析限流重置时间，保守等待 30s ...", flush=True)
                    time.sleep(30)
                    total_rate_limit_wait += 30
                    if total_rate_limit_wait >= MAX_TOTAL_RATE_LIMIT_WAIT:
                        rate_limit_exceeded = True
                        return None
                continue  # 等待结束后重试本次请求

            # 其他可重试错误（500, 502, 503 等）
            wait = 3 + attempt * 2
            print(f"[{now_str()}] {operation_name} 错误，等待 {wait}s 后重试...", flush=True)
            time.sleep(wait)

        except requests.exceptions.Timeout:
            print(f"[{now_str()}] {operation_name} 超时 (尝试 {attempt}/{max_retries})", flush=True)
        except requests.exceptions.ConnectionError:
            print(f"[{now_str()}] {operation_name} 连接错误 (尝试 {attempt}/{max_retries})", flush=True)
        except Exception as e:
            print(f"[{now_str()}] {operation_name} 异常: {e} (尝试 {attempt}/{max_retries})", flush=True)
            time.sleep(3 * attempt)

    print(f"[{now_str()}] {operation_name} 多次失败，已跳过", flush=True)
    return None


def check_rate_limit():
    """
    检查限流状态。如果累计等待已超过阈值或超限标志被设置，
    抛出 RuntimeError 终止收集。
    调用者应在搜索循环中捕获此异常以优雅退出。
    """
    if rate_limit_exceeded or total_rate_limit_wait >= MAX_TOTAL_RATE_LIMIT_WAIT:
        raise RuntimeError("限流超限，终止收集")


# ==================== 其他工具函数 ====================

def safe_base64_decode(s: str) -> Optional[str]:
    """
    安全 Base64 解码，自动补全 padding，容错非标准字符。
    如果解码失败返回 None。
    """
    s = s.strip().replace('-', '+').replace('_', '/')
    if not s:
        return None
    padding = len(s) % 4
    if padding:
        s += '=' * (4 - padding)
    try:
        # validate=False 跳过无效字符检查,兼容填充不规范的 Base64
        return base64.b64decode(s, validate=False).decode('utf-8', errors='replace')
    except Exception:
        return None


def now_str() -> str:
    """返回北京时间字符串，格式：YYYY-MM-DD HH:MM:SS，用于日志输出。"""
    return datetime.now(beijing_tz).strftime('%Y-%m-%d %H:%M:%S')


def timeout_decorator(seconds: int):
    """
    函数超时装饰器（基于 signal.alarm，仅 Linux 可用）。
    注意：不能嵌套使用，不能在多线程环境中使用。
    如果 seconds 为 None 或 <= 0，则不应用超时。
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
