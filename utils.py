"""
通用工具函数：网络请求、超时保护、Base64 解码、北京时间格式化等。
新增全局限流累计时长统计，以及超限自动终止检查。
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

beijing_tz = timezone(timedelta(hours=8))

# ==================== 限流控制 ====================
MAX_TOTAL_RATE_LIMIT_WAIT = 600          # 累计限流等待超过 10 分钟则强制终止收集
total_rate_limit_wait = 0.0              # 全局累计限流等待秒数

# ==================== Session 管理 ====================

def create_session() -> requests.Session:
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
    安全 HTTP GET 请求。
    遇到 403 限流时，会等待直到限流结束再重试，不再直接放弃。
    全局累计限流等待超过 MAX_TOTAL_RATE_LIMIT_WAIT 秒时，返回 None。
    """
    global total_rate_limit_wait
    session = create_session()

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=headers, timeout=timeout)

            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                print(f"[{now_str()}] {operation_name} 404，跳过", flush=True)
                return None
            if resp.status_code == 409:
                print(f"[{now_str()}] {operation_name} 409，跳过", flush=True)
                return None

            print(f"[{now_str()}] {operation_name} 返回 {resp.status_code} (尝试 {attempt}/{max_retries})", flush=True)

            # 处理 403 限流：等待，但不放弃
            if resp.status_code == 403:
                reset_time = resp.headers.get('X-RateLimit-Reset')
                if reset_time:
                    try:
                        wait_seconds = int(reset_time) - int(time.time()) + 5
                        if wait_seconds > 0:
                            # 判断累计等待是否超过全局阈值
                            if total_rate_limit_wait + wait_seconds > MAX_TOTAL_RATE_LIMIT_WAIT:
                                print(f"[{now_str()}] 累计限流等待已达 {total_rate_limit_wait:.0f}s，"
                                      f"再加 {wait_seconds}s 将超过阈值 {MAX_TOTAL_RATE_LIMIT_WAIT}s，放弃请求", flush=True)
                                return None
                            print(f"[{now_str()}] 触发限流，等待 {wait_seconds}s ...", flush=True)
                            time.sleep(wait_seconds)
                            total_rate_limit_wait += wait_seconds
                            continue       # 等待结束后重试
                    except Exception:
                        pass
                # 无法解析 reset time，保守等待 30 秒
                if total_rate_limit_wait + 30 > MAX_TOTAL_RATE_LIMIT_WAIT:
                    print(f"[{now_str()}] 累计限流等待即将超限，放弃", flush=True)
                    return None
                time.sleep(30)
                total_rate_limit_wait += 30
                continue

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
    如果累计限流等待超过全局阈值，则抛出 RuntimeError 终止收集。
    调用者可以在捕获后优雅退出。
    """
    if total_rate_limit_wait >= MAX_TOTAL_RATE_LIMIT_WAIT:
        raise RuntimeError("累计限流等待超过阈值，提前终止收集")


# ==================== 其他工具函数 ====================

def safe_base64_decode(s: str) -> Optional[str]:
    s = s.strip().replace('-', '+').replace('_', '/')
    if not s:
        return None
    padding = len(s) % 4
    if padding:
        s += '=' * (4 - padding)
    try:
        return base64.b64decode(s, validate=True).decode('utf-8', errors='replace')
    except Exception:
        return None


def now_str() -> str:
    return datetime.now(beijing_tz).strftime('%Y-%m-%d %H:%M:%S')


def timeout_decorator(seconds: int):
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
