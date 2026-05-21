"""
通用工具函数：网络请求、超时保护、Base64 解码等
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

def safe_get(url: str, headers: Dict[str, str], timeout=(8, 15), max_retries=2,
             operation_name="请求") -> Optional[requests.Response]:
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

            if resp.status_code == 403:
                reset_time = resp.headers.get('X-RateLimit-Reset')
                if reset_time:
                    wait_seconds = int(reset_time) - int(time.time()) + 5
                    if wait_seconds > 120:
                        print(f"[{now_str()}] 限流等待过长({wait_seconds}s)，放弃", flush=True)
                        return None
                    print(f"[{now_str()}] 触发限流，等待 {wait_seconds}s ...", flush=True)
                    time.sleep(max(wait_seconds, 10))
                else:
                    time.sleep(30)
                continue

            wait = 3 + attempt * 2
            time.sleep(wait)

        except requests.exceptions.Timeout:
            print(f"[{now_str()}] {operation_name} 超时 (尝试 {attempt}/{max_retries})", flush=True)
        except requests.exceptions.ConnectionError:
            print(f"[{now_str()}] {operation_name} 连接错误 (尝试 {attempt}/{max_retries})", flush=True)
        except Exception as e:
            print(f"[{now_str()}] {operation_name} 异常: {e} (尝试 {attempt}/{max_retries})", flush=True)
            time.sleep(3 * attempt)

    return None

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
