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


# ==================== TCP 预筛选 ====================

import socket
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional


def parse_host_port(node_str: str) -> Tuple[Optional[str], Optional[int]]:
    """从节点 URI 中提取 host 和 port。

    Args:
        node_str: 原始节点 URI，如 vmess://xxx、ss://xxx、trojan://pass@host:port

    Returns:
        (host, port) 如果无法解析返回 (None, None)
    """
    import urllib.parse
    try:
        if "://" not in node_str:
            return None, None
        # 对于标准 URI（trojan、vless、hysteria2 等）
        if "@" in node_str:
            # trojan://password@host:port
            # ss://method:password@host:port
            after_at = node_str.split("@", 1)[1]
            if "?" in after_at:
                after_at = after_at.split("?")[0]
            if "#" in after_at:
                after_at = after_at.split("#")[0]
            if ":" in after_at:
                host, port_str = after_at.rsplit(":", 1)
                return host, int(port_str)
        else:
            # ss://base64 或 vmess://base64（以 base64 起始，不包含 @）
            # 尝试 URL 解码
            parsed = urllib.parse.urlparse(node_str)
            if parsed.hostname and parsed.port:
                return parsed.hostname, parsed.port
    except Exception:
        pass
    return None, None


def tcp_check(host: str, port: int, timeout: float = 2.0) -> bool:
    """检查指定主机和端口是否可达。

    Args:
        host: IP 或域名
        port: 端口号
        timeout: 连接超时秒数

    Returns:
        True 如果 TCP 连接成功
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def tcp_prescreen(node_list: List[str], timeout: float = 2.0,
                  max_workers: int = 500) -> List[str]:
    """TCP 端口预筛选：从节点列表中过滤出 TCP 可达的节点。

    使用线程池并发连接，适合快速过滤明显不可达的节点。

    Args:
        node_list: 节点 URI 字符串列表
        timeout: 单次 TCP 连接超时（秒）
        max_workers: 并发线程数

    Returns:
        TCP 可达的节点 URI 列表
    """
    if not node_list:
        return []

    alive = []
    tasks = {}

    # 线程池线程设为 daemon：tcp_check 网络卡住时程序退出不被阻塞
    # （与 collector 的 _daemon_thread_init 同理，见 08091 收尾挂 30 分钟）
    def _daemon_init():
        threading.current_thread().daemon = True

    with ThreadPoolExecutor(max_workers=max_workers,
                            initializer=_daemon_init) as executor:
        for node in node_list:
            host, port = parse_host_port(node)
            if host and port:
                tasks[executor.submit(tcp_check, host, port, timeout)] = node
            else:
                alive.append(node)  # 无法解析的节点默认放行

        for future in as_completed(tasks):
            node = tasks[future]
            try:
                if future.result():
                    alive.append(node)
            except Exception:
                pass

    return alive
