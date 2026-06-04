"""
HTTP 请求客户端。

封装 requests 库，集成:
  - 限流入口拦截（RateLimiter.should_stop()）
  - GitHub API 认证头
  - 智能 403 限流处理（解析 X-RateLimit-Reset + RateLimiter 协调）
  - 重试策略（自动退避）
  - 连接池管理

替代 utils.py 中的 safe_get，消除全局变量依赖。
"""

import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Dict, Tuple

from rate_limiter import RateLimiter
from config import GITHUB_TOKEN


class HttpClient:
    """带限流保护的 HTTP 客户端。

    核心设计：入口即检查 limiter.should_stop()，
    避免在限流超限后仍发出无意义请求（浪费 API 配额）。

    Args:
        token: GitHub Personal Access Token。空字符串表示未认证请求。
        rate_limiter: RateLimiter 实例（可多客户端共享同一个）。
        pool_connections: 连接池大小（默认 10）
        pool_maxsize: 连接池最大连接数（默认 10）
        user_agent: 自定义 User-Agent

    使用示例：
        limiter = RateLimiter()
        client = HttpClient(token="ghp_xxx", rate_limiter=limiter)
        resp = client.get("https://api.github.com/search/repositories?q=...")
        if resp is None:
            return  # 限流超限或网络错误
    """

    def __init__(
        self,
        token: str = "",
        rate_limiter: RateLimiter = None,
        pool_connections: int = 10,
        pool_maxsize: int = 10,
        user_agent: str = "Mozilla/5.0 (compatible; FreeNodesCollector/5.0)",
    ):
        self.token = token or GITHUB_TOKEN
        self.limiter = rate_limiter or RateLimiter()
        self.user_agent = user_agent

        # 构建带重试策略的 Session
        self.session = self._create_session(pool_connections, pool_maxsize)

    def _create_session(self, pool_connections: int, pool_maxsize: int) -> requests.Session:
        """创建带重试策略和连接池的 requests Session。

        重试策略：
          - 最多重试 2 次
          - 只对 429/500/502/503/504 状态码触发重试
          - 退避因子为 1（重试间隔递增：1s, 2s, 4s...）

        Returns:
            配置好的 requests.Session 实例
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=2,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    @property
    def headers(self) -> Dict[str, str]:
        """构建请求头。

        每次访问动态生成，确保 token 变化后自动更新。

        Returns:
            包含 Authorization 和 User-Agent 的请求头字典
        """
        h = {"User-Agent": self.user_agent}
        if self.token:
            h["Authorization"] = f"token {self.token}"
        return h

    def get(
        self,
        url: str,
        timeout: Tuple[float, float] = (8, 15),
        max_retries: int = 2,
        operation_name: str = "请求",
    ) -> Optional[requests.Response]:
        """发送 GET 请求，带智能限流处理和自动重试。

        关键流程：
          1. 入口检查 limiter.should_stop() → 超限直接返回 None（不发请求）
          2. 发送 HTTP GET
          3. 200 → 返回 Response
          4. 403 → 解析限流信息 → 通过 limiter 记录等待 → 重试
          5. 404/409 → 快速失败，不重试
          6. 其他错误 → 自动退避重试

        Args:
            url: 请求 URL
            timeout: (connect_timeout, read_timeout) 元组，单位秒
            max_retries: 最大重试次数（默认 2）
            operation_name: 操作描述，用于日志输出

        Returns:
            成功时返回 Response 对象，失败时返回 None。
            返回 None 后调用者应检查 limiter.should_stop() 决定是否继续。
        """
        for attempt in range(1, max_retries + 1):
            # ---- 入口拦截：限流超限则直接放弃 ----
            if self.limiter.should_stop():
                return None

            try:
                resp = self.session.get(url, headers=self.headers, timeout=timeout)

                # 成功
                if resp.status_code == 200:
                    return resp

                # 404 / 409：快速失败，不重试
                if resp.status_code == 404:
                    print(f"[{_now()}] {operation_name} 404，跳过", flush=True)
                    return None
                if resp.status_code == 409:
                    print(f"[{_now()}] {operation_name} 409，跳过", flush=True)
                    return None

                print(f"[{_now()}] {operation_name} 返回 {resp.status_code} "
                      f"(尝试 {attempt}/{max_retries})", flush=True)

                # ---- 403 限流处理 ----
                if resp.status_code == 403:
                    wait_seconds = self._parse_ratelimit_wait(resp)

                    # 计算是否会导致超限
                    if self.limiter.total_wait + wait_seconds > self.limiter.max_wait:
                        print(f"[{_now()}] 限流等待 {wait_seconds}s 后将超过阈值 "
                              f"（累计 {self.limiter.total_wait:.0f}s），放弃后续请求", flush=True)
                        self.limiter.exceeded = True
                        return None

                    if wait_seconds > 0:
                        print(f"[{_now()}] 触发限流，等待 {wait_seconds}s ...", flush=True)
                        ok = self.limiter.record_wait(wait_seconds)
                        if not ok:
                            return None
                    else:
                        # 无法解析等待时间，保守等待 30s
                        if self.limiter.total_wait + 30 > self.limiter.max_wait:
                            print(f"[{_now()}] 保守等待 30s 后将超过阈值，放弃后续请求", flush=True)
                            self.limiter.exceeded = True
                            return None
                        print(f"[{_now()}] 无法解析限流重置时间，保守等待 30s ...", flush=True)
                        ok = self.limiter.record_wait(30)
                        if not ok:
                            return None
                    continue  # 等待结束，重试本次请求

                # 其他可重试错误（500, 502, 503 等）
                wait = 3 + attempt * 2
                print(f"[{_now()}] {operation_name} 错误，等待 {wait}s 后重试...", flush=True)
                time.sleep(wait)

            except requests.exceptions.Timeout:
                print(f"[{_now()}] {operation_name} 超时 (尝试 {attempt}/{max_retries})", flush=True)
            except requests.exceptions.ConnectionError:
                print(f"[{_now()}] {operation_name} 连接错误 (尝试 {attempt}/{max_retries})", flush=True)
            except Exception as e:
                print(f"[{_now()}] {operation_name} 异常: {e} (尝试 {attempt}/{max_retries})", flush=True)
                time.sleep(3 * attempt)

        print(f"[{_now()}] {operation_name} 多次失败，已跳过", flush=True)
        return None

    def get_json(self, url: str, timeout: Tuple[float, float] = (8, 15),
                 max_retries: int = 2, operation_name: str = "请求") -> Optional[dict]:
        """发送 GET 请求并尝试解析 JSON 响应。

        Args:
            同 get()

        Returns:
            解析后的 dict，失败返回 None
        """
        resp = self.get(url, timeout=timeout, max_retries=max_retries,
                        operation_name=operation_name)
        if resp is None:
            return None
        try:
            return resp.json()
        except Exception as e:
            print(f"[{_now()}] {operation_name} JSON 解析失败: {e}", flush=True)
            return None

    # ---- 私有方法 ----

    @staticmethod
    def _parse_ratelimit_wait(resp: requests.Response) -> int:
        """从 403 响应中解析建议等待时间。

        优先使用 X-RateLimit-Reset 头（精确到秒），
        无法解析时返回 0（由调用者使用保守默认值）。

        Args:
            resp: 403 响应对象

        Returns:
            建议等待秒数，0 表示无法解析
        """
        reset_time = resp.headers.get("X-RateLimit-Reset")
        if reset_time:
            try:
                wait_seconds = max(0, int(reset_time) - int(time.time()) + 5)
                return wait_seconds
            except (ValueError, TypeError):
                pass
        return 0


# ---- 内部辅助 ----

from datetime import datetime, timezone, timedelta

_BEIJING_TZ = timezone(timedelta(hours=8))


def _now() -> str:
    """返回北京时间字符串，用于日志。"""
    return datetime.now(_BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
