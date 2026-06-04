"""
限流状态管理器。

替代 utils.py 中的模块级全局变量 (total_rate_limit_wait / rate_limit_exceeded)，
将限流状态封装在实例中，支持：
  - 多实例独立运行（可测试）
  - 入口拦截（避免在超限后仍发起无效请求）
  - 线程安全的基本状态操作
"""

import time
from config import MAX_TOTAL_RATE_LIMIT_WAIT


class RateLimiter:
    """GitHub API 限流状态管理器。

    跟踪累计的限流等待时间，在超过上限后设置停止标志。
    所有需要调用 GitHub API 的组件共享同一个 RateLimiter 实例。

    Attributes:
        total_wait: 累计限流等待秒数（实际 sleep 过的总时间）
        exceeded: 是否已超过最大等待限制
        max_wait: 最大允许等待秒数（默认取自 config.MAX_TOTAL_RATE_LIMIT_WAIT）

    使用示例：
        limiter = RateLimiter(max_wait=600)
        ...
        if limiter.should_stop():
            return  # 立即停止
        ...
        if resp.status_code == 403:
            if not limiter.record_wait(estimated_wait):
                return  # 超限，放弃
    """

    def __init__(self, max_wait: int = None):
        """初始化限流管理器。

        Args:
            max_wait: 最大允许累计等待秒数。
                      默认使用 config.MAX_TOTAL_RATE_LIMIT_WAIT (600)。
                      设为 0 相当于关闭限流保护（不推荐）。
        """
        self.max_wait = max_wait if max_wait is not None else MAX_TOTAL_RATE_LIMIT_WAIT
        self.total_wait = 0.0
        self.exceeded = False

    def should_stop(self) -> bool:
        """检查是否应立即停止所有 API 请求。

        调用者应在每次发起 API 请求前调用此方法。
        如果返回 True，应直接跳过请求，不再发起 HTTP 调用。

        Returns:
            True 表示累计等待已超限或已经标记超限，应立即停止。
        """
        return self.exceeded or self.total_wait >= self.max_wait

    def record_wait(self, seconds: int) -> bool:
        """记录一次限流等待，并实际 sleep。

        在 sleep 之前检查是否会导致超限：
          - 如果不超限：sleep 指定秒数，累加 total_wait，返回 True。
          - 如果超限：设置 exceeded = True，不 sleep，返回 False。

        调用者应检查返回值，如果为 False 则终止后续操作。

        Args:
            seconds: 预计需要等待的秒数（通常来自 X-RateLimit-Reset 头）

        Returns:
            True 表示等待成功且未超限，可继续重试请求。
            False 表示等待会导致超限，应终止。
        """
        if self.total_wait + seconds > self.max_wait:
            self.exceeded = True
            return False

        if seconds > 0:
            time.sleep(seconds)
            self.total_wait += seconds

            # 睡醒后再次检查（兜底保护）
            if self.total_wait >= self.max_wait:
                self.exceeded = True
                return False

        return True

    def record_wait_no_sleep(self, seconds: int) -> bool:
        """仅记录等待时间，不实际 sleep。

        用于无法精确获取等待时长、只需标记超限的场景。

        Args:
            seconds: 等待秒数

        Returns:
            True 表示未超限，False 表示超限。
        """
        if self.total_wait + seconds > self.max_wait:
            self.exceeded = True
            return False
        self.total_wait += seconds
        if self.total_wait >= self.max_wait:
            self.exceeded = True
        return not self.exceeded

    def reset(self):
        """重置限流状态。

        仅用于测试或特殊场景（如用户手动确认继续），正常流程不应调用。
        """
        self.total_wait = 0.0
        self.exceeded = False

    def __repr__(self) -> str:
        return (f"RateLimiter(total_wait={self.total_wait:.0f}s, "
                f"exceeded={self.exceeded}, max={self.max_wait}s)")
