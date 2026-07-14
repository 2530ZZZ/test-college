"""
API 配额管理器 — 全局追踪、主动限速、停止信号。

所有 HttpClient 共享同一个 QuotaManager 实例，实现：
  1. 100% API 调用可见（消除 Pool Worker 统计盲区）
  2. 主动限速（调用前检查，超前配额预算则延迟等待）
  3. 配额耗尽统一通知（替代分散的 RateLimiter.exceeded）
  4. 区分"可恢复限流"和"永久配额耗尽"
"""

import time
import threading


class QuotaManager:
    """全局 API 配额管理器。

    设计原则：
      - check() 在每次 API 调用前调用 → 超配额返回 False
      - record() 在每次 API 调用后调用 → 累加计数
      - 主动限速不会 sleep 超过 2 秒，避免阻塞过久
      - exceeded 一旦设为 True 不会自动恢复（等窗口重置）

    使用示例：
        qm = QuotaManager(max_per_hour=4800)
        client = HttpClient(token="...", quota_manager=qm)
        # 在 http_client.get() 内部自动调用 qm.check() + qm.record()
    """

    def __init__(self, max_per_hour: int = 4800):
        """初始化配额管理器。

        Args:
            max_per_hour: 每小时最大 API 调用次数。
                          默认 4800（留 200 次余量，避免刚好耗尽）。
        """
        self.max_per_hour = max_per_hour
        self.calls = 0                # 当前窗口已用次数
        self.window_start = time.time()
        self._lock = threading.Lock()
        self.exceeded = False         # 配额耗尽标志（本窗口内不恢复）
        self.total_calls = 0          # 累计调用（统计用）
        self.throttle_count = 0       # 主动延迟次数（统计用）
        self.failed_calls = 0         # 失败调用次数（403/网络错误）

    # ── 限速检查 ──

    def check(self) -> bool:
        """每次 API 调用前调用。

        主动限速逻辑：
          如果当前消费速度快于配额预算 20%，sleep 等待。
          等待时间不超过 2 秒。

        Returns:
            True: 可以发起调用
            False: 配额已耗尽（exceeded=True），不应调用
        """
        with self._lock:
            now = time.time()
            elapsed = now - self.window_start

            # 窗口重置（每小时）
            if elapsed >= 3600:
                self.calls = 0
                self.window_start = now
                self.exceeded = False
                return True

            # 配额耗尽 — 硬停止
            if self.calls >= self.max_per_hour:
                self.exceeded = True
                return False

            # 主动限速：超前预算 20% 则延迟
            expected = (elapsed / 3600) * self.max_per_hour
            if self.calls > expected * 1.2:
                deficit = self.calls - expected
                avg_interval = 3600 / self.max_per_hour
                delay = min(deficit * avg_interval, 2.0)
                if delay > 0.01:
                    self.throttle_count += 1
                # sleep 在锁外执行会更好，但 2 秒内的阻塞可接受
                if delay > 0.01:
                    time.sleep(delay)

            return True

    # ── 调用记录 ──

    def record(self):
        """记录一次成功的 API 调用。"""
        with self._lock:
            self.calls += 1
            self.total_calls += 1

    def record_failed(self):
        """记录一次失败的 API 调用（403/网络错误等，配额已被消耗）。"""
        with self._lock:
            self.calls += 1
            self.total_calls += 1
            self.failed_calls += 1

    # ── 状态查询 ──

    def remaining(self) -> int:
        """返回当前窗口剩余配额。"""
        with self._lock:
            return max(0, self.max_per_hour - self.calls)

    def get_stats(self) -> dict:
        """返回统计信息，供 _finalize 使用。"""
        with self._lock:
            return {
                "total": self.total_calls,
                "remaining": self.remaining(),
                "throttled": self.throttle_count,
                "failed": self.failed_calls,
                "exceeded": self.exceeded,
            }

    def get_stats_report(self) -> str:
        """返回格式化的统计报告字符串。"""
        s = self.get_stats()
        lines = [
            "API 调用统计 (QuotaManager)",
            "=" * 40,
            f"  总请求数:    {s['total']}",
            f"  失败请求:    {s['failed']}",
            f"  主动限速:    {s['throttled']} 次",
            f"  剩余配额:    {s['remaining']}/{self.max_per_hour}",
            f"  配额耗尽:    {'是' if s['exceeded'] else '否'}",
            "=" * 40,
        ]
        return "\n".join(lines)

    def reset_window(self):
        """手动重置配额窗口（仅用于测试）。"""
        with self._lock:
            self.calls = 0
            self.window_start = time.time()
            self.exceeded = False
