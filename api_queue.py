"""
API 速率门 — 滑动窗口限速 + 端点分级 + 速率滞回 + 观测统计。

设计背景：
  同步 HttpClient 模型下，API 请求在途数 ≤ Worker 数（16），
  队列不会积压 → 无需队列结构/dispatcher 线程。
  真正的削峰依据是"最近 60s 放行速率"，用滑动窗口统计。

职责：
  - 全局限速（API_MAX_PER_MINUTE，默认 600/分钟）
  - 端点分级限速（search 30/分钟特殊，其他 900/分钟）
  - 速率滞回（≥480 暂停 Worker 取新任务，≤300 恢复）
  - 观测统计（端点分布、HTTP 状态、Retry-After 样本）

使用：
  gate = ApiRateGate()
  # HttpClient.get() 内：
  while not gate.acquire(url):
      time.sleep(random.uniform(0.3, 0.8))   # 自旋重试（抖动防惊群）
"""

import time
import threading
from collections import deque


class ApiRateGate:
    """API 速率门：滑动窗口限速器。

    所有 HttpClient 共享同一个实例。acquire() 非阻塞，
    放行返回 True，限速返回 False（调用方自旋重试）。
    """

    # GitHub 端点级限速（官方规则，请求/分钟）
    ENDPOINT_LIMITS = {
        "search": 30,     # ⚠️ Search API 特殊限制：30/分钟（认证）
        "tree": 900,
        "contents": 900,
        "commits": 900,
        "repos": 900,
        "forks": 900,
        "users": 900,
        "raw": None,      # raw 下载不计配额，直接放行
    }

    def __init__(self, max_per_minute: int = 600,
                 pause_at_rate: int = 480,
                 resume_at_rate: int = 300):
        """初始化 API 速率门。

        Args:
            max_per_minute: 全局速率上限（次/分钟）。
                            默认 600（次级限流 900 的 67%，留余量）。
            pause_at_rate: 速率 ≥ 此值 → 暂停 Worker 取新任务。
                           默认 480（600 的 80%，接近上限前提前收手）。
            resume_at_rate: 速率 ≤ 此值 → 恢复 Worker。
                           默认 300（600 的 50%，滞回防抖）。
        """
        self.max_per_minute = max_per_minute
        self.pause_at_rate = pause_at_rate
        self.resume_at_rate = resume_at_rate

        # 滑动窗口：[(timestamp, etype), ...] 最近 60s 放行记录
        self._window = deque()
        self._by_type = {}        # etype → 窗口内计数
        self._paused = False      # Worker 暂停标志
        self._warned = set()      # 已发预警的端点（节流）
        self._lock = threading.Lock()

        # 观测统计
        self.stats = {
            "total": 0,               # 总放行数
            "by_type": {},            # 端点类型 → 放行数（累计）
            "by_status": {},          # 响应状态码 → 次数
            "retry_after": [],        # 429 的 Retry-After 值列表
        }

    # ── 分类 ──

    @staticmethod
    def classify(url: str) -> str:
        """将 URL 分类为端点类型。"""
        if "raw.githubusercontent.com" in url or "raw." in url:
            return "raw"
        if "/search/" in url:
            return "search"
        elif "/git/trees/" in url:
            return "tree"
        elif "/contents/" in url or "/contents" in url:
            return "contents"
        elif "/commits" in url:
            return "commits"
        elif "/forks" in url:
            return "forks"
        elif "/users/" in url:
            return "users"
        else:
            return "repos"

    # ── 放行 ──

    def acquire(self, url: str) -> bool:
        """非阻塞放行检查。

        Returns:
            True: 允许发送（已计入窗口）
            False: 限速中，调用方应 sleep 后重试
        """
        etype = self.classify(url)
        if etype == "raw":
            return True  # raw 下载免费，直接放行

        with self._lock:
            now = time.time()
            self._prune(now)

            limit = self.ENDPOINT_LIMITS.get(etype, 900)
            count = self._by_type.get(etype, 0)

            # 端点超限 → 拒绝
            if limit and count >= limit:
                return False
            # 全局满速 → 拒绝
            if len(self._window) >= self.max_per_minute:
                return False

            # 端点预警（80% 上限，每端点只预警一次）
            if limit and count >= limit * 0.8 and etype not in self._warned:
                self._warned.add(etype)
                print(f"[{time.strftime('%H:%M:%S')}] "
                      f"⚠️ 端点 {etype} 达 80% 上限（{count}/{limit}/分钟）",
                      flush=True)

            # 放行
            self._window.append((now, etype))
            self._by_type[etype] = count + 1
            self.stats["total"] += 1
            self.stats["by_type"][etype] = self.stats["by_type"].get(etype, 0) + 1
            return True

    def _prune(self, now: float):
        """弹出 60 秒前的记录（O(1) 摊销）。

        双计数结构：_window（全量时间序列，用于全局速率）与
        _by_type（端点计数，用于端点级限速）必须同步增删；
        max(0, ...) 防负数——防御性写法，正常流程不会为负。
        """
        while self._window and self._window[0][0] < now - 60:
            _, etype = self._window.popleft()
            self._by_type[etype] = max(0, self._by_type.get(etype, 0) - 1)

    # ── 速率滞回 ──

    def should_pause(self) -> bool:
        """速率 ≥ pause_at_rate → 暂停 Worker 取新任务；≤ resume_at_rate → 恢复。

        滞回带（300-480）防止在阈值附近反复抖动。
        """
        with self._lock:
            rate = len(self._window)
            if rate >= self.pause_at_rate:
                self._paused = True
            elif rate <= self.resume_at_rate:
                self._paused = False
            return self._paused

    def current_rate(self) -> int:
        """当前速率（最近 60s 放行数）。"""
        with self._lock:
            return len(self._window)

    # ── 响应记录（供 HttpClient 调用，次级限流观测） ──

    def record_response(self, url: str, status_code: int,
                        retry_after: int = None):
        """记录一次 API 响应。"""
        etype = self.classify(url)
        with self._lock:
            self.stats["by_status"][status_code] = \
                self.stats["by_status"].get(status_code, 0) + 1
            if status_code in (403, 429) and retry_after:
                self.stats["retry_after"].append(retry_after)
                if len(self.stats["retry_after"]) > 100:
                    self.stats["retry_after"] = self.stats["retry_after"][-100:]

    def get_stats_report(self) -> str:
        """格式化统计报告。"""
        with self._lock:
            rate = len(self._window)
            lines = [
                "API 速率门统计 (ApiRateGate)",
                "=" * 40,
                f"  总放行:    {self.stats['total']}",
                f"  当前速率:  {rate}/{self.max_per_minute} 次/分钟",
                f"  暂停状态:  {'是' if self._paused else '否'}",
            ]
            for etype, count in sorted(self.stats["by_type"].items()):
                lines.append(f"  {etype:<10} {count}")
            for status, count in sorted(self.stats["by_status"].items()):
                lines.append(f"  HTTP {status:<4} {count}")
            if self.stats["retry_after"]:
                ra = self.stats["retry_after"]
                lines.append(f"  Retry-After 样本: min={min(ra)} max={max(ra)} "
                             f"avg={sum(ra)//len(ra)} (n={len(ra)})")
            lines.append("=" * 40)
            return "\n".join(lines)
