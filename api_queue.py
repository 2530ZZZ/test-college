"""
API 速率门 — core 核心 API 滑动窗口限速 + 端点分级 + 速率滞回 + 观测统计。

设计背景：
  同步 HttpClient 模型下，API 请求在途数 ≤ Worker 数（200），
  队列不会积压 → 无需队列结构/dispatcher 线程。
  真正的削峰依据是"最近 60s 放行速率"，用滑动窗口统计。

职责：
  - core 核心 API 限速（API_MAX_PER_MINUTE=300/分钟，仅 repos/tree/
    contents/commits/forks/users 共享窗口，08171：防突发触发次级限流）
  - 端点分级限速（search_code 10/min、search_repos 30/min 独立窗口，
    不占 core 窗口；其他 900/分钟）
  - 速率滞回（≥240 暂停 Worker 取新任务，≤150 恢复）
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
import config


class ApiRateGate:
    """API 速率门：core 核心 API 滑动窗口限速器。

    所有 HttpClient 共享同一个实例。acquire() 非阻塞，
    放行返回 True，限速返回 False（调用方自旋重试）。
    """

    # GitHub 端点级限速（官方规则，请求/分钟）
    ENDPOINT_LIMITS = {
        "search_code": config.SEARCH_CODE_PER_MINUTE,   # 10/min（官方硬限制）
        "search_repos": config.SEARCH_OTHER_PER_MINUTE, # 30/min（官方硬限制）
        "tree": 900,
        "contents": 900,
        "commits": 900,
        "repos": 900,
        "forks": 900,
        "users": 900,
        "raw": None,      # raw 下载不计配额，直接放行
    }

    # core 核心 API 端点类型（计入 core 共享窗口；search/raw 独立）
    CORE_TYPES = ("repos", "tree", "contents", "commits", "forks", "users")

    def __init__(self, max_per_minute: int = 300,
                 pause_at_rate: int = 240,
                 resume_at_rate: int = 150,
                 max_concurrency: int = 50):
        """初始化 API 速率门。

        Args:
            max_per_minute: core 核心 API 速率上限（次/分钟）。
                            默认 300（5/s 持续，08171 触发线 12.8/s 的 40%，
                            08173：200 太紧导致启动排队 60s+ 误伤）。
            pause_at_rate: 速率 ≥ 此值 → 暂停 Worker 取新任务。
                           默认 240（300 的 80%，接近上限前提前收手）。
            resume_at_rate: 速率 ≤ 此值 → 恢复 Worker。
                           默认 150（300 的 50%，滞回防抖）。
            max_concurrency: 在途 API 请求并发上限（已发出未收到响应，
                           含 core+search，raw 不计）。默认 50——GitHub
                           次级限流 REST+GraphQL 并发 ≤100，官方建议 50；
                           CPU 90s/60s（总响应时间）约束：50×1.5s=75s<90
                           （08173 讨论确认，08171 的 202 并发 403 根因）。
        """
        self.max_per_minute = max_per_minute
        self.pause_at_rate = pause_at_rate
        self.resume_at_rate = resume_at_rate
        self.max_concurrency = max_concurrency

        # core 滑动窗口：[(timestamp, etype), ...] 最近 60s core 放行记录
        # （08171：仅 core 类型入窗——search 有独立端点级限制，raw 免费；
        # 混算会让 core 突发被 search 挤掉余量或反之）
        self._core_window = deque()
        # Search 子端点独立窗口（08174：search_code 10/min、search_repos 30/min
        # 官方限制不同，必须分开统计；也供 _prune 清理 _by_type 计数）
        self._search_code_window = deque()
        self._search_repos_window = deque()
        self._by_type = {}        # etype → 窗口内计数
        self._paused = False      # Worker 暂停标志
        self._warned = set()      # 已发预警的端点（节流）
        self._lock = threading.Lock()
        # 08173：在途 API 请求数（已发出未收到响应，含 core+search；
        # raw 不计）。acquire 放行 +1、http_client 请求完成 release -1，
        # 必须严格配对（08112 信号量泄漏教训：release 放 finally）。
        self._inflight = 0

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
        """将 URL 分类为端点类型。

        Search 子端点区分（08174）：
          - /search/code = 10/min（官方硬限制）
          - 其他 /search/（repositories/issues/commits）= 30/min
        """
        if "raw.githubusercontent.com" in url or "raw." in url:
            return "raw"
        # Search 子端点优先匹配（code 限制更严）
        if "/search/code" in url:
            return "search_code"
        if "/search/" in url:
            return "search_repos"
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

            # 08173：在途并发上限（防 08171 的 200 并发超 GitHub 100 上限）
            if self._inflight >= self.max_concurrency:
                return False

            limit = self.ENDPOINT_LIMITS.get(etype, 900)
            count = self._by_type.get(etype, 0)

            # 端点超限 → 拒绝
            if limit and count >= limit:
                return False
            # core 核心 API 满速 → 拒绝（search/raw 不占 core 窗口）
            if (etype in self.CORE_TYPES
                    and len(self._core_window) >= self.max_per_minute):
                return False

            # 端点预警（80% 上限，每端点只预警一次）
            if limit and count >= limit * 0.8 and etype not in self._warned:
                self._warned.add(etype)
                print(f"[{time.strftime('%H:%M:%S')}] "
                      f"⚠️ 端点 {etype} 达 80% 上限（{count}/{limit}/分钟）",
                      flush=True)

            # 放行
            if etype in self.CORE_TYPES:
                self._core_window.append((now, etype))
            elif etype == "search_code":
                self._search_code_window.append((now, etype))
            elif etype == "search_repos":
                self._search_repos_window.append((now, etype))
            self._inflight += 1  # 08173：在途请求 +1（调用方请求完成后 release）
            self._by_type[etype] = count + 1
            self.stats["total"] += 1
            self.stats["by_type"][etype] = self.stats["by_type"].get(etype, 0) + 1
            return True

    def release(self, url: str = ""):
        """请求完成释放在途并发许可（08173）。

        与 acquire 严格配对（08112 信号量泄漏教训）：http_client 在
        acquire 成功后 try/finally 中调用——无论成功/403/异常/重试
        都释放，否则 50 个许可耗尽后所有 API 请求永久排队。
        raw 请求不 acquire（不计入），无需 release。
        """
        with self._lock:
            self._inflight = max(0, self._inflight - 1)

    def inflight(self) -> int:
        """当前在途 API 请求数（监控显示）。"""
        with self._lock:
            return self._inflight

    def _prune(self, now: float):
        """弹出 60 秒前的记录（O(1) 摊销）。

        三窗口结构：_core_window、_search_code_window、_search_repos_window
        分别供 _prune 同步清理 _by_type（端点级限速计数）。
        max(0, ...) 防负数——防御性写法，正常流程不会为负。
        """
        while self._core_window and self._core_window[0][0] < now - 60:
            _, etype = self._core_window.popleft()
            self._by_type[etype] = max(0, self._by_type.get(etype, 0) - 1)
        while self._search_code_window and self._search_code_window[0][0] < now - 60:
            _, etype = self._search_code_window.popleft()
            self._by_type[etype] = max(0, self._by_type.get(etype, 0) - 1)
        while self._search_repos_window and self._search_repos_window[0][0] < now - 60:
            _, etype = self._search_repos_window.popleft()
            self._by_type[etype] = max(0, self._by_type.get(etype, 0) - 1)

    # ── 速率滞回 ──

    def should_pause(self) -> bool:
        """core 速率 ≥ pause_at_rate → 暂停 Worker 取新任务；
        ≤ resume_at_rate → 恢复。

        滞回带（100-160）防止在阈值附近反复抖动。
        """
        with self._lock:
            rate = len(self._core_window)
            if rate >= self.pause_at_rate:
                self._paused = True
            elif rate <= self.resume_at_rate:
                self._paused = False
            return self._paused

    def current_rate(self) -> int:
        """当前 core 速率（最近 60s 放行数）。"""
        with self._lock:
            return len(self._core_window)

    def window_moving(self, within: float = 30.0) -> bool:
        """速率门窗口是否在滚动（最近 within 秒内有请求被放行）。

        供 http_client 自旋等待区分"正常排队 vs 异常卡死"（081XX）：
        - 窗口一直在滚动 = 请求正常消耗，排队是暂时的，继续等
        - 窗口完全停摆 = gate 计数异常（08172 事故），等下去只会空转
        任一窗口（core/search_code/search_repos）有放行即视为滚动。
        """
        with self._lock:
            now = time.time()
            for w in (self._core_window, self._search_code_window,
                      self._search_repos_window):
                if w and now - w[-1][0] <= within:
                    return True
            return False

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
            rate = len(self._core_window)
            lines = [
                "API 速率门统计 (ApiRateGate)",
                "=" * 40,
                f"  总放行:    {self.stats['total']}",
                f"  core 速率:  {rate}/{self.max_per_minute} 次/分钟",
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
