"""
API 配额管理器 — 全局追踪、主动限速、停止信号。

所有 HttpClient 共享同一个 QuotaManager 实例，实现：
  1. 100% API 调用可见（消除 Pool Worker 统计盲区）
  2. 主动限速（调用前检查，超前配额预算则延迟等待）
  3. 配额耗尽统一通知（替代分散的 RateLimiter.exceeded）
  4. 区分"可恢复限流"和"永久配额耗尽"
"""

import time
import random
import threading
from datetime import datetime, timezone, timedelta

from log_sink import log_sink


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
        # 对齐 UTC 整点（GitHub 配额窗口边界）。之前用启动时刻导致程序窗口
        # 与 GitHub 窗口错位 30 分钟：程序计数先归零误判"配额耗尽"，
        # 实际 GitHub 窗口还剩大量配额（08081 日志 17:21 误判 0/4800）。
        self.window_start = time.time() // 3600 * 3600
        self._reset_time = 0          # GitHub 返回的真实重置时间戳
        self._lock = threading.Lock()
        self.exceeded = False         # 配额耗尽标志（本窗口内不恢复）
        self.secondary_limited = False  # 次级限流标志
        self.total_calls = 0          # 累计调用（统计用）
        self._current_utc_window = int(time.time() // 3600)  # 当前 UTC 整点窗口
        self._window_calls = 0        # 当前窗口已用次数（整点统计）
        self.throttle_count = 0       # 主动延迟次数（统计用）
        self.failed_calls = 0         # 失败调用次数（403/网络错误）
        self._reset_wait_logged = False  # 配额等待日志去重（跨线程只打一条）
        self._exhausted_window = 0     # 08161：耗尽时的 UTC 窗口序号（恢复判断基准）
        self._recovery_done = False    # 08171：本次窗口恢复动作是否已执行（互斥）

    @staticmethod
    def _bj_now() -> str:
        """北京时间时间戳（与 collector 的 now_str 保持一致）。"""
        return datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')

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

    def acquire(self) -> bool:
        """原子操作：检查配额并预占一个名额（防超发）。

        check()+record() 分开调用有竞态窗口——多线程并发时多个线程
        同时通过 check（calls 都 < max）再同时 record → 超发
        （08105：UTC 11:00 窗口 4806/4800，超发触发 GitHub 惩罚 403，
        _reset_time 被推到 13:00，24 个 worker 傻等 60 分钟）。
        acquire() 在单次加锁内完成检查+计数，杜绝超发。

        Returns:
            True: 已预占一个名额，可以发起调用
            False: 配额耗尽（exceeded=True），不应调用
        """
        with self._lock:
            now = time.time()
            elapsed = now - self.window_start
            # 窗口重置（每小时）
            if elapsed >= 3600:
                self.calls = 0
                self.window_start = now
                self.exceeded = False
                self._check_utc_window()
                return True
            # 配额耗尽 — 硬停止
            if self.calls >= self.max_per_hour:
                self.exceeded = True
                return False
            # 主动限速：超前预算 20% 则延迟（与 check 一致，锁内 sleep ≤2s）
            expected = (elapsed / 3600) * self.max_per_hour
            if self.calls > expected * 1.2:
                deficit = self.calls - expected
                avg_interval = 3600 / self.max_per_hour
                delay = min(deficit * avg_interval, 2.0)
                if delay > 0.01:
                    self.throttle_count += 1
                    time.sleep(delay)
            # 预占名额（原子：检查+计数在同一锁内）
            self.calls += 1
            self.total_calls += 1
            self._check_utc_window()
            return True

    # ── 调用记录 ──

    def record(self):
        """记录一次成功的 API 调用。"""
        with self._lock:
            self.calls += 1
            self.total_calls += 1
            self._check_utc_window()

    def record_failed(self):
        """记录一次失败的 API 调用（403/网络错误等，配额已被消耗）。"""
        with self._lock:
            self.calls += 1
            self.total_calls += 1
            self.failed_calls += 1
            self._check_utc_window()

    def _check_utc_window(self):
        """UTC 整点窗口检测：窗口变化时打印上一窗口消耗。

        GitHub 核心 API 配额在 UTC 整点刷新，窗口边界必须用 UTC。
        每次 record 调用一次，整数比较开销可忽略。
        """
        window = int(time.time() // 3600)
        if window != self._current_utc_window:
            if self._window_calls > 0:
                prev_hh = datetime.fromtimestamp(
                    (window - 1) * 3600, tz=timezone.utc).strftime('%H:%M')
                log_sink.emit(f"[{self._bj_now()}] "
                              f"🕐 UTC {prev_hh} 窗口已用 "
                              f"{self._window_calls}/{self.max_per_hour} 配额")
            self._current_utc_window = window
            self._window_calls = 0
        self._window_calls += 1

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

    def set_reset_time(self, reset_timestamp: int):
        """记录 GitHub API 返回的 X-RateLimit-Reset 时间戳。"""
        # 只增不减：多个 worker 并发上报，取"最晚的重置时间"为权威——
        # 若允许被更早的值覆盖，等待中的 worker 可能提前恢复触发 403
        # （与 http_client 中"200 响应不调用本方法"配合，见 08081 事故）
        if reset_timestamp > self._reset_time:
            self._reset_time = reset_timestamp

    def wait_for_reset(self, should_stop: callable = None) -> bool:
        """配额耗尽时暂停等待 GitHub 窗口恢复。

        优先使用 GitHub 返回的 X-RateLimit-Reset 时间戳（真实恢复时间），
        无数据时用绝对 UTC 窗口判断（08161 修复）。

        08161 修复：无真实时间的恢复条件从"now - window_start >= 3600"
        （相对 elapsed）改为"绝对窗口序号变化"（int(now//3600) !=
        _exhausted_window）——原相对判断会被"恢复后第一个 API 调用重置
        window_start"推后破坏 → 死等到下一整点且每整点重复（08161 实测
        16:00/17:00/18:00 连续死等，2 小时配额全浪费）。窗口序号用 UTC
        整点（北京时间整点与 UTC 整点一致）。

        日志去重：首次进入打印一条"配额耗尽"，多线程并发只打一次
        （_reset_wait_logged 实例标志），恢复时复位。

        Args:
            should_stop: 返回 True 表示应提前终止（如运行超时）。

        Returns:
            True: 配额已恢复可以继续
            False: 提前终止
        """
        # 记录耗尽时的 UTC 窗口序号（恢复判断基准，08161）。
        # 08171 修正：仅在新窗口的耗尽时更新基准 + 复位互斥——同窗口内
        # 并发进入/二次耗尽沿用基准（否则后进入线程刷新 exhausted_window=
        # 当前窗口，且首个恢复线程已清零 _reset_time → 绝对窗口判断永不
        # 满足 → 死等到下一整点，实测 5 线程并发恢复卡死）。
        _now_window = int(time.time() // 3600)
        if self._exhausted_window != _now_window:
            self._exhausted_window = _now_window
            self._recovery_done = False
        while True:
            with self._lock:
                now = time.time()
                _recovered = False
                if self._reset_time:
                    # 有 GitHub 真实重置时间 → 严格按它等（不提前恢复，避免 403 风暴）
                    if now >= self._reset_time:
                        self._reset_time = 0
                        _recovered = True
                else:
                    # 无真实时间 → 满足其一即恢复：
                    # a) 绝对窗口变化（整点已过，08161）
                    # b) 本窗口已恢复过（_recovery_done）且未再次耗尽
                    #    （calls < max）——并发等待线程的快速恢复路径，
                    #    首个恢复线程已清零 calls/exceeded，跟随者无需
                    #    再等整点（08171 修复：原逻辑跟随者死等）
                    if (int(now // 3600) != self._exhausted_window
                            or (self._recovery_done
                                and self.calls < self.max_per_hour)):
                        _recovered = True
                if _recovered:
                    self.exceeded = False
                    # 08171：恢复动作互斥——只有"首个"满足恢复条件的线程
                    # 清零 calls（否则后恢复线程把先恢复线程的计数清零 →
                    # 计数永远到不了 4800 → acquire 永远放行 → 恢复风暴超发，
                    # 08171 实测 00:53 剩余 245→4446 跳变 + 30s 4523 次）。
                    # 跨窗口恢复 = 新周期，允许清零；同窗口跟随者不清零。
                    if (not self._recovery_done
                            or int(now // 3600) != self._exhausted_window):
                        self._recovery_done = True
                        self.calls = 0
                    if self._reset_wait_logged:
                        self._reset_wait_logged = False
                        log_sink.emit(f"[{self._bj_now()}] 🔄 配额恢复，继续工作")
                    break
            # 首次等待日志（跨线程去重）
            if not self._reset_wait_logged:
                self._reset_wait_logged = True
                with self._lock:
                    wait = 0
                    if self._reset_time:
                        wait = max(0, self._reset_time - time.time())
                    else:
                        wait = 3600 - (time.time() - self.window_start) + 10
                    reset_bj = datetime.fromtimestamp(
                        time.time() + wait, tz=timezone(timedelta(hours=8))
                    ).strftime('%H:%M')
                log_sink.emit(f"[{self._bj_now()}] "
                      f"⏳ 已用 {self.calls}/{self.max_per_hour}（配额耗尽），"
                      f"等待 {wait/60:.0f}min 至 {reset_bj} 北京时间")
            # 计算等待时长
            with self._lock:
                if self._reset_time:
                    wait = max(0, self._reset_time - time.time()) + 2
                else:
                    wait = 3600 - (time.time() - self.window_start) + 10
            # 30 秒轮询上限：等待期间不空睡（每 30s 检查一次恢复/停止信号），
            # 也不高频轮询（避免无谓 CPU 与日志噪音）
            sleep_sec = min(30, wait)
            if sleep_sec > 0:
                time.sleep(sleep_sec)
            if should_stop and should_stop():
                return False
        # 08171：恢复后每线程随机退避 0.5-2s——200 线程同时恢复瞬间冲 API
        # 是次级限流触发源（08171 实测恢复后 30s 计数 4523 次）。随机错开
        # 让线程陆续恢复，配合 api_gate core 200/min 限速兜底，杜绝风暴。
        time.sleep(random.uniform(0.5, 2.0))
        return True

    def reset_window(self):
        """手动重置配额窗口（仅用于测试）。"""
        with self._lock:
            self.calls = 0
            self.window_start = time.time()
            self.exceeded = False
