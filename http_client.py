"""
HTTP 请求客户端。

封装 requests 库，集成:
  - 配额管理（QuotaManager.check/record）
  - 限流入口拦截（RateLimiter.should_stop()）
  - GitHub API 认证头
  - 智能 403 限流处理
  - 重试策略（自动退避）
  - 连接池管理
"""

import time
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Dict, Tuple

from rate_limiter import RateLimiter
from quota_manager import QuotaManager
from config import GITHUB_TOKEN, GATE_SPIN_TIMEOUT_SECONDS
from log_sink import log_sink


class HttpClient:
    """带限流保护和配额管理的 HTTP 客户端。

    核心设计：
      - 入口检查 QuotaManager.check()（配额耗尽直接拒绝）
      - 入口检查 limiter.should_stop()（限流超限直接拒绝）
      - 所有 API 调用统一汇报给 QuotaManager（消除统计盲区）

    Args:
        token: GitHub Personal Access Token。空字符串表示未认证请求。
        rate_limiter: RateLimiter 实例（Pool Worker 用独立实例管理自己的等待）。
        quota_manager: QuotaManager 实例（全局共享，追踪所有配额消耗）。
        pool_connections: 连接池大小（默认 10）
        pool_maxsize: 连接池最大连接数（默认 10）
        user_agent: 自定义 User-Agent

    使用示例：
        qm = QuotaManager(max_per_hour=4800)
        limiter = RateLimiter()
        client = HttpClient(token="ghp_xxx", rate_limiter=limiter, quota_manager=qm)
        resp = client.get("https://api.github.com/search/repositories?q=...")
    """

    def __init__(
        self,
        token: str = "",
        rate_limiter: RateLimiter = None,
        quota_manager: QuotaManager = None,
        api_gate=None,
        # 08131：连接池扩容 10→128——72 worker + 96 并发下载共享连接池，
        # 10 个位置耗尽时并发下载报 HTTPSConnectionPool 错误（20:12 爆发
        # 59 次的根因）。128 覆盖 96 并发许可 + API 请求。
        pool_connections: int = 128,
        pool_maxsize: int = 128,
        # 08113：浏览器 UA——原 "FreeNodesCollector" 是爬虫标识，raw CDN
        # 反爬检测（IP/UA/流量模型多层判定）直接命中 UA 层被限速。
        # API 请求有 token 认证不受 UA 影响；raw 无认证只能靠 UA 伪装。
        user_agent: str = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/126.0.0.0 Safari/537.36"),
    ):
        self.token = token or GITHUB_TOKEN
        self.limiter = rate_limiter or RateLimiter()
        self.quota = quota_manager or QuotaManager()
        self.api_gate = api_gate  # ApiRateGate 实例（None = 不限速）
        self.user_agent = user_agent

        # API 调用统计（当前实例）
        self.stats = {"total": 0, "by_endpoint": {}, "by_status": {}}

        # raw 下载统计（滑动窗口，最近 60 秒）
        self._raw_window = []          # [(timestamp, bytes)]
        self._raw_total = 0            # 累计字节
        self._raw_count = 0            # 累计文件数

        # 最近 404 的 operation_name 集合（区分 404 与网络错误用，
        # 调用方 get_json 返回 None 后查此集合决定 404 处理）
        self.last_404 = set()

        # 08173：自旋超时日志去重（每端点每分钟一条，防刷屏）
        self._gate_spin_log = {}

        # 08131：下载失败分类统计移到 collector 层（本类多实例共享监控
        # 块不便）；本类只通过返回值 reason 上报，不再自行汇总打印。

        # 构建带重试策略的 Session
        self.session = self._create_session(pool_connections, pool_maxsize)

    def _classify_url(self, url: str) -> str:
        """将 URL 分类为端点类型，用于统计。

        Returns:
            端点类型名称，如 "search", "repo", "tree", "commits", "raw", "compare"
        """
        # 按优先级匹配，`/search/` 比 `/repos/` 更具体
        if "/search/" in url:
            return "search"
        elif "/git/trees/" in url:
            return "tree"
        elif "/repos/" in url and "/compare/" in url:
            return "compare"
        elif "/repos/" in url and "/commits" in url:
            return "commits"
        elif "/repos/" in url and "/contents" in url:
            return "contents"
        elif "/repos/" in url and "/git/refs/" in url:
            return "refs"
        elif "/repos/" in url:
            return "repo"
        elif "raw.githubusercontent.com" in url:
            return "raw"
        elif "api.github.com" in url:
            return "api_other"
        else:
            return "other"

    def _record_call(self, url: str, status: int = 0, remaining: int = None):
        """记录一次 API 调用。

        Args:
            url: 请求的 URL
            status: HTTP 状态码（0 表示请求未发出）
            remaining: X-RateLimit-Remaining 值（如果可用）
        """
        self.stats["total"] += 1
        endpoint = self._classify_url(url)
        self.stats["by_endpoint"][endpoint] = self.stats["by_endpoint"].get(endpoint, 0) + 1
        status_group = f"{status // 100}xx" if status else "no_req"
        self.stats["by_status"][status_group] = self.stats["by_status"].get(status_group, 0) + 1
        if remaining is not None:
            self.stats["last_quota"] = remaining

    def get_stats_report(self) -> str:
        """生成 API 调用统计报告。"""
        lines = [
            "API 调用统计",
            "=" * 40,
            f"  总请求数:    {self.stats['total']}",
            "",
            "  按端点类型:",
        ]
        for ep, count in sorted(self.stats["by_endpoint"].items()):
            lines.append(f"    {ep:15s} {count}")
        lines.append("")
        lines.append("  按状态码:")
        for st, count in sorted(self.stats["by_status"].items()):
            lines.append(f"    {st:15s} {count}")
        if "last_quota" in self.stats:
            lines.append("")
            lines.append(f"  最后剩余配额: {self.stats['last_quota']}")
        lines.append("=" * 40)
        return "\n".join(lines)

    def reset_stats(self):
        """重置统计。"""
        self.stats = {"total": 0, "by_endpoint": {}, "by_status": {}}

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
        """API 请求头（api.github.com）：含 Authorization + User-Agent。

        每次访问动态生成，确保 token 变化后自动更新。
        """
        h = {"User-Agent": self.user_agent}
        if self.token:
            h["Authorization"] = f"token {self.token}"
        return h

    @property
    def raw_headers(self) -> Dict[str, str]:
        """raw.githubusercontent.com 请求头（08113：不带 Authorization）。

        raw 域名不支持认证头，社区实测携带 Authorization 的 raw 请求
        会被反爬特殊对待（触发 429/限流）——raw 下载只能靠 UA 伪装。
        """
        return {"User-Agent": self.user_agent}

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
        # 核心 API：api.github.com 且非 search（08171：search 有独立
        # 30/min 端点级限速，不消耗 4800/h 核心配额预算——search 占预算
        # 会让核心预算虚减；按端点类型显式匹配，比 is_search 判断更直白）
        is_core_api = "api.github.com" in url and "/search/" not in url
        is_search_api = "api.github.com" in url and "/search/" in url
        is_api = is_core_api or is_search_api

        for attempt in range(1, max_retries + 1):
            # ---- 入口拦截：限流超限则直接放弃 ----
            if self.limiter.should_stop():
                self._record_call(url, 0)
                return None

            # ---- API 速率门：削峰填谷（raw 下载直接放行） ----
            # 所有 api.github.com（core + search）都经速率门；gate 内部
            # 对 search 只查端点级 30/min、不占 core 共享窗口。
            # 08173：acquire 成功持有并发许可（_gate_held），请求结束
            # finally 严格配对 release（08112 信号量泄漏教训）。
            _gate_held = False
            if is_api and self.api_gate is not None:
                _gate_wait_t0 = time.time()
                while not self.api_gate.acquire(url):
                    # 08172/08173：自旋超时兜底——gate 计数 bug（search
                    # 计数泄漏）曾致 acquire 永久拒绝 → 无限自旋 → 主线程
                    # 卡死空转 3.5 小时。08173 修正：
                    # a) 超时 30→90s（>60s 窗口 + 余量，总量限速的正常排队
                    #    就超 30s，08173 实测 30s 误伤 50 次）
                    # b) 超时后**放弃本次请求**（return None，不强制放行
                    #    ——08173 实证强制放行绕过限速）
                    # c) 日志去重：每端点每分钟一条（08173 1 秒 50 条刷屏）
                    if time.time() - _gate_wait_t0 > GATE_SPIN_TIMEOUT_SECONDS:
                        _et = self._classify_url(url)
                        _now_ts = time.time()
                        if _now_ts - self._gate_spin_log.get(_et, 0) > 60:
                            self._gate_spin_log[_et] = _now_ts
                            log_sink.emit(f"[{_now()}] ⚠️ API 速率门自旋超时"
                                  f"（>{GATE_SPIN_TIMEOUT_SECONDS}s 未放行），"
                                  f"放弃请求 {operation_name}")
                        self._record_call(url, 0)
                        return None
                    time.sleep(random.uniform(0.3, 0.8))  # 自旋重试（抖动防惊群）
                _gate_held = True

            # ---- 配额检查：原子预占（防超发）+ 配额耗尽等待恢复 ----
            # acquire() 在单次加锁内完成检查+计数（08105 超发 4806/4800
            # 触发 GitHub 惩罚的根因：check/record 分开有竞态窗口）。
            # 仅核心 API 消耗 4800/h 预算（search 独立 30/min，不占）。
            if is_core_api and not self.quota.acquire():
                if _gate_held:
                    self.api_gate.release(url)  # 08173：配额失败也释放许可
                self._record_call(url, 0)
                self.quota.wait_for_reset()
                continue  # 配额恢复，重试本次请求

            try:
                resp = self.session.get(url, headers=self.headers, timeout=timeout)

                # 记录调用：仅 API 调用消耗配额（acquire 已预占计数）
                remaining_hdr = resp.headers.get("X-RateLimit-Remaining")
                remaining = None
                if remaining_hdr and is_api:
                    remaining = int(remaining_hdr)
                reset_hdr = resp.headers.get("X-RateLimit-Reset")
                # 注意：不在此处 set_reset_time——200 响应的 X-RateLimit-Reset
                # 总是"下一 UTC 整点"，会污染 wait_for_reset 的等待条件
                # （08081 事故：35 个 worker 被拨闹钟无限推迟）。
                # 只在 403 核心配额耗尽分支设置（下方 403 处理）。
                self._record_call(url, resp.status_code, remaining)
                if is_api:
                    # 记录响应状态（次级限流观测）
                    if self.api_gate is not None:
                        retry_after = resp.headers.get("Retry-After")
                        self.api_gate.record_response(
                            url, resp.status_code,
                            int(retry_after) if retry_after else None)

                # 成功
                if resp.status_code == 200:
                    # raw 下载统计（滑动窗口 60 秒）
                    if not is_api:
                        now = time.time()
                        size = len(resp.content)
                        self._raw_window.append((now, size))
                        self._raw_total += size
                        self._raw_count += 1
                        while self._raw_window and \
                                now - self._raw_window[0][0] > 60:
                            self._raw_window.pop(0)
                    return resp

                # 202 / 404 / 409：快速失败，不重试
                # 202 = DuckDuckGo 限流信号，重试只会加重限流
                if resp.status_code == 202:
                    log_sink.emit(f"[{_now()}] {operation_name} 202 (限流)，跳过")
                    return None
                if resp.status_code == 404:
                    self.last_404.add(operation_name)
                    log_sink.emit(f"[{_now()}] {operation_name} 404，跳过")
                    return None
                if resp.status_code == 409:
                    log_sink.emit(f"[{_now()}] {operation_name} 409，跳过")
                    return None

                log_sink.emit(f"[{_now()}] {operation_name} 返回 {resp.status_code} "
                      f"(尝试 {attempt}/{max_retries})")

                # ---- 403 处理（区分限流类型） ----
                if resp.status_code == 403:
                    remaining_hdr = resp.headers.get("X-RateLimit-Remaining")
                    retry_after = resp.headers.get("Retry-After")
                    body = (resp.text or "").lower()
                    remaining_val = (int(remaining_hdr)
                                     if remaining_hdr is not None else -1)
                    # 次级限流特征：Retry-After 头 或 body 含 secondary
                    # （速率过快/超发惩罚——短等待重试，不设 _reset_time，
                    #  不当核心配额等整点；08105 超发触发惩罚的教训）
                    if retry_after is not None \
                            or "secondary rate limit" in body:
                        wait_s = 60
                        if retry_after is not None:
                            try:
                                wait_s = min(max(int(retry_after), 1), 120)
                            except (ValueError, TypeError):
                                pass
                        log_sink.emit(f"[{_now()}] ⚠️ 次级限流"
                              f"{f'（Retry-After {retry_after}s）' if retry_after is not None else ''}"
                              f"，等待 {wait_s}s 后重试（remaining={remaining_val}）")
                        self.quota.secondary_limited = True
                        time.sleep(wait_s)
                        self.quota.secondary_limited = False
                        continue  # 重试

                    if remaining_val == 0:
                        # 08171：非整点 reset = GitHub 次级限流伪装（abuse 403
                        # 也可能带 remaining=0 + 非整点 reset；PAT 主限流 reset
                        # 必为 UTC 整点——08131 曾修"非整点=abuse"，08161 绝对
                        # 窗口重构时回归丢失）。误判为"核心配额耗尽"会死等
                        # 13-32 分钟（08171 实测 4 批 409 次、等 789s/1917s）。
                        # 非整点 → 按次级限流短退避重试，不 set_reset_time。
                        if reset_hdr:
                            try:
                                reset_ts = int(reset_hdr)
                            except (ValueError, TypeError):
                                reset_ts = 0
                            if reset_ts and abs(reset_ts % 3600) > 300:
                                log_sink.emit(f"[{_now()}] ⚠️ 次级限流伪装"
                                      f"（非整点 reset {reset_hdr}，remaining=0），"
                                      f"等待 60s 后重试")
                                time.sleep(60)
                                continue  # 短退避后重试本次请求
                        # 核心 API 配额耗尽（remaining=0）→ 记录 GitHub 重置时间
                        # （403-only：200 响应带同一 reset 头但语义不同，见上方注释）
                        if reset_hdr and is_core_api:
                            try:
                                self.quota.set_reset_time(int(reset_hdr))
                            except Exception:
                                pass
                        wait_seconds = self._parse_ratelimit_wait(resp)
                        log_sink.emit(f"[{_now()}] ⚠️ 核心 API 配额耗尽"
                              f"（remaining=0，重置 {reset_hdr}），"
                              f"等待 {wait_seconds}s 恢复")
                        # 计算是否会导致超限
                        if self.limiter.total_wait + wait_seconds > self.limiter.max_wait:
                            log_sink.emit(f"[{_now()}] 限流等待 {wait_seconds}s 后将超过阈值 "
                                  f"（累计 {self.limiter.total_wait:.0f}s），放弃后续请求")
                            self.limiter.exceeded = True
                            return None
                        ok = self.limiter.record_wait(wait_seconds)
                        if not ok:
                            return None
                        continue  # 等待结束，重试本次请求

                    # 其他 403（remaining > 0 且非次级）→ 访问被拒
                    log_sink.emit(f"[{_now()}] {operation_name} 403（访问被拒，"
                          f"配额剩余 {remaining_val}），跳过")
                    return None

                # 其他可重试错误（500, 502, 503 等）
                wait = 3 + attempt * 2
                log_sink.emit(f"[{_now()}] {operation_name} 错误，等待 {wait}s 后重试...")
                time.sleep(wait)

            except requests.exceptions.Timeout:
                log_sink.emit(f"[{_now()}] {operation_name} 超时 (尝试 {attempt}/{max_retries})")
            except requests.exceptions.ConnectionError:
                log_sink.emit(f"[{_now()}] {operation_name} 连接错误 (尝试 {attempt}/{max_retries})")
            except Exception as e:
                log_sink.emit(f"[{_now()}] {operation_name} 异常: {e} (尝试 {attempt}/{max_retries})")
                time.sleep(3 * attempt)
            finally:
                # 08173：并发许可严格配对（08112 信号量泄漏教训）——无论
                # 成功/403 重试/异常/返回，acquire 后必 release；否则 50 个
                # 许可耗尽 → 所有 API 请求永久排队（下一轮尝试重新 acquire）
                if _gate_held:
                    self.api_gate.release(url)
                    _gate_held = False

        log_sink.emit(f"[{_now()}] {operation_name} 多次失败，已跳过")
        return None

    def download_with_timeout(self, url: str, timeout: Tuple[float, float],
                              max_total_s: int, operation_name: str = "",
                              idle_max_s: int = 0) -> Tuple[Optional[bytes], str]:
        """流式下载 + 总时长上限（防 CDN 慢速限速无限下载）。

        read timeout 只防"无数据"——CDN 慢速限速（0.1MB/s 持续送数据）不会
        触发，100MB 文件能慢速下载 1000s+（08102 W-3 卡 2600s 根因）。
        此方法累计下载耗时，超过 max_total_s 立即放弃。

        idle_max_s（08111 新增）：0 字节窗口——持续 idle_max_s 秒收不到
        任何数据即判定为限流挂起并放弃（raw CDN 限流特征：连接挂着但
        不给数据，0MB 且空 chunk 持续送，read timeout 不触发）。传 0 禁用。

        Returns:
            (完整内容 bytes, "") 成功；
            (None, reason) 失败——reason 归一化：404 / HTTP xxx /
            timeout / connect / idle（0 字节窗口）/ max_total（超总时长）。
            08121：调用方据此区分"网络类失败"（限流信号）与"404/4xx"
            （文件不存在，不算限流）。
        """
        t0 = time.time()
        chunks = []
        total = 0
        last_data = t0
        try:
            # 08113：raw_headers（无 Authorization）——raw 域名不支持认证，
            # 带 token 反而触发反爬
            with self.session.get(url, headers=self.raw_headers,
                                  timeout=timeout, stream=True) as resp:
                if resp.status_code != 200:
                    if resp.status_code == 404:
                        self.last_404.add(operation_name)
                    # 08131：失败分类统计移到 collector 监控块（reason 上报）
                    return None, f"HTTP {resp.status_code}"
                for chunk in resp.iter_content(chunk_size=65536):
                    if time.time() - t0 > max_total_s:
                        log_sink.emit(f"[{_now()}] {operation_name} "
                              f"下载超总时长({max_total_s}s/{total/1024/1024:.0f}MB)，放弃")
                        return None, "max_total"
                    if chunk:
                        chunks.append(chunk)
                        total += len(chunk)
                        last_data = time.time()
                    elif idle_max_s > 0 and time.time() - last_data > idle_max_s:
                        # 0 字节窗口超时：连接挂着但无数据 = 限流挂起，
                        # 快速放弃（下次重试），不再死等 MAX_DOWNLOAD_SECONDS
                        log_sink.emit(f"[{_now()}] {operation_name} "
                              f"无数据({idle_max_s}s/{total/1024/1024:.0f}MB)，放弃")
                        return None, "idle"
        except requests.exceptions.Timeout:
            log_sink.emit(f"[{_now()}] {operation_name} 下载超时，放弃")
            return None, "timeout"
        except requests.exceptions.ConnectionError:
            return None, "connect"
        except Exception as e:
            # 08132：error 带具体异常名（如 error:SSLError）——监控失败
            # 分类可追溯；此类通用异常不计入降级信号（非限流特征）
            return None, f"error:{type(e).__name__}"
        content = b"".join(chunks) if chunks else None
        if content:
            # 08121：raw 下载统计（原在 get() 里，但 raw 下载走本方法不走
            # get() → 统计永远 0；移到这里修复）
            now = time.time()
            self._raw_window.append((now, len(content)))
            self._raw_total += len(content)
            self._raw_count += 1
            while self._raw_window and now - self._raw_window[0][0] > 60:
                self._raw_window.pop(0)
        return content, ""

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
            log_sink.emit(f"[{_now()}] {operation_name} JSON 解析失败: {e}")
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
                # +5 秒余量：GitHub 与本地时钟可能有秒级偏差，
                # 提前恢复会立刻撞上未重置的配额 → 403 风暴
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
