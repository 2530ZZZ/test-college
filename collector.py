"""
GitHub 节点收集器 — 使用 git/trees API 获取递归文件树，
通过 URI 协议解析提取 StandardProxy，边搜集边持久化。

版本 5.0 改进：
  - 集成 HttpClient + RateLimiter（修复限流无法提前终止的 Bug）
  - 移除冗余 Repo Info API（使用搜索结果数据）
  - 支持可配置的文件时间检查策略（sha_only / commits_per_repo / commits_per_file）
  - SHA 缓存改进（相同 SHA 永久跳过，TTL 清理）
  - 集成 uri_parser 协议解析层
  - 边搜集边持久化分批写入
  - 全局去重（server, port, protocol）

【当前架构总览】（截至 08131，详见 docs/DESIGN.md）
  数据流：搜索（种子/关键词/Code）→ 仓库入主队列 → 72 个 worker 消费 →
  process_repo 前置判断（补信息/语言/缓存/size/年龄，零 API）→ size 分流
  （<50MB partial clone / ≥50MB tree+commits 增量）→ raw 下载（连接节流
  30/s + 并发信号量 96 + 0字节30s快速放弃）→ 解析（>1MB 进程池 2 进程，
  其余线程，内存预算 2GB）→ 批次内去重写 batches/ 分片 → 收尾全量去重
  → no_his/（7 天窗口）+ no/ 分片 → git push → 下游 subs-check 测速。

  关键机制（都有对应事故记录，改动前必读 docs/DESIGN.md）：
  - 队列：主队列 200 + 发现队列 20000（PriorityQueue），阈值调度
    （disc≥19000 强制消费 / ≤1000 允许取主队列），源头 Semaphore 限并发
  - 配额：QuotaManager 4800/h，UTC 整点对齐窗口，acquire 原子化防超发，
    wait_for_reset 等整点（403-only 设置 reset_time，防"闹钟拨快"睡死）
  - 追踪：标志位 [userN]/[forkN]/[rawN]/[kwN]/[cdN] 与层数解耦
    （_should_trace = 层数 < MAX_TRACE_DEPTH），TRACE_RETRY_DAYS=30 重追踪
  - 监控：60s 监控块（配额耗尽+60s 无日志降频 10min），log_sink 双队列
    高优先级通道防盲区，内存>80% faulthandler 转储一次
"""

import os
import time
import json
import random
import shutil
import re
import faulthandler
import gc
import pickle
import subprocess
import threading
import signal
import multiprocessing
from queue import Queue, PriorityQueue, Empty, Full
from collections import deque
from urllib.parse import quote
from datetime import datetime, timezone, timedelta
from concurrent.futures import (ThreadPoolExecutor, ProcessPoolExecutor,
                                as_completed, TimeoutError as FutureTimeoutError)
from typing import List, Set, Optional, Tuple, Dict

from config import (
    GITHUB_TOKEN, MAX_PAGES, PER_PAGE, REPO_SLEEP_SECONDS, PAGE_SLEEP_SECONDS, SEARCH_FORK,
    REPO_TIMEOUT_SECONDS, MAX_FILE_SIZE, FILE_PROCESS_TIMEOUT,
    ALLOWED_EXTENSIONS, SKIP_LANGUAGES,
    SEARCH_TIMEOUT, FILE_DOWNLOAD_TIMEOUT, MAX_DOWNLOAD_SECONDS,
    DOWNLOAD_IDLE_TIMEOUT, MAX_DOWNLOAD_CONCURRENCY,
    DOWNLOAD_STALL_THRESHOLD, DOWNLOAD_THROTTLE_SECONDS,
    SMALL_REPO_CLONE_MB, MAX_RAW_DOWNLOAD_CONNECTS_PER_SEC,
    FULL_CLONE_MB, FULL_CLONE_DISK_MIN_GB,
    SAMPLE_THRESHOLD, SAMPLE_PER_EXT, NO_NODE_REPOS_FILE, NO_NODE_RETRY_DAYS,
    EXTRACT_PROCESSES, DOWNLOAD_MEMORY_BUDGET_MB, EXTRACT_PROCESS_MIN_MB,
    NO_SPLIT_SIZE, MAIN_QUEUE_PAUSE_AT, MAIN_QUEUE_RESUME_AT,
    QUOTA_ENDGAME_MINUTES,
    CONTENTS_API_TIMEOUT, COMMITS_API_TIMEOUT, TREE_API_TIMEOUT,
    USE_RECURSIVE_TREE, MAX_COMMITS_PER_REPO,
    MAX_RAW_DOWNLOADS_PER_REPO, SEED_REPOS_FILE,
    PARALLEL_DOWNLOAD_THRESHOLD, PARALLEL_DOWNLOAD_WORKERS,
    FORK_CHAIN_ENABLED, FORK_CHAIN_MAX_FORKS,
    FORK_CHILD_MAX, FORK_SIBLING_MAX, FORK_PARENT_TRACE_ENABLED, FORK_PER_PAGE,
    MAX_TRACE_DEPTH, MAIN_TAKE_COOLDOWN,
    AUTO_SEED_ENABLED,
    TOPIC_SEARCH_ENABLED, TOPIC_QUERIES,
    README_SEARCH_ENABLED, README_QUERIES, README_MAX_PAGES,
    CODE_SEARCH_ENABLED, CODE_QUERIES, CODE_MAX_PAGES,
    MAX_PAGES_ZH_MULTIPLIER,
    USER_REPOS_ENABLED, USER_REPOS_MAX_PER_USER,
    VERBOSE_LOG, SHA_CACHE_DIR, SHA_CACHE_MAX_BYTES, SHA_CACHE_MAX_ENTRIES,
    SHARED_POOL_WORKERS,
    MAIN_QUEUE_SIZE, DISCOVERY_QUEUE_SIZE,
    DISC_FORCE_CONSUME_AT, DISC_MAIN_OK_AT, DISC_PUT_BACKPRESSURE,
    API_MAX_PER_MINUTE, API_PAUSE_AT_RATE, API_RESUME_AT_RATE,
    API_MAX_CONCURRENCY,
    SECONDARY_RATE_LIMIT_DEGRADE, DEGRADE_WORKERS,
    ENABLE_RAW_RECURSIVE, MAX_RECURSIVE_REPOS,
    PARTIAL_CLONE_ENABLED, PARTIAL_CLONE_TIMEOUT, PARTIAL_CLONE_CONCURRENCY,
    CLONE_FIRST_MODE,
    DOWNLOAD_QUEUE_SIZE, DOWNLOAD_WORKER_THREADS,
    PARSE_THREAD_POOL_SIZE, PARSE_WATCHDOG_SECONDS, PARSE_TIME_PERCENTILES,
    DOWNLOAD_MEMORY_BACKPRESSURE_MB, PROCESS_QUEUE_MAX,
    CONTENTS_API_FALLBACK_ENABLED, MONITOR_INTERVAL,
    MAIN_SOURCE_LIMIT, TRACE_RETRY_DAYS,
    KEYWORD_TRACE_DEPTH, CODE_TRACE_DEPTH, INFO_BACKFILL_ENABLED,
    AUTO_SEED_MIN_NODES_FOR_SEED,
    SEED_MAX_AGE_HOURS, SEED_MAX_ENTRIES, SEED_EVICTION_RATIO,
    SEEN_REPOS_PERSIST_ENABLED, SEEN_REPOS_DIR, SEEN_REPOS_MAX_BYTES,
    SEEN_CACHE_MAX_ENTRIES, SEEN_CACHE_EVICTION_RATIO,
    NOT_FOUND_REPOS_FILE, NOT_FOUND_REPOS_MAX,
    SUB_URL_MAX_PER_FILE, SAFE_WRITE_ENABLED,
    SEED_STAGE_ENABLED, CODE_STAGE_ENABLED, KEYWORD_STAGE_ENABLED,
    CHUNK_SIZE, DEDUP_STRATEGY, DEDUP_ENABLED, BATCH_DIR, BATCH_FLUSH_SIZE,
    MAX_RUNTIME_SECONDS,
    GITHUB_SEARCH_ENABLED, QUOTA_MAX_PER_HOUR,
    SKIP_PROCESSING_AGE_HOURS, LOG_MONITOR_FILE,
    QUEUE_PUT_TIMEOUT_SECONDS,
    LOG_FAILED_CANDIDATES,
    PARALLEL_DOWNLOAD_MB_HIGH, PARALLEL_DOWNLOAD_MB_MED,
    DOWNLOAD_DRAIN_TIMEOUT_SECONDS, WORKER_JOIN_TIMEOUT_SECONDS,
    WATCHDOG_DUMP_FILE,
    DOWNLOAD_BACKPRESSURE_QUEUE, DOWNLOAD_BACKPRESSURE_MEM_MB,
    DOWNLOAD_BACKPRESSURE_RESUME_QUEUE, DOWNLOAD_BACKPRESSURE_RESUME_MEM_MB,
    RESULT_QUEUE_SIZE,
)
from http_client import HttpClient, RateLimiter
from api_queue import ApiRateGate
from parsers import (
    extract_all_strategies, extract_embedded_uris, extract_clash_yaml,
    extract_singbox_json, extract_surge_format,
)
from utils import now_str
from quota_manager import QuotaManager
from log_sink import log_sink


def dedup_batches_write_no() -> int:
    """读本轮 batches → 单次去重 → 写 no/ 分片（08141：7 天窗口已回退）。

    供主流程 _finalize 与 workflow 兜底脚本 finalize.py 共用（两条路径
    产出一致——08111 取消场景下主流程收尾日志进 devnull、git 抢跑，
    no/ 丢失，此函数作为独立进程兜底）。

    语义（08141+）：no/ = 本轮运行的去重结果（单次运行，不做跨轮
    历史合并——7 天节点合并由用户本地自行处理）。内存优化不变：
    运行时只写 batches，收尾读全部批次去重，内存峰值 = 唯一节点数。

    Returns:
        唯一节点数；0 = 无批次（保留旧 no/ 不动）。
    """
    import glob
    batch_dir = os.path.join(os.getcwd(), BATCH_DIR)
    batch_files = sorted(glob.glob(os.path.join(batch_dir, "no_batch_*.txt")))
    if not batch_files:
        return 0  # 无产出 → 保留上次 no/（崩溃/无节点场景）
    all_nodes = set()
    for f in batch_files:
        with open(f, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if line:
                    all_nodes.add(line)
    nodes = sorted(all_nodes)

    # 清空旧 no/（上次运行的分片，本次重新生成）
    no_dir = os.path.join(os.getcwd(), "no")
    if os.path.isdir(no_dir):
        for old in glob.glob(os.path.join(no_dir, "*.txt")):
            try:
                os.remove(old)
            except OSError:
                pass
    os.makedirs(no_dir, exist_ok=True)

    repo_name = os.getenv("GITHUB_REPOSITORY", "2530ZZZ/cooo")
    branch_name = os.getenv("GITHUB_REF_NAME", "main")
    with open("no_w_li.txt", "w", encoding="utf-8") as fw:
        for i in range(0, len(nodes), NO_SPLIT_SIZE):
            seq = i // NO_SPLIT_SIZE + 1
            with open(os.path.join(no_dir, f"{seq:03d}.txt"),
                      "w", encoding="utf-8") as f:
                f.write("\n".join(nodes[i:i + NO_SPLIT_SIZE]))
            fw.write(f"https://raw.githubusercontent.com/{repo_name}"
                     f"/{branch_name}/no/{seq:03d}.txt\n")
    log_sink.emit(f"[{now_str()}] 📦 收尾去重: {len(batch_files)} 批次 "
                  f"→ {len(nodes)} 唯一节点 → "
                  f"{len(nodes) // NO_SPLIT_SIZE + 1} 个 no/ 分片")
    return len(nodes)


class MonitoredLock:
    """带持有者监控的 RLock（OOM 排查：显示谁持锁多久）。

    API 与 RLock 兼容（acquire/release/__enter__/__exit__），
    监控循环通过 holder_info() 查看当前持锁线程与时长——
    若 worker 全部卡在锁等待，监控直接显示"谁持锁 X 秒"。
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._holder = None
        self._since = 0.0
        self._depth = 0

    def acquire(self, *args, **kwargs):
        ok = self._lock.acquire(*args, **kwargs)
        if ok:
            self._depth += 1
            if self._depth == 1:
                self._holder = threading.current_thread().name
                self._since = time.time()
        return ok

    def release(self):
        self._lock.release()
        self._depth -= 1
        if self._depth == 0:
            self._holder = None

    def holder_info(self):
        """返回 (持有线程名, 已持有秒数) 或 None（无持有）。"""
        if self._holder:
            return self._holder, time.time() - self._since
        return None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()
        return False


class Collector:
    """多源并行节点收集器。

    同时启动 GitHub 搜索、网页搜索、Telegram 抓取三个独立线程。
    共享去重、缓存、批次输出，线程间通过锁保护共享状态。

    线程安全设计：
      - _state_lock（RLock）：保护所有共享的 set/dict/list
      - 每个线程持有独立的 HttpClient 实例（避免连接池竞争）
      - RateLimiter 仅绑定到 GitHub 线程的 HttpClient
      - 临界区极短（微秒级的 set/dict 操作），锁竞争可忽略
    """

    def __init__(self, token: str = "", queries: List[str] = None,
                 on_batch_flush: callable = None):
        """初始化收集器。

        Args:
            token: GitHub API token
            queries: 搜索关键词列表
            on_batch_flush: 批次刷盘回调，签名: (batch_id, file_path, node_count) -> None
        """
        self.token = token
        self.queries = queries or []

        # 全局配额管理器（所有 HttpClient 共享，消除统计盲区）
        self.quota_mgr = QuotaManager(max_per_hour=QUOTA_MAX_PER_HOUR)

        # API 速率门（所有 HttpClient 共享，削峰填谷 + 端点限速 + 在途并发）
        self.api_gate = ApiRateGate(max_per_minute=API_MAX_PER_MINUTE,
                                    pause_at_rate=API_PAUSE_AT_RATE,
                                    resume_at_rate=API_RESUME_AT_RATE,
                                    max_concurrency=API_MAX_CONCURRENCY)

        # 主 HTTP 客户端 + 线程局部存储（并行 fork/用户仓库用）
        self._main_http = HttpClient(token=token, quota_manager=self.quota_mgr,
                                     api_gate=self.api_gate)
        self._http_local = threading.local()

        # ── 共享状态（线程安全保护） ──
        self._state_lock = MonitoredLock()            # 保护下方所有 set/dict/list（带持锁监控）
        self.all_links: List[str] = []
        self.unique_nodes: Set[str] = set()            # 全局已收集节点 URI（收尾统计用，运行时少量）
        self.global_dedup_keys: Set[tuple] = set()     # (server, port, protocol) 去重（收尾统计用）
        self._batch_dedup: Set[str] = set()            # 当前批次内去重（≤5000 条小 set，内存优化）
        self._total_batch_nodes = 0                    # 批次累计节点数（含重复，统计展示）
        self._final_node_count = 0                     # 收尾去重后唯一节点数（统计展示）
        # ── 多进程解析池 + 内存预算（08104：GIL 1 核 + content 占满 11GB）──
        # 081XX：强制 fork context——进程池任务闭包引用 self（Collector
        # 含锁/会话/队列，spawn 下 pickle 必炸）；fork 靠内存继承工作。
        # Windows 本地无 fork → 降级线程池（_extract_pool=None）。
        # _pool_wd_queue：进程池看门狗回报队列——子进程把"开始/完成解析"
        # 时间戳报给主进程（fork 继承同一 pipe 连接），解决进程池路径
        # 看门狗盲区（子进程写 self._parse_watchdog 是内存副本，主进程
        # 读不到——08192 诊断确认，08241 大文件卡 21s 完全不可见）。
        self._extract_pool = None
        self._pool_wd_queue = None
        if EXTRACT_PROCESSES > 0:
            try:
                _mp_ctx = multiprocessing.get_context("fork")
                self._extract_pool = ProcessPoolExecutor(
                    max_workers=EXTRACT_PROCESSES, mp_context=_mp_ctx)
                self._pool_wd_queue = _mp_ctx.Queue()
            except Exception:
                self._extract_pool = None
                self._pool_wd_queue = None
        self._parsing_bytes = 0                        # 解析中+等待解析的 content 总字节（预算控制）
        self._budget_wait_count = 0                    # 预算等待次数（统计）
        # ── 08111：进程池调度（大文件优先，小文件补位）──
        self._pool_big_running = 0                     # 进程池中未完成的大文件数
        self._pool_small_running = 0                   # 进程池中未完成的小文件数
        self._parsing_sizes = set()                    # 当前解析中文件的 size(MB)（监控实时最大）
        # ── 08111：下载并发上限 + 限流退避 ──
        self._download_sem = threading.Semaphore(MAX_DOWNLOAD_CONCURRENCY)
        # 08112：失败计数改 60s 窗口统计（原共享计数被 288 线程共用 →
        # 2 秒刷屏 12 次退避 + 窗口续期 + 信号量泄漏 → 全 worker 卡死 2h）
        # 08131：改为 (time, reason) 元组——分类计数（监控块显示）+ 降级
        # 信号（网络类失败 ≥ 阈值）共用同一窗口
        self._raw_fail_times = deque()                 # 近 60s raw 下载失败 [(time, reason)]
        self._download_throttled_until = 0.0           # 限流降级截止时间（期间速率减半）
        self._pending_downloads = 0                    # 待下载文件数（监控）
        self._pending_download_bytes = 0               # 待下载文件总字节（监控）
        # ── 08113：全局连接速率节流（1s 窗口 ≤10 个新下载连接）──
        self._dl_gap_lock = threading.Lock()
        self._dl_gap_times = []                        # 最近 1s 内下载开始时间戳
        # ── 08113：分布统计（收尾输出，校准分流阈值 SMALL_REPO_CLONE_MB）──
        self._candidate_hist = {}                      # 候选文件数分布（分桶）
        self._repo_size_hist = {}                      # 仓库大小分布（分桶）
        # ── 08133：近 60s 解析统计（监控显示，读取时清理窗口防残留）──
        self._parsed_60s = deque()                     # [(time, size_mb)] 近 60s 解析完成
        # ── 08141：仓库处理统计（监控显示，读取时清理窗口）──
        self._repos_done_60s = deque()                 # [(time, size_mb)] 近 60s 完成仓库
        self._repos_done_total = 0                     # 累计完成仓库数
        self._repos_done_size_total = 0                # 累计完成仓库大小(KB)
        self._repos_parsed_total = 0                   # 累计解析过文件的仓库数
        # 解析过仓库的大小分布由 _repo_size_hist（收尾输出）覆盖
        self._seed_repos_done_total = 0                # 累计完成种子仓库数
        self._seed_repos_done_size_total = 0           # 累计完成种子仓库大小(KB)
        self._full_clone_60s = deque()                 # [(time, size_mb)] 近 60s 全量下载仓库
        self._full_clone_total = 0                     # 累计全量下载仓库数
        self._full_clone_size_total = 0                # 累计全量下载仓库大小(MB)
        # ── 08142：无节点黑名单（取样判断跳过，no_node_repos.txt 持久化）──
        self._repo_no_node = {}                        # repo(lower) -> 检查时间戳（30 天重试）
        self._sample_skipped_60s = deque()             # [(time,)] 近 60s 取样跳过仓库
        self._sample_skipped_total = 0                 # 累计取样跳过仓库数
        # 08161：跳过原因细分（监控显示 worker 时间去向）
        self._repos_no_cand_total = 0                  # 无候选文件仓库数
        self._repos_black_hit_total = 0                # 黑名单（404/403/无节点）命中跳过数
        self._total_parsed_files = 0                   # 累计下载解析的文件数（监控）
        self._total_parsed_mb = 0.0                    # 累计下载解析的文件总大小（MB）
        self._parsing_waiting = 0                      # 等待解析名额的文件数（预算排队）
        self._parsing_waiting_bytes = 0                # 等待解析的 content 总字节
        self._total_parsed_nodes = 0                   # 累计提取的有效节点数（监控）
        self._nodes_60s = deque()                      # 近60秒提取节点数: [(time, count)]
        self.seen_repos: Set[str] = set()  # 存储小写，大小写不敏感
        self.checked_count: int = 0
        self.processed_dir_shas: Set[str] = set()
        self.processed_file_shas: Set[str] = set()
        self.sha_cache: Dict[str, datetime] = {}
        self._branch_cache: Dict[str, str] = {}        # repo → 真实分支名
        self._repo_not_found: Set[str] = set()         # 404 仓库（本次运行）
        self._repo_forbidden: Set[str] = set()         # 403 访问拒绝（本次运行）
        self._repo_checking: Set[str] = set()          # 正在 API 检查中的仓库
        self._search_resume = threading.Event()         # Worker 唤醒搜索的信号
        self._search_resume.set()                       # 初始允许搜索
        # 日志走模块级单例 log_sink（http_client/quota_manager 共用，全程序统一）
        self._main_take_lock = threading.Lock()         # 取主队列互斥锁（原子性）
        self._worker_last_main = {}                      # thread name → 上次取主队列时间
        self._source_sem = threading.Semaphore(
            MAIN_SOURCE_LIMIT if MAIN_SOURCE_LIMIT > 0 else SHARED_POOL_WORKERS)  # 源头并发限制
        self._worker_state = {}                          # thread name → 当前状态（监控用）
        self._clone_sem = threading.Semaphore(PARTIAL_CLONE_CONCURRENCY)  # clone 并发限制
        self._repos_by_result = {"clone_ok": [], "clone_fail": []}  # 结果分类（汇总展示）
        self._quota_exhausted_times = []                # 配额耗尽时间（UTC）
        self._skip_counts = {"lang": 0, "size0": 0, "disabled": 0,
                             "stale": 0, "cached": 0}   # 跳过原因计数（汇总展示）
        # 08171 统计细分：仓库维度 + 文件维度（监控块 + 收尾统计）
        self._repos_cached_total = 0        # 已处理缓存跳过仓库数（= _skip_counts["cached"] 镜像）
        self._repos_partial_total = 0       # partial clone 仓库数
        self._repos_tree_total = 0          # tree API 仓库数
        self._repos_with_nodes_total = 0    # 提取出节点（≥1）的仓库数
        self._tag_counts = {}               # 标志位（[种子仓库]/[kw0]/[cd0]/[forkN]...）→ 仓库处理总数
        self._files_sha_skip_total = 0      # SHA 缓存命中跳过文件数
        self._files_with_nodes_total = 0    # 解析出节点（≥1）的文件数
        self._files_no_nodes_total = 0      # 解析无节点的文件数
        self._files_404_total = 0           # 下载 404 文件数
        self._files_timeout_total = 0       # 下载超时（timeout/idle/max_total）文件数
        self._backfill_count = 0                        # 信息补查次数（INFO_BACKFILL 统计）
        # 阶段进度（监控/完成日志显示）：种子/关键词/Code 处理到第几个
        self._seed_progress = ""                        # 如 "505/634"
        self._kw_total = 0                              # 关键词总数（_collect_keywords 设置）
        self._kw_progress = ""                          # 如 "3/105 第2/5页"
        self._cd_progress = ""                          # 如 "2/5 第3/5页"
        self._clone_repos = 0                           # Partial Clone 成功仓库数
        self._clone_files = 0                           # Partial Clone 列出文件数
        # ── Clone 统计（CLONE_FIRST 实验数据，_finalize 写 clone_stats.json）──
        self._clone_stats = []                          # 每次 clone: (repo, size_kb, time_s, files, ok)
        self._clone_fail_details = []                   # 失败明细: (repo, size_kb, reason)
        self._clone_fail_breakdown = {}                 # 失败分类计数: {"timeout": n, ...}
        self._clone_active = 0                          # 当前进行中 clone 数（监控采样）
        self._clone_active_peak = 0                     # clone 并发峰值（监控采样）
        self._cpu_load_peak = 0.0                       # CPU 负载峰值（监控采样，clone_stats）
        self._disk_free_min = 999.0                     # 磁盘可用最低（GB，监控采样）
        # ── clone 滑动窗口（监控近 60 秒统计）──
        self._clone_ok_window = deque()                 # [(完成时间, 仓库数, 文件数)] 成功窗口
        self._clone_traffic_window = deque()            # [(完成时间, 字节数)] 流量窗口（成功+失败）
        self._clone_fail_count = 0                      # 累计 clone 失败仓库数
        # ── OOM 定位诊断（08084：内存 100% OOM 终止，线程静默卡死）──
        self._dump_done = False                         # faulthandler 转储已触发（去重）
        self._parsing_active = 0                        # 当前正在解析的文件数
        # 08111：_parsing_max_mb（历史峰值）已废弃，改 _parsing_sizes 实时集合
        # ── 08103 监测（定位卡死与内存增长）：下载中计数 / 文件耗时 ──
        self._downloading_active = 0                    # 当前正在下载的文件数
        self._file_times = []                           # 每文件耗时: (下载s, 解析s, 大小MB)
        self._file_times_total = 0                      # 累计记录数（统计平均）
        # 081XX：_file_times 由 deque(500) 改为无上限 list——08241 分析
        # 需要全量耗时分布（每 5% 区间 + 最慢文件明细）定位慢解析；
        # 内存量：数万条 × 80B ≈ 几 MB，可控。
        self._last_activity = 0.0                       # 最近一次 _wlog 时间（监控降频信号）
        self._reset_waiting = False                     # 配额等待去重标志
        self._monitor_start = time.time()               # 监控基准
        self._net_bytes_start = self._read_net_bytes()  # 网络基准（程序启动）
        self._net_samples = []                           # [(time, bytes)] 10 秒采样
        # 08112：_net_peak（历史最大值）已删除——监控"峰值"改 60s 窗口
        # 最大速率，静止时不再残留旧峰值误导判断
        self._worker_local = threading.local()          # 线程独立前缀
        self._disc_seq = None                             # 发现队列 PriorityQueue 序号（分配在 run 中）
        self._worker_idle_since: Dict[str, float] = {} # Worker 闲置起始时间
        self._worker_repo_count: Dict[str, int] = {}   # Worker 处理仓库计数
        self._main_queue_total: int = 0                # 主队列总计（不含 fork/raw/用户）
        self.seen_cache: Dict[str, str] = {}            # 已处理仓库持久化
        self._sub_urls_seen: Set[str] = set()           # 订阅链接跨文件去重
        self._sub_urls_seen.clear()

        # 批次持久化（共享）
        self.batch_buffer: List[str] = []
        self.batch_id: int = 0
        self.batch_file_paths: List[str] = []
        self.failed_candidates_buffer: List[str] = []  # 解析失败记录
        self.on_batch_flush = on_batch_flush

        # 递归发现计数
        self.recursive_count = 0

        # ── 08174 异步下载管道 ──
        # worker 把确认要下载的 raw 链接丢进队列（放链接不占内存），
        # 固定数量下载线程消费（DOWNLOAD_WORKER_THREADS=48）——下载与
        # worker 解耦：worker 不再阻塞在下载/解析（08181 实测 worker 卡
        # 1300s 的根因之一），下载并发从"96 信号量+每仓库 4-8 线程叠加
        # 无上限"变成固定 48 线程。
        self._dl_queue = Queue(maxsize=DOWNLOAD_QUEUE_SIZE)
        self._dl_enqueue_lock = threading.Lock()   # 入队锁：同仓库文件连成一段
        self._dl_stop = threading.Event()          # 收尾停止信号
        self._dl_workers = []                      # 下载线程列表
        # 08174：下载中 content 预估字节（背压检查用——取链接时预留、
        # 下载完成释放；实际内存由解析预算 _parsing_bytes 接管）。
        # 旧版下载中 content 不计入预算 → 48 线程 × 大文件无上限
        # （08191 实测 6 分钟 11GB 内存的根因）。
        self._downloading_bytes = 0
        # 解析共享线程池（08174）：小文件解析不再每文件临时开 1 线程
        # （08181 峰值 87 并发、无上限是隐患），改固定 32 线程池，积压排队。
        self._parse_pool = ThreadPoolExecutor(
            max_workers=PARSE_THREAD_POOL_SIZE,
            thread_name_prefix="ParsePool")
        # 解析看门狗（08174）：{任务key: 解析开始时间}——超
        # PARSE_WATCHDOG_SECONDS 未完成 → 信号转储线程栈（只打印不取消）。
        self._parse_watchdog = {}
        self._parse_watchdog_lock = threading.Lock()
        self._watchdog_dumped = set()              # 已转储过的任务（去重）
        # 批次缓冲写盘锁（异步后多个下载线程可同时触发 _flush_batch）
        self._batch_flush_lock = threading.Lock()

        # ── 081XX 第 3 批：仓库记账器 + 结果队列 + 源头背压 ──
        # 记账器：repo -> RepoState dict（total/done/has_node/extracted/
        # added/mode/tmp_dir/stats/tag/branch/raw_links/repo_links/sub_urls）。
        # 作用：集中汇总"仓库全部文件是否处理完"（文件完成时间不一，由
        # 记账器自动聚合），done==total → 发"仓库完成"事件进结果队列，
        # work 消费后做后续（黑名单/统计/删 tmp/递归入队/订阅嗅探）。
        # 文件列表不驻留（worker 同步段持有，入队后释放）；tmp_dir 只存
        # 路径字符串——目录列表不会在内存堆积。
        self._trackers = {}
        self._trackers_lock = threading.Lock()
        self._result_queue = Queue(maxsize=RESULT_QUEUE_SIZE)
        self._backpressure_active = False   # 源头背压标志（滞回，见 _backpressure）

        # ── 独立组件（每线程一份） ──
        self.limiter = RateLimiter()  # 仅 GitHub 线程使用
        self._max_runtime = MAX_RUNTIME_SECONDS or None
        self._start_time = 0.0

        # 分渠道统计（每线程一份，_finalize 时汇总）
        self._channel_stats = {}  # channel_name → dict
        self._channel_new_nodes = {}  # channel_name → 本渠道新增计数（线程安全）

        # 加载持久化状态
        self.load_sha_cache()
        self.load_seen_cache()

        # 加载 404 仓库持久化（跨运行跳过死链接，避免每轮重复查询）
        try:
            if os.path.exists(NOT_FOUND_REPOS_FILE):
                with open(NOT_FOUND_REPOS_FILE, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip().lower()
                        if line:
                            self._repo_not_found.add(line)
                if self._repo_not_found:
                    self._wlog(f"已加载 404 仓库 {len(self._repo_not_found)} 条")
        except Exception:
            pass

        # 08142：加载无节点黑名单（取样判断跳过的仓库，NO_NODE_RETRY_DAYS
        # 天后重试——仓库可能"这次无节点下次有"）
        try:
            if os.path.exists(NO_NODE_REPOS_FILE):
                with open(NO_NODE_REPOS_FILE, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        parts = line.split("\t")
                        r = parts[0].lower()
                        ts = 0.0
                        if len(parts) > 1:
                            try:
                                ts = float(parts[1])
                            except ValueError:
                                ts = 0.0
                        self._repo_no_node[r] = ts
                if self._repo_no_node:
                    self._wlog(f"已加载无节点黑名单 {len(self._repo_no_node)} 条")
        except Exception:
            pass

    # ── 线程安全 HTTP 客户端 ──

    @property
    def http(self):
        """线程局部 HTTP 客户端。并行线程各自持有，主线程用默认。"""
        h = getattr(self._http_local, 'http', None)
        return h if h is not None else self._main_http

    @http.setter
    def http(self, val):
        self._http_local.http = val

    # ==================== 持久化 IO ====================

    @staticmethod
    def _safe_write(filepath: str, content: str, mode: str = "w"):
        """安全写入：先写 tmp 再 rename，防崩溃丢数据。"""
        if not SAFE_WRITE_ENABLED:
            with open(filepath, mode, encoding="utf-8") as f:
                f.write(content)
            return
        import tempfile, os as _os
        _dir = _os.path.dirname(filepath) or "."
        _fd, _tmp = tempfile.mkstemp(dir=_dir, suffix=".tmp")
        try:
            with open(_fd, mode, encoding="utf-8") as f:
                f.write(content)
            _os.replace(_tmp, filepath)
        except Exception:
            _os.unlink(_tmp)

    def load_seen_cache(self):
        """加载已处理仓库缓存（分片 pickle）。"""
        if not SEEN_REPOS_PERSIST_ENABLED:
            return
        if not os.path.isdir(SEEN_REPOS_DIR):
            self.seen_cache = {}
            return
        self.seen_cache = {}
        try:
            for fname in sorted(os.listdir(SEEN_REPOS_DIR)):
                if fname.endswith('.pkl'):
                    with open(os.path.join(SEEN_REPOS_DIR, fname), 'rb') as f:
                        self.seen_cache.update(pickle.load(f))
        except Exception:
            self.seen_cache = {}
        self._wlog(f"加载已处理仓库缓存 {len(self.seen_cache)} 条")

    def save_seen_cache(self):
        """保存已处理仓库缓存：按命中降序排列，淘汰低命中+过期条目，分片写入。"""
        if not SEEN_REPOS_PERSIST_ENABLED or not self.seen_cache:
            return
        os.makedirs(SEEN_REPOS_DIR, exist_ok=True)
        for old in os.listdir(SEEN_REPOS_DIR):
            if old.endswith('.pkl'):
                os.remove(os.path.join(SEEN_REPOS_DIR, old))

        # ── 按 (hits DESC, last_hit DESC) 排序 ──
        # 不按时间淘汰：pushed_at 比较已是最精确的过期判断。
        # 即使缓存半年的条目，只要 pushed_at 未变就正确跳过。
        def _sort_key(item):
            _repo, entry = item
            if isinstance(entry, str):
                return (0, "")
            return (entry.get("hits", 0), entry.get("last_hit", ""))

        sorted_items = sorted(self.seen_cache.items(), key=_sort_key, reverse=True)

        # ── 超量淘汰：保留前 SEEN_CACHE_MAX_ENTRIES ──
        if SEEN_CACHE_MAX_ENTRIES > 0 and len(sorted_items) > SEEN_CACHE_MAX_ENTRIES:
            evict = (len(sorted_items) - SEEN_CACHE_MAX_ENTRIES) + max(
                1, len(sorted_items) // SEEN_CACHE_EVICTION_RATIO)
            sorted_items = sorted_items[:-evict]

        # ── 分片写入 ──
        chunk, seq = {}, 0
        for k, v in sorted_items:
            chunk[k] = v
            if len(chunk) * 120 >= SEEN_REPOS_MAX_BYTES:
                with open(os.path.join(SEEN_REPOS_DIR, f"seen_{seq:04d}.pkl"), 'wb') as f:
                    pickle.dump(chunk, f)
                chunk.clear(); seq += 1
        if chunk:
            with open(os.path.join(SEEN_REPOS_DIR, f"seen_{seq:04d}.pkl"), 'wb') as f:
                pickle.dump(chunk, f)

    def _check_seen_cache(self, repo: str, pushed_at: str) -> bool:
        """检查已处理仓库缓存（解析维度）。True=已解析可跳过解析，False=需要处理。

        命中时递增 hits 计数并更新 last_hit 时间。
        兼容旧格式（纯 pushed_at 字符串）和新格式（dict with hits/parsed/traced）。
        """
        if not SEEN_REPOS_PERSIST_ENABLED or not pushed_at:
            return False
        r = repo.lower()
        entry = self.seen_cache.get(r)
        if entry is None:
            return False
        # 兼容旧格式：纯字符串 pushed_at
        if isinstance(entry, str):
            if entry == pushed_at:
                self.seen_cache[r] = {"pushed_at": pushed_at, "hits": 1,
                                      "last_hit": datetime.now(timezone.utc).isoformat(),
                                      "parsed": False, "traced": []}
                return False  # 旧格式无 parsed 信息 → 需要解析
            return False
        # 新格式：dict
        if entry.get("pushed_at") == pushed_at:
            entry["hits"] = entry.get("hits", 0) + 1
            entry["last_hit"] = datetime.now(timezone.utc).isoformat()
            return bool(entry.get("parsed", False))
        return False

    def _mark_seen_cache(self, repo: str, pushed_at: str, parsed: bool = True,
                         nodes_extracted: int = None, nodes_added: int = None,
                         language: str = ""):
        """标记仓库已处理（保留已有 hits，更新 pushed_at）。

        hits 语义 = 记录被处理的次数：新建记录初始 1，已有记录 +1
        （解析/跳过解析/追踪/跳过追踪四种情况都先经过此处或 _check_seen_cache，
        两处合计覆盖全部命中）。

        Args:
            parsed: 是否已解析过文件（默认 True，解析完成后调用）。
            nodes_extracted: 本仓库解析提取出的有效节点数（含重复）。
                None = 本次未解析（不更新历史值）；>=0 = 解析过（覆盖）。
            nodes_added: 全局去重后新增节点数（同上语义）。
            language: 仓库主要语言（记录用于统计/快速过滤）。
        """
        if SEEN_REPOS_PERSIST_ENABLED and pushed_at:
            r = repo.lower()
            existing = self.seen_cache.get(r, {})
            if isinstance(existing, str):
                existing = {}
            is_new = "pushed_at" not in existing
            existing["pushed_at"] = pushed_at
            if is_new:
                existing["hits"] = 1
            else:
                existing["hits"] = existing.get("hits", 0) + 1
            existing["last_hit"] = datetime.now(timezone.utc).isoformat()
            existing["parsed"] = parsed
            existing.setdefault("traced", [])   # 追踪过的层数集合
            if nodes_extracted is not None:
                existing["nodes_extracted"] = nodes_extracted
            if nodes_added is not None:
                existing["nodes_added"] = nodes_added
            if language:
                existing["language"] = language
            self.seen_cache[r] = existing
            if is_new:
                self._wlog(f"📝 已处理缓存: {repo}")

    def _mark_traced(self, repo: str, pushed_at: str, depth: int):
        """记录仓库在指定层数被追踪过（低层覆盖高层：1 覆盖 1、2、3...）。

        记录最近追踪时间 last_traced_at（全局一个，任意层追踪都刷新）。
        """
        if not SEEN_REPOS_PERSIST_ENABLED or not pushed_at:
            return
        r = repo.lower()
        existing = self.seen_cache.get(r, {})
        if isinstance(existing, str):
            existing = {}
        existing["pushed_at"] = pushed_at
        traced = existing.setdefault("traced", [])
        if depth not in traced:
            traced.append(depth)
        existing["last_traced_at"] = time.time()  # 最近追踪时间（超期重追踪用）
        existing.setdefault("hits", 1)
        existing["last_hit"] = datetime.now(timezone.utc).isoformat()
        self.seen_cache[r] = existing

    def _is_traced(self, repo: str, pushed_at: str, depth: int) -> bool:
        """检查仓库在指定层数是否已被追踪过（含低层覆盖 + 超期重试）。

        覆盖规则：已追踪层数 t ≤ depth 则视为已追踪（小数字覆盖大数字）。
        超期：距上次追踪超过 TRACE_RETRY_DAYS 天 → 需要再追踪。
        """
        if not pushed_at:
            return False
        entry = self.seen_cache.get(repo.lower())
        if not isinstance(entry, dict):
            return False
        if entry.get("pushed_at") != pushed_at:
            return False
        traced = entry.get("traced", [])
        if not traced or min(traced) > depth:
            return False  # 未覆盖（min 小数字覆盖大数字）
        if TRACE_RETRY_DAYS <= 0:
            return True  # 不重追踪
        last = entry.get("last_traced_at", 0)
        return time.time() - last < TRACE_RETRY_DAYS * 86400

    def _wlog(self, msg: str, **kwargs):
        """统一日志：优先用显式前缀，否则 fallback 到线程名。

        经 LogSink 队列输出：单消费者线程打印，不挤行、不阻塞生产线程。
        同时刷新 _last_activity（监控降频信号）——任何 work/主线程活动
        必然打日志；监控块走 log_sink.emit 不经此函数，不污染信号。
        """
        self._last_activity = time.time()
        tn = getattr(getattr(self, '_worker_local', None), 'prefix', None)
        if tn is None:
            tn = threading.current_thread().name
        prefix = f"[{tn}] " if tn else ""
        log_sink.emit(f"[{now_str()}] {prefix}{msg}")

    def _qs(self) -> str:
        """队列状态：主队列 + 发现队列 + 标志位分布。"""
        mq = getattr(self, '_task_queue', None)
        dq = getattr(self, '_disc_queue', None)
        # 统计扩展队列中各标志位的仓库数量（遍历队列元素样本，最多 500）
        tag_counts = {}
        if dq is not None:
            for _entry in list(dq.queue)[:500]:
                if not isinstance(_entry, tuple) or len(_entry) < 4:
                    continue  # sentinel 等异常元素跳过
                _item = _entry[3]
                if not isinstance(_item, tuple) or len(_item) < 3:
                    continue
                _kw = _item[2]
                if not isinstance(_kw, dict):
                    continue
                _tag = _kw.get("tag", "?")
                tag_counts[_tag] = tag_counts.get(_tag, 0) + 1
        tag_str = " ".join(f"{k}:{v}" for k, v in
                           sorted(tag_counts.items())) if tag_counts else "空"
        # Worker 工作率
        _w, _i, _r = self._worker_stats()
        return (f"主队列 {mq.qsize() if mq else 0}/{MAIN_QUEUE_SIZE} "
                f"发现队列 {dq.qsize() if dq else 0}/{DISCOVERY_QUEUE_SIZE} "
                f"[{tag_str}] | Worker {_w}忙/{_i}闲({_r}%)")

    def _qt(self) -> str:
        """配额状态：剩余/上限 + UTC 时间（看距整点刷新还有多久）。"""
        from datetime import datetime, timezone
        utc = datetime.now(timezone.utc).strftime('%H:%M')
        return f"剩余配额 {self.quota_mgr.remaining()}/{QUOTA_MAX_PER_HOUR} {utc}UTC"

    # ── 系统观测（监控线程） ──

    @staticmethod
    def _read_net_bytes() -> int:
        """读 /proc/net/dev 所有接口接收字节总和（系统级总网络下载量）。"""
        try:
            with open('/proc/net/dev') as f:
                lines = f.readlines()[2:]
            return sum(int(l.split()[1]) for l in lines)
        except Exception:
            return 0

    @staticmethod
    def _read_mem_gb() -> tuple:
        """读内存使用/总量（GB）。"""
        try:
            with open('/proc/meminfo') as f:
                d = {}
                for line in f:
                    k, v = line.split(':', 1)
                    d[k] = int(v.strip().split()[0])
            used = (d.get('MemTotal', 0) - d.get('MemAvailable', d.get('MemFree', 0))) / 1024 / 1024
            total = d.get('MemTotal', 0) / 1024 / 1024
            return used, total
        except Exception:
            return 0.0, 0.0

    @staticmethod
    def _read_disk_gb() -> tuple:
        """读当前目录所在磁盘可用/总量（GB）。

        Clone-First 模式下 30 并发 git clone 瞬时占用可达 5-15GB，
        监控显示验证磁盘压力（GA runner 磁盘 70GB+）。
        """
        try:
            import shutil
            usage = shutil.disk_usage(os.getcwd())
            return usage.free / 1024 ** 3, usage.total / 1024 ** 3
        except Exception:
            return 0.0, 0.0

    def _set_worker_state(self, what: str):
        """记录当前 Worker 状态（监控显示用）。"""
        try:
            self._worker_state[threading.current_thread().name] = \
                {"what": what, "since": time.time()}
        except Exception:
            pass

    @staticmethod
    def _read_python_rss_gb() -> float:
        """读本进程 RSS（GB）——/proc/self/status 的 VmRSS。

        区分"程序自身内存"vs"系统内存"（GA runner 其他进程干扰），
        08084 OOM 时程序 RSS 是否确实接近 15.6GB 由它确认。
        """
        try:
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) / 1024 / 1024  # kB → GB
        except Exception:
            pass
        return 0.0

    def _worker_stats(self) -> tuple:
        """Worker 统计：(工作中数, 空闲数, 工作率%)。

        工作 = 状态非"空闲"且非"等待配额"。
        """
        working = 0
        for st in self._worker_state.values():
            w = st.get("what", "")
            if w and w not in ("空闲", "等待配额"):
                working += 1
        idle = SHARED_POOL_WORKERS - working
        rate = working * 100 // SHARED_POOL_WORKERS
        return working, idle, rate

    def _net_status(self) -> dict:
        """网络状态（近 60 秒滑动窗口）：总下载/平均/峰值。

        独立 10 秒采样循环维护 _net_samples（60 秒窗口内 6 个点），
        修复峰值恒 0（之前采样频率 = 输出频率）。
        """
        now = time.time()
        # 当前采样（若超过 10 秒未采样则补一个点）
        cur = self._read_net_bytes()
        if not self._net_samples or now - self._net_samples[-1][0] >= 10:
            self._net_samples.append((now, cur))
            while self._net_samples and now - self._net_samples[0][0] > 60:
                self._net_samples.pop(0)
        if len(self._net_samples) < 2:
            return {"total_mb": 0.0, "avg_mb": 0.0, "peak_mb": 0.0}
        # 近 60 秒：首尾差值
        dt = self._net_samples[-1][0] - self._net_samples[0][0]
        db = self._net_samples[-1][1] - self._net_samples[0][1]
        total_mb = max(0, db) / 1024 / 1024
        avg_mb = total_mb / dt if dt > 0 else 0.0
        # 峰值：相邻采样点差值的最大速率
        # 08112：返回 60 秒窗口内峰值（原返回历史最大值 _net_peak——
        # 静止 2 小时仍显示 07:53 的 26.87MB/s，误导判断，已删除）
        peak_mb = 0.0
        for i in range(1, len(self._net_samples)):
            dti = self._net_samples[i][0] - self._net_samples[i - 1][0]
            dbi = self._net_samples[i][1] - self._net_samples[i - 1][1]
            if dti > 0:
                peak_mb = max(peak_mb, dbi / dti / 1024 / 1024)
        return {"total_mb": total_mb, "avg_mb": avg_mb, "peak_mb": peak_mb}

    def _monitor_loop(self):
        """监控线程：每 MONITOR_INTERVAL 秒输出一个集中监控块。

        内容（全部近 60 秒数据）：CPU 负载比例/内存比例/网络/API/队列/Worker。
        采样循环：每 10 秒维护网络采样点（独立于输出）。
        """
        last_sample = 0.0
        while True:
            time.sleep(5)  # 5 秒循环（内部负责 10 秒采样 + 60 秒输出）
            try:
                now = time.time()
                # 网络 10 秒采样（独立维护）
                if now - last_sample >= 10:
                    last_sample = now
                    self._net_status()
                # 动态输出间隔（活动信号判断）：配额耗尽且 60 秒无任何日志
                # （所有 work 真停，干等配额恢复）→ 10 分钟一次，避免刷屏；
                # work 仍在处理（0 API 仓库，_last_activity 持续刷新）→
                # 保持分钟级（额度耗尽期间 work 还能干活，需要监控数据）。
                # interval 每 5 秒循环重算：配额恢复（exceeded False）或
                # work 恢复活动（新日志刷新 _last_activity）→ 下一轮立即
                # 回 60 秒，last_out 已过期 → 立刻输出第一条，及时恢复分钟级。
                _w, _i, _r = self._worker_stats()
                # 08174 解析看门狗：每 5s 检查解析任务是否超时（进入 CPU
                # 解析开始计时）——超 PARSE_WATCHDOG_SECONDS 未完成 → 打印
                # 任务信息 + 触发 SIGUSR1 信号转储线程栈（只打印不取消；
                # 大文件解析几分钟正常，faulthandler 转储可定位卡死的正则）。
                try:
                    _wd_now = time.time()
                    # 081XX：先收进程池看门狗回报（子进程 start/done 事件
                    # 经 Queue 送达，主进程据此维护进程池路径的计时字典——
                    # 之前子进程写字典主进程读不到，进程池路径完全盲区）
                    _pq = self._pool_wd_queue
                    if _pq is not None:
                        while True:
                            try:
                                _ev = _pq.get_nowait()
                            except Exception:
                                break
                            if _ev is None or len(_ev) < 2:
                                continue
                            if _ev[0] == "start" and len(_ev) >= 3:
                                with self._parse_watchdog_lock:
                                    self._parse_watchdog[_ev[1]] = _ev[2]
                            elif _ev[0] == "done":
                                with self._parse_watchdog_lock:
                                    self._parse_watchdog.pop(_ev[1], None)
                    _wd_overdue = []
                    with self._parse_watchdog_lock:
                        for _k, _t0 in list(self._parse_watchdog.items()):
                            if _wd_now - _t0 > PARSE_WATCHDOG_SECONDS \
                                    and _k not in self._watchdog_dumped:
                                _wd_overdue.append((_k, _wd_now - _t0))
                    for _k, _age in _wd_overdue:
                        self._watchdog_dumped.add(_k)
                        self._wlog(f"🚨 解析超时 {_age:.0f}s 未完成：{_k}"
                              f"（触发线程转储，仅打印不取消）")
                        try:
                            os.kill(os.getpid(), signal.SIGUSR1)
                        except Exception:
                            pass
                except Exception:
                    pass
                if self.quota_mgr.exceeded and now - self._last_activity > 60:
                    interval = MONITOR_INTERVAL * 10
                else:
                    interval = MONITOR_INTERVAL
                if now - self._monitor_start < interval:
                    continue
                last_out = getattr(self, '_last_monitor_out', None)
                if last_out is not None and now - last_out < interval:
                    continue
                self._last_monitor_out = now
                elapsed = now - self._monitor_start
                try:
                    load = os.getloadavg()[0]
                    cpu_pct = min(100, load / 2.0 * 100)  # 2 核，满载 = 2.0
                except Exception:
                    load, cpu_pct = -1, 0
                used_gb, total_gb = self._read_mem_gb()
                mem_pct = used_gb / total_gb * 100 if total_gb else 0
                disk_free_gb, disk_total_gb = self._read_disk_gb()
                py_rss_gb = self._read_python_rss_gb()
                # 资源峰值采样（clone_stats.json 用）
                self._cpu_load_peak = max(self._cpu_load_peak, load)
                self._disk_free_min = min(self._disk_free_min, disk_free_gb)
                # ── OOM 防护与定位（08141：内存 96% 持续 → OOM 进程被杀无日志）──
                # 1. gc.collect() 定期（每 5 分钟）：回收循环引用对象，辅助降内存
                # 2. 内存 > 75% 连续预警（每次 60s 监控打一条，仅 >75% 时）
                # 3. 内存 > 85% 且未转储过 → faulthandler 打印线程栈
                #    （08084 OOM 前 30 分钟线程静默卡死的定位工具；85% 而非
                #    原 80%——转储本身耗内存，留余量避免转储加剧 OOM）
                if int(elapsed // 300) > getattr(self, '_gc_round', -1):
                    self._gc_round = int(elapsed // 300)
                    gc.collect()
                if mem_pct > 75:
                    self._wlog(f"⚠️ 内存 {mem_pct:.0f}% 偏高（>75%），"
                          f"RSS {py_rss_gb:.1f}GB")
                if mem_pct > 85 and not self._dump_done:
                    self._dump_done = True
                    self._wlog(f"🚨 内存 {mem_pct:.0f}% 超过 85%，"
                          f"触发线程转储（仅一次，定位静默卡死）")
                    # 081XX：与看门狗同文件落盘（stderr 会被 GA 丢弃）
                    faulthandler.dump_traceback(
                        file=getattr(self, "_wd_dump_f", None))
                net = self._net_status()
                # 08131：raw 下载失败分类（60s 窗口清理，先于 _raw_fail_n
                # 统计执行——保证下方失败数/连接数不含过期条目）
                _dl_fail_txt = self._raw_fail_summary()
                if self._download_throttled_until > now:
                    _dl_fail_txt += (
                        f"降级{MAX_RAW_DOWNLOAD_CONNECTS_PER_SEC // 2}/s"
                        f"(剩{max(0, self._download_throttled_until - now):.0f}s) | ")
                # 08133：近 60s raw 成功窗口（读取时过滤防旧数据残留）——
                # 成功下载在 http_client._raw_window；失败在 _raw_fail_times
                _raw_ok = [x for x in getattr(
                    getattr(self, 'http', None), '_raw_window', [])
                    if now - x[0] <= 60]
                _raw_ok_n = len(_raw_ok)
                _raw_ok_mb = sum(b for _, b in _raw_ok) / 1024 / 1024
                _raw_fail_n = len(self._raw_fail_times)  # 上面 summary 已清理窗口
                # 近 60s 解析完成窗口（清理防残留）
                if self._parsed_60s:
                    _p60 = [(t, s) for t, s in self._parsed_60s if now - t <= 60]
                    self._parsed_60s = deque(_p60)
                else:
                    _p60 = []
                _parsed_60s_n = len(_p60)
                _parsed_60s_mb = sum(s for _, s in _p60)
                # clone 近 60 秒窗口统计（成功仓库/文件 + 流量含失败）
                _t60 = now - 60
                _ok60_repos = sum(r for _t, r, _f in self._clone_ok_window if _t > _t60)
                _ok60_files = sum(f for _t, _r, f in self._clone_ok_window if _t > _t60)
                _tr60_mb = sum(b for _t, b in self._clone_traffic_window
                               if _t > _t60) / 1024 / 1024
                # Worker 状态（全部按编号排序）
                wc = []
                for i in range(SHARED_POOL_WORKERS):
                    st = self._worker_state.get(f"W-{i}",
                                                {"what": "无记录", "since": now})
                    wc.append(f"W-{i} {st['what']}({now-st['since']:.0f}s)")
                # ── OOM 诊断采样：log_sink 健康 / 解析中文件 / 锁持有 ──
                _log_q = log_sink.qsize()
                _log_ok = "OK" if log_sink.consumer_alive() else "DEAD"
                _lk = self._state_lock.holder_info()
                _lk_txt = f"锁: {_lk[0]} {_lk[1]:.0f}s" if _lk else "锁: 空闲"
                _now_dt = datetime.fromtimestamp(now, tz=timezone.utc)
                # 08121：监控行重排——下载/解析/API 分行，名称直白，同类合并
                _dl_waiting = max(0, self._pending_downloads - self._downloading_active)
                _pool_total = self._pool_big_running + self._pool_small_running
                lines = [
                    f"📊 [{_now_dt.strftime('%H:%M')} UTC] 运行 {elapsed:.0f}s",
                    f"   CPU: {cpu_pct:.0f}% (负载 {load:.2f}/2核) | "
                    f"内存: {mem_pct:.0f}% ({used_gb:.1f}/{total_gb:.1f}GB) | "
                    f"RSS: {py_rss_gb:.1f}GB | "
                    f"磁盘: {disk_free_gb:.1f}/{disk_total_gb:.0f}GB 可用",
                    f"   网络: 60s下载 {net['total_mb']/1024:.2f}GB | "
                    f"平均 {net['avg_mb']:.2f}MB/s | 峰值 {net['peak_mb']:.2f}MB/s",
                    f"   raw下载: 进行中 {self._downloading_active}/{MAX_DOWNLOAD_CONCURRENCY}"
                    f"(空闲许可{self._download_sem._value}) | "
                    f"待下载 {self._pending_downloads}文件"
                    f"/{self._pending_download_bytes/1024/1024:.0f}MB"
                    f"(等连接配额{_dl_waiting}) | "
                    f"队列 {self._dl_queue.qsize()}/{DOWNLOAD_QUEUE_SIZE} | "
                    f"下载中内存 {self._downloading_bytes/1024/1024:.0f}MB | "
                    f"60s成功 {_raw_ok_n}文件/{_raw_ok_mb:.1f}MB | "
                    f"60s连接 {_raw_ok_n + _raw_fail_n} | {_dl_fail_txt}",
                    f"   解析: 队列 {self._parsing_active}文件"
                    f"(含排队/最大{self._parsing_cur_max_mb():.0f}MB) | "
                    f"近60s解析 {_parsed_60s_n}文件/{_parsed_60s_mb:.1f}MB | "
                    f"等内存预算 {self._parsing_waiting}文件"
                    f"/{self._parsing_waiting_bytes/1024/1024:.0f}MB | "
                    f"内存占用 {self._parsing_bytes/1024/1024:.0f}"
                    f"/{DOWNLOAD_MEMORY_BUDGET_MB}MB | "
                    f"进程池 {min(_pool_total, EXTRACT_PROCESSES)}/{EXTRACT_PROCESSES}"
                    f"(排队{max(0, _pool_total - EXTRACT_PROCESSES)}) | "
                    f"节点 60s {sum(c for _t, c in self._nodes_60s)}"
                    f"/累计 {self._total_parsed_nodes} | "
                    f"累计解析 {self._total_parsed_files}文件"
                    f"/{self._total_parsed_mb/1024:.1f}GB"
                    f" | 文件: sha跳过{self._files_sha_skip_total}"
                    f"/有节点{self._files_with_nodes_total}"
                    f"/无节点{self._files_no_nodes_total}"
                    f"/404:{self._files_404_total}"
                    f"/超时{self._files_timeout_total}",
                    f"   API: 剩余 {self.quota_mgr.remaining()}/{QUOTA_MAX_PER_HOUR} | "
                    f"放行 {self.api_gate.current_rate()}/分钟 | "
                    f"clone 成功{self._clone_repos}/失败{self._clone_fail_count}"
                    f" | clone并发 {self._clone_active}/{PARTIAL_CLONE_CONCURRENCY}"
                    f"(峰值{self._clone_active_peak})",
                    f"   仓库: 近60s完成 {self._repos_done_60s_clean(now)} | "
                    f"累计完成 {self._repos_done_total}仓库"
                    f"/{self._repos_done_size_total:.0f}MB | "
                    f"解析过 {self._repos_parsed_total}仓库"
                    f"(有节点{self._repos_with_nodes_total}) | "
                    f"跳过: 取样{self._sample_skipped_total}"
                    f"(60s {self._sample_skipped_60s_clean(now)})/"
                    f"无候选{self._repos_no_cand_total}/"
                    f"黑名单{self._repos_black_hit_total}/"
                    f"缓存{self._repos_cached_total}/"
                    f"未更新{self._skip_counts.get('stale', 0)}/"
                    f"禁用{self._skip_counts.get('disabled', 0)}/"
                    f"大小0{self._skip_counts.get('size0', 0)} | "
                    f"分流: 全量{self._full_clone_total}仓库"
                    f"/{self._full_clone_size_total:.0f}MB"
                    f"(60s {len(self._full_clone_60s_clean(now))})/"
                    f"partial{self._repos_partial_total}/"
                    f"tree{self._repos_tree_total} | "
                    f"按标志: " + " ".join(
                        f"{k[1:-1]}{v}" for k, v in
                        sorted(self._tag_counts.items(),
                               key=lambda kv: -kv[1])[:8]),
                    f"   诊断: log队列 {_log_q}/{_log_ok} | "
                    f"线程 {threading.active_count()} | {_lk_txt}",
                    f"   进度: 种子 {self._seed_progress or '-'} | "
                    f"关键词 {self._kw_progress or '-'} | "
                    f"Code {self._cd_progress or '-'}",
                    f"   {self._qs()}",
                    f"   Worker: {_w}忙/{_i}闲({_r}%) | " + " | ".join(wc),
                ]
                _ts = now_str()
                _block = f"[{_ts}] " + f"\n[{_ts}] ".join(lines)
                # 08111：高优先级通道——刷屏日志再多监控块也不丢（盲区根因）
                log_sink.emit_priority(_block)
                # 08171：监控块双通道落盘——stdout 通道死亡（LogSink 消费者
                # 异常/管道问题）时文件仍有心跳，确认程序存活（启动时清空，
                # 每轮一份 log/monitor.log）
                try:
                    if not getattr(self, '_monitor_file_inited', False):
                        self._monitor_file_inited = True
                        os.makedirs(os.path.dirname(LOG_MONITOR_FILE),
                                    exist_ok=True)
                        with open(LOG_MONITOR_FILE, "w",
                                  encoding="utf-8") as _mf:
                            _mf.write(f"[{_ts}] 监控落盘启动\n")
                    with open(LOG_MONITOR_FILE, "a",
                              encoding="utf-8") as _mf:
                        _mf.write(_block + "\n")
                except Exception:
                    pass  # 落盘失败不影响主流程（stdout 正常时不需要它）
            except Exception as e:
                # 首次异常打印一次（定位监控静默根因），之后静默不影响主流程
                if not getattr(self, '_monitor_err_logged', False):
                    self._monitor_err_logged = True
                    self._wlog(f"⚠️ 监控输出异常（仅记录一次）: {e}")

    def _disc_put(self, item: tuple, label: str = ""):
        """放入发现队列（PriorityQueue）。

        优先级：不需要追踪（priority 0）> 需要追踪（priority 1），
        从标志位派生——不追踪的仓库消费即减（降队列水位），
        需追踪的会产生新条目（增队列压力），延后处理。
        背压：队列 ≥ DISC_PUT_BACKPRESSURE 时等待（最后防线，item 不丢）。
        """
        dq = getattr(self, '_disc_queue', None)
        if not dq:
            return False
        tag = item[2].get("tag", "") if len(item) > 2 else ""
        # 08141 配额末段（方案 B）：priority 动态化——末段需 API 仓库
        # （需追踪 = 非最深层）优先消费（priority 0），消化剩余配额；
        # 平时保持"不追踪（最深层）先消费"（priority 0）。
        if self._quota_endgame():
            priority = 0 if self._should_trace(tag) else 1
        else:
            priority = 1 if self._should_trace(tag) else 0
        while dq.qsize() >= DISC_PUT_BACKPRESSURE:
            if self._should_stop():
                return False
            time.sleep(5)
        try:
            dq.put((priority, time.time(), next(self._disc_seq), item),
                   timeout=QUEUE_PUT_TIMEOUT_SECONDS)
            return True
        except Exception:
            if label:
                fn = item[1] if len(item) > 1 else "?"
                self._wlog(f"🗑️ 发现队列满，丢弃{label} {fn}")
            return False

    def _main_put(self, item: tuple, timeout=None):
        """放入主队列（PriorityQueue，按入队时间排序，旧的优先）。"""
        mq = getattr(self, '_task_queue', None)
        if not mq:
            return False
        try:
            mq.put((time.time(), next(self._disc_seq), item),
                   timeout=timeout if timeout is not None else QUEUE_PUT_TIMEOUT_SECONDS)
            return True
        except Exception:
            return False

    def _wait_reset(self):
        """配额耗尽等待（多线程去重 + 队列上下文显示）。

        多个 Worker 同时耗尽时，只有第一个打印日志，其余静默等待。
        """
        first = False
        with self._state_lock:
            if not self._reset_waiting:
                self._reset_waiting = True
                first = True
        if first:
            self._wlog(f"⏳ 配额耗尽 | {self._qs()}")
            # 08171：零 API 模式提示——耗尽期间缺信息任务（需 API 补查
            # repo info）由 _requeue_for_quota 降级队尾，worker 优先处理
            # 有信息任务（零 API 的 clone/raw 路径），配额恢复后补查。
            self._wlog(f"🔋 零 API 模式：缺信息任务降级队尾，"
                  f"优先处理无需核心 API 的仓库（clone/raw 本地解析）")
            self._quota_exhausted_times.append(
                datetime.now(timezone.utc).strftime('%H:%M'))
        self._set_worker_state("等待配额")
        try:
            return self.quota_mgr.wait_for_reset(self._runtime_exceeded)
        finally:
            with self._state_lock:
                self._reset_waiting = False

    def _repos_done_60s_clean(self, now: float) -> str:
        """近 60s 完成仓库数/大小（读取时清理窗口防残留，08141）。"""
        self._repos_done_60s = deque(
            (t, s) for t, s in self._repos_done_60s if now - t <= 60)
        _n = len(self._repos_done_60s)
        _mb = sum(s for _, s in self._repos_done_60s)
        return f"{_n}仓库/{_mb:.0f}MB"

    def _sample_skipped_60s_clean(self, now: float) -> int:
        """近 60s 取样跳过仓库数（读取时清理窗口防残留，08142）。"""
        self._sample_skipped_60s = deque(
            t for t in self._sample_skipped_60s if now - t <= 60)
        return len(self._sample_skipped_60s)

    def _full_clone_60s_clean(self, now: float) -> list:
        """近 60s 全量下载仓库（读取时清理窗口防残留，08141）。"""
        self._full_clone_60s = deque(
            (t, s) for t, s in self._full_clone_60s if now - t <= 60)
        return list(self._full_clone_60s)

    def _quota_endgame(self) -> bool:
        """配额末段判断（08141）：距整点 < QUOTA_ENDGAME_MINUTES 分钟
        且核心 API 还有剩余 → 末段模式（需 API 任务优先，消化配额）。
        """
        try:
            _rem = self.quota_mgr.remaining()
            _sec_to_hour = 3600 - (time.time() % 3600)
            active = _rem > 0 and _sec_to_hour < QUOTA_ENDGAME_MINUTES * 60
            # 08171：末段状态变化日志（触发/退出各打一次，跨线程去重——
            # 此函数被 _disc_put/worker 频繁调用，仅在状态翻转时输出）
            if active != getattr(self, '_endgame_active', False):
                self._endgame_active = active
                if active:
                    self._wlog(f"🔚 末段模式：距整点 {_sec_to_hour/60:.0f}min，"
                          f"剩余配额 {_rem}——需 API 仓库优先，"
                          f"允许取主队列消化配额")
                else:
                    self._wlog(f"🔚 末段模式结束（整点已过或配额耗尽），"
                          f"回到常规策略（零 API 优先、disc 优先）")
            return active
        except Exception:
            return False

    def _requeue_for_quota(self, item: tuple, from_queue):
        """配额耗尽时，缺信息任务降级优先级放回原队列（排到队尾）。

        原理：PriorityQueue 队头阻塞——缺信息任务（需 API 补查 repo info）
        放回队头附近会堵住后面的有信息任务；降级 priority/ts 排到队尾后，
        worker 自然先处理有信息任务（零 API clone 路径），配额恢复后
        缺信息任务再被正常取出补查处理。任务不丢弃。
        """
        try:
            if from_queue is getattr(self, '_disc_queue', None):
                # 扩展队列元素: (priority, ts, seq, item) → priority 降级
                from_queue.put((1000, time.time(), next(self._disc_seq), item))
            elif from_queue is getattr(self, '_task_queue', None):
                # 主队列元素: (ts, seq, item) → ts 推后到队尾
                from_queue.put((time.time() + 1e9, next(self._disc_seq), item))
            else:
                return  # 未知队列，任务随 finally task_done 丢弃（极少发生）
            from_queue.task_done()  # 抵消本次取出计数（新元素已入队）
        except Exception:
            pass

    def _add_seen(self, repo: str):
        """标记仓库为已处理（大小写不敏感），线程安全。"""
        with self._state_lock:
            self.seen_repos.add(repo.lower())

    def _is_seen(self, repo: str) -> bool:
        """检查仓库是否已处理（大小写不敏感），线程安全。"""
        with self._state_lock:
            return repo.lower() in self.seen_repos

    def _check_and_add_seen(self, repo: str) -> bool:
        """原子操作：检查仓库是否已处理，未处理则标记。

        Returns: False=已处理可跳过，True=新仓库已标记应处理。
        线程安全：检查和标记在同一个锁内完成，无竞态窗口。
        """
        r = repo.lower()
        with self._state_lock:
            if r in self.seen_repos:
                return False
            self.seen_repos.add(r)
            return True

    def _wait_queue_slot(self, mq) -> bool:
        """队列背压：≥ MAIN_QUEUE_PAUSE_AT 暂停，等 Worker 消费到
        < MAIN_QUEUE_RESUME_AT（08141：大队列 3000 下的滞回阈值）。
        Returns: False=运行超时应终止, True=可继续。
        """
        if mq.qsize() < MAIN_QUEUE_PAUSE_AT:
            return True
        self._search_resume.clear()
        self._wlog(f"⏸️  主队列 ≥ {MAIN_QUEUE_PAUSE_AT}"
              f"（{mq.qsize()}/{mq.maxsize}），搜索暂停")
        while mq.qsize() >= MAIN_QUEUE_RESUME_AT:
            if self._runtime_exceeded():
                self._search_resume.set()
                return False
            self._search_resume.wait(timeout=30)
        self._wlog(f"▶️ 主队列 < {MAIN_QUEUE_RESUME_AT}"
              f"（{mq.qsize()}/{mq.maxsize}），搜索恢复")
        self._search_resume.set()
        return True

    def _is_repo_dead(self, repo: str) -> bool:
        """检查仓库是否已知不可达（404/403，大小写不敏感）。"""
        r = repo.lower()
        # 08142：无节点黑名单（取样判断跳过；超 NO_NODE_RETRY_DAYS 重试）
        _nn = self._repo_no_node.get(r)
        if _nn is not None and \
                time.time() - _nn < NO_NODE_RETRY_DAYS * 86400:
            return True
        return r in self._repo_not_found or r in self._repo_forbidden

    def _mark_repo_no_node(self, repo: str):
        """标记无节点黑名单（08142：取样全无节点跳过的仓库）。"""
        with self._state_lock:
            self._repo_no_node[repo.lower()] = time.time()

    def _mark_repo_not_found(self, repo: str):
        """记录 404 仓库（本轮内存 + 持久化文件追加，跨运行跳过）。"""
        r = repo.lower()
        with self._state_lock:
            if r in self._repo_not_found:
                return
            self._repo_not_found.add(r)
        try:
            with open(NOT_FOUND_REPOS_FILE, "a", encoding="utf-8") as f:
                f.write(r + "\n")
        except Exception:
            pass

    def _mark_repo_forbidden(self, repo: str):
        """标记仓库为 403 访问拒绝（私有/被封），本次运行内不再重试。"""
        self._repo_forbidden.add(repo.lower())

    def load_sha_cache(self):
        """加载 SHA 缓存（分片 pickle 目录）。"""
        if not os.path.isdir(SHA_CACHE_DIR):
            self.sha_cache = {}
            return
        self.sha_cache = {}
        total = 0
        try:
            for fname in sorted(os.listdir(SHA_CACHE_DIR)):
                if fname.endswith('.pkl'):
                    with open(os.path.join(SHA_CACHE_DIR, fname), 'rb') as f:
                        chunk = pickle.load(f)
                        self.sha_cache.update(chunk)
                        total += len(chunk)
        except Exception as e:
            self._wlog(f"加载 SHA 缓存失败: {e}")
            self.sha_cache = {}
            return
        self._wlog(f"加载 SHA 缓存 {total} 条 "
              f"({len(os.listdir(SHA_CACHE_DIR))} 分片)")

    def save_sha_cache(self):
        """保存 SHA 缓存到分片目录。每片 ≤ SHA_CACHE_MAX_BYTES。

        策略：
          1. 清理 30 天前的条目
          2. 按时间排序，保留最近的 SHA_CACHE_MAX_ENTRIES 条（若设了上限）
          3. 分片写入，每片不超过 GitHub 100MB 限制

        整个方法加锁：并行下载线程同时写 sha_cache，遍历时不能有并发写入。
        """
        with self._state_lock:
            cutoff = datetime.now(timezone.utc) - timedelta(days=30)
            self.sha_cache = {sha: ts for sha, ts in self.sha_cache.items() if ts > cutoff}
            if SHA_CACHE_MAX_ENTRIES > 0 and len(self.sha_cache) > SHA_CACHE_MAX_ENTRIES:
                sorted_items = sorted(self.sha_cache.items(), key=lambda x: x[1], reverse=True)
                self.sha_cache = dict(sorted_items[:SHA_CACHE_MAX_ENTRIES])

            os.makedirs(SHA_CACHE_DIR, exist_ok=True)
            # 清除旧分片
            for old in os.listdir(SHA_CACHE_DIR):
                if old.endswith('.pkl'):
                    os.remove(os.path.join(SHA_CACHE_DIR, old))

            # 按时间戳排序，分片写入
            sorted_items = sorted(self.sha_cache.items(), key=lambda x: x[1])
            chunk = {}
            seq = 0
            for sha, ts in sorted_items:
                chunk[sha] = ts
                # 估算：每个条目约 60 字节（sha40 + datetime20）
                if len(chunk) * 60 >= SHA_CACHE_MAX_BYTES:
                    self._write_sha_chunk(seq, chunk)
                    chunk.clear()
                    seq += 1
            if chunk:
                self._write_sha_chunk(seq, chunk)

            total_files = seq + (1 if chunk else 0)
            self._wlog(f"SHA 缓存已保存: {len(self.sha_cache)} 条, {total_files} 分片")

    @staticmethod
    def _write_sha_chunk(seq: int, chunk: dict):
        """写入单个 SHA 缓存分片。"""
        path = os.path.join(SHA_CACHE_DIR, f"sha_{seq:04d}.pkl")
        try:
            with open(path, 'wb') as f:
                pickle.dump(chunk, f)
        except Exception as e:
            self._wlog(f"SHA 分片写入失败: {e}")

    def _sha_in_cache(self, sha: str) -> bool:
        """检查文件 SHA 是否已在持久化缓存中（只读，不写入）。

        SHA 是 Git 内容哈希 — 相同 SHA 永远意味着相同内容。
        命中时更新 LRU 时间戳，保证高频 SHA 不会被淘汰。

        注意：此方法只检查不写入。SHA 的写入在 _handle_one_file 下载成功后进行，
        确保限流导致下载失败的文件不会被误标记为"已处理"。

        Args:
            sha: Git blob SHA

        Returns:
            True 表示已在缓存中可跳过，False 表示需要处理
        """
        with self._state_lock:
            if sha in self.sha_cache:
                self.sha_cache[sha] = datetime.now(timezone.utc)  # LRU: 更新时间戳
                return True  # 已处理，跳过
            return False  # 新内容，需要处理

    # ==================== 批次持久化（线程安全） ====================

    def _flush_batch(self, force: bool = False):
        """将当前 buffer 刷盘为批次文件。

        线程安全：_state_lock 保护 buffer/id/paths。
        刷盘后调用 on_batch_flush 回调（如果设置），用于投喂测速编排器。
        多个线程可并发调用，同一时刻只有一个线程执行写盘。

        force=False（运行中）：锁内二次确认 buffer >= BATCH_FLUSH_SIZE 才写
        ——防竞态：多线程锁外检查到阈值后同时调 flush，第一个写盘期间
        其他线程积累的小批次（48/98 节点）被误 flush（08105 的 35 个小分片）。
        force=True（收尾）：无条件写（残留数据不丢）。
        """
        with self._state_lock:
            if not self.batch_buffer:
                return
            if not force and len(self.batch_buffer) < BATCH_FLUSH_SIZE:
                return  # 竞态保护：运行中不足阈值不写（留在 buffer 等下次）
            self.batch_id += 1
            seq = self.batch_id
            nodes_to_write = list(self.batch_buffer)
            node_count = len(nodes_to_write)
            self.batch_buffer.clear()
            self._batch_dedup.clear()  # 内存优化：新批次开始，重置批次内去重

        batch_dir = os.path.join(os.getcwd(), BATCH_DIR)
        os.makedirs(batch_dir, exist_ok=True)
        filepath = os.path.join(batch_dir, f"no_batch_{seq:04d}.txt")
        text = "\n".join(nodes_to_write).encode("utf-8", errors="replace").decode("utf-8")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(text)

        self.batch_file_paths.append(filepath)
        mq = getattr(self, '_task_queue', None)
        dq = getattr(self, '_disc_queue', None)
        mq_sz = mq.qsize() if mq else 0
        dq_sz = dq.qsize() if dq else 0
        self._total_batch_nodes += node_count  # 累计（含批次间重复，收尾去重为准）
        self._wlog(f"📦 批次 {seq:04d} 已持久化: "
              f"{filepath} ({node_count} 个节点, 累计 {self._total_batch_nodes} 个)"
              f" | {self._qs()}")
        # 批次刷盘时顺带保存持久化状态，防止中途崩溃丢失
        self.save_sha_cache()
        self.save_seen_cache()

        # 内存优化（两阶段持久化）：运行时只写 batches 中间产物，
        # no/ 分片与 no_w_li.txt 由 _finalize 收尾全量去重后统一生成。
        # 保留 no_li.txt（源链接，量小）与 failed_candidates.txt。
        with open("no_li.txt", "w", encoding="utf-8", errors="replace") as f:
            f.write("\n".join(dict.fromkeys(self.all_links)))
        # 覆写 failed_candidates.txt（解析失败记录）
        if self.failed_candidates_buffer:
            with open("failed_candidates.txt", "w", encoding="utf-8") as f:
                f.write("\n".join(self.failed_candidates_buffer))

        # 通知测速编排器
        if self.on_batch_flush:
            try:
                self.on_batch_flush(seq, filepath, node_count)
            except Exception as e:
                self._wlog(f"⚠️ 批次回调异常: {e}")

    def _dedup_batches_write_no(self):
        """收尾去重（08111+）：委托模块级 dedup_batches_write_no()。

        模块级函数与 workflow 兜底脚本 finalize.py 共用，两条路径产出
        一致（正常完成 vs 取消/超时）。语义变化：no/ 从"本轮去重结果"
        变为"7 天窗口内全部节点"（本轮新的 + 历史补充，见模块级函数）。
        内存优化（两阶段持久化）不变：运行时只写 batches，收尾读全部
        批次 + 旧 no_his 去重，内存峰值 = 唯一节点数（约 1-2GB），安全。
        """
        count = dedup_batches_write_no()
        if count:
            self._final_node_count = count  # 收尾唯一节点数（_finalize 统计展示）

    def _add_node(self, node_uri: str, proxy=None) -> str:
        """添加节点到当前批次 buffer。

        去重检查在调用前已完成（server_port_protocol）。

        Returns:
            "added": 新增到全局集合
            "dup":   与已有节点重复（URI 已存在）
        """
        with self._state_lock:
            # 内存优化：批次内去重（全局去重收尾做）
            if node_uri in self._batch_dedup:
                return "dup"
            self._batch_dedup.add(node_uri)
            self.batch_buffer.append(node_uri)
            need_flush = len(self.batch_buffer) >= BATCH_FLUSH_SIZE

        if need_flush:
            self._flush_batch()
        return "added"

    # ==================== 种子文件管理 ====================
    # 种子文件（seed_repos.json / seed_channels.json）直接存储来源及其元数据。
    # 每次运行直接更新这些文件：有产出来源刷新时间戳，无产出来源被淘汰。
    # 不再需要中间层 sources.json。

    @staticmethod
    def _load_seed_file(filepath: str) -> dict:
        """加载种子文件，返回 {source_key: metadata_dict}。
        兼容旧格式（纯字符串数组）和新格式（带元数据的对象）。
        """
        import json
        if not os.path.exists(filepath):
            return {}
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return {}

        result = {}
        # 新格式：{"repos": {"key": {"last_new_node": "...", ...}}}
        # 或      {"channels": {"key": {"last_new_node": "...", ...}}}
        for container_key in ("repos", "channels"):
            if container_key in data and isinstance(data[container_key], dict):
                for key, meta in data[container_key].items():
                    if isinstance(meta, dict):
                        result[key] = meta
                    else:
                        result[key] = {}  # 旧格式无元数据
        # 旧格式：纯数组 ["key1", "key2", ...]
        if isinstance(data, list):
            for item in data:
                if isinstance(item, str):
                    result[item] = {}
        return result

    @staticmethod
    def _save_seed_file(filepath: str, container_key: str, seeds: dict):
        """保存种子文件。"""
        import json
        try:
            data = {container_key: seeds,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "max_entries": SEED_MAX_ENTRIES,
                    "max_age_hours": SEED_MAX_AGE_HOURS}
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"[{now_str()}] ⚠️ 保存 {filepath} 失败: {e}", flush=True)

    @staticmethod
    def _update_seed_entry(seeds: dict, key: str, new_node_count: int,
                           pushed_at: str = ""):
        """更新种子条目：记录节点产出时间 + GitHub 推送时间。"""
        if key not in seeds:
            seeds[key] = {}
        if new_node_count > 0:
            seeds[key]["last_new_node"] = datetime.now(timezone.utc).isoformat()
            seeds[key]["_had_nodes"] = True
        if pushed_at:
            seeds[key]["pushed_at"] = pushed_at

    @staticmethod
    def _sort_seeds(seeds: dict):
        """种子仓库排序：pushed_at 近的靠前，远的/无时间的靠后。"""
        def _key(item):
            _repo, meta = item
            _pushed = meta.get("pushed_at", "")
            try:
                return datetime.fromisoformat(_pushed.replace("Z", "+00:00"))
            except Exception:
                return datetime.min.replace(tzinfo=timezone.utc)
        sorted_items = sorted(seeds.items(), key=_key, reverse=True)
        seeds.clear()
        for k, v in sorted_items:
            v.pop("_had_nodes", None)  # 临时标记，写盘前清理
            seeds[k] = v

    @staticmethod
    def _prune_seeds(seeds: dict) -> dict:
        """淘汰种子：先按 pushed_at 排序 → 超 SEED_MAX_ENTRIES 时淘汰末尾。

        优先级：先删 pushed_at > SEED_MAX_AGE_HOURS 的（超龄种子），
        还不够则继续删最旧的（pushed_at 最小的）。
        新种子（无 pushed_at）保留。
        """
        if not seeds:
            return seeds

        # ── 先排序（确保尾部是最旧的） ──
        Collector._sort_seeds(seeds)

        # ── 条数超限 → 淘汰 ──
        if SEED_MAX_ENTRIES > 0 and len(seeds) > SEED_MAX_ENTRIES:
            excess = len(seeds) - SEED_MAX_ENTRIES
            evict_target = excess + max(1, len(seeds) // SEED_EVICTION_RATIO) if SEED_EVICTION_RATIO > 0 else excess
            pruned = 0
            cutoff = datetime.now(timezone.utc) - timedelta(hours=SEED_MAX_AGE_HOURS) if SEED_MAX_AGE_HOURS > 0 else None

            # 第一轮：删超龄种子
            if cutoff:
                for key in reversed(list(seeds.keys())):
                    if pruned >= evict_target:
                        break
                    pushed = seeds[key].get("pushed_at", "")
                    if not pushed:
                        continue
                    try:
                        if datetime.fromisoformat(pushed.replace("Z", "+00:00")) < cutoff:
                            del seeds[key]
                            pruned += 1
                    except Exception:
                        pass

            # 第二轮：还不够 → 删最旧的（从尾部）
            for key in reversed(list(seeds.keys())):
                if pruned >= evict_target:
                    break
                del seeds[key]
                pruned += 1

            if pruned:
                print(f"[{now_str()}] 淘汰 {pruned} 个种子（超龄+超限）, 保留 {len(seeds)} 个", flush=True)

        return seeds

    # ==================== 主流程 ====================

    def _should_stop(self) -> bool:
        """综合停止检查：限流超限/运行超时→立即停，配额耗尽→等待恢复。"""
        if self.limiter.should_stop():
            return True
        if self._runtime_exceeded():
            return True
        if self.quota_mgr.exceeded:
            return not self._wait_reset()
        return False

    def _runtime_exceeded(self) -> bool:
        """检查是否超出最大运行时间。GA 6 小时超时前 30 分钟触发。

        线程安全：_start_time 只在 run() 中设置一次，此后只读，无需加锁。
        """
        if self._max_runtime is None:
            return False
        return time.time() - self._start_time > self._max_runtime

    def run(self):
        """主入口：Code Search 串行先跑，GitHub 搜索线程并行。

        架构：
          - GitHub 线程：关键词/种子仓库搜索 → 共用线程池处理
          - Code Search：主线程串行，搜文件内容直接定位节点文件
          → 两者共享同一个线程池（8 Workers），fork 链/用户仓库自动传播

        每个线程持有独立 HttpClient，共享去重/缓存/批次状态。
        所有共享操作通过 _state_lock 保护。
        """
        self._wlog(f"🚀 程序启动 | 配额上限 {QUOTA_MAX_PER_HOUR}/小时")
        self._start_time = time.time()

        # 加载 + 持有种子文件（让 process_repo 内部也能更新）
        repo_seeds = self._load_seed_file(SEED_REPOS_FILE)
        self._repo_seeds = repo_seeds

        # 清空上次运行的文件
        batch_dir = os.path.join(os.getcwd(), BATCH_DIR)
        if os.path.exists(batch_dir): shutil.rmtree(batch_dir)
        os.makedirs(batch_dir, exist_ok=True)
        no_dir = os.path.join(os.getcwd(), "no")
        if os.path.exists(no_dir): shutil.rmtree(no_dir)
        os.makedirs(no_dir, exist_ok=True)
        for _fname in ("no_w_li.txt", "no_li.txt", "failed_candidates.txt"):
            with open(_fname, "w", encoding="utf-8") as _f:
                pass

        # ── 创建共用线程池 ──
        main_queue = PriorityQueue(maxsize=MAIN_QUEUE_SIZE)
        disc_queue = PriorityQueue(maxsize=DISCOVERY_QUEUE_SIZE)
        self._task_queue = main_queue
        self._disc_queue = disc_queue
        self._disc_seq = __import__('itertools').count()  # 线程安全计数器
        workers = [threading.Thread(target=self._pool_worker,
                                    args=(main_queue, disc_queue),
                                    name=f"W-{i}", daemon=True)
                   for i in range(SHARED_POOL_WORKERS)]
        for w in workers: w.start()
        # 08174：下载线程启动（异步下载管道——worker 只入队，下载线程消费）
        self._start_download_workers()
        # 08174：解析看门狗信号注册（SIGUSR1 → faulthandler 转储线程栈；
        # 内核信号直达，即使 GIL 被正则卡死也能抓现场）。
        # 08191 事故修复：faulthandler 转储后会"重抛信号"→ SIGUSR1 默认
        # 动作终止进程（exit 138，程序被自己的看门狗杀死）。先设忽略再
        # chain=True 注册——转储后链到"忽略"，只转储不终止。
        try:
            signal.signal(signal.SIGUSR1, signal.SIG_IGN)
            # 081XX：转储落盘——stderr 被 GA 日志管道丢弃（08192 的 44 次
            # 看门狗转储全丢，看不到卡在哪个正则）。落盘每轮可复盘。
            try:
                self._wd_dump_f = open(WATCHDOG_DUMP_FILE, "a",
                                       encoding="utf-8")
            except Exception:
                self._wd_dump_f = None
            faulthandler.register(signal.SIGUSR1,
                                  file=self._wd_dump_f,
                                  all_threads=True, chain=True)
        except Exception:
            self._wd_dump_f = None
        # 监控线程（系统状态观测）
        threading.Thread(target=self._monitor_loop, name="Monitor",
                         daemon=True).start()

        # 阶段专用 http（08131：连接池用默认 128——72 worker + 96 并发
        # 下载共享连接池，10/20 太小会在并发升高时报 HTTPSConnectionPool）
        http = HttpClient(token=self.token, rate_limiter=self.limiter,
                          quota_manager=self.quota_mgr,
                          api_gate=self.api_gate)
        self.http = http

        # ═══════════════════════════════════════════
        # 阶段 1: 种子仓库
        # ═══════════════════════════════════════════
        if SEED_STAGE_ENABLED and not self._runtime_exceeded():
            self._wlog(f"🔵 阶段 1: 种子仓库 ({len(repo_seeds)} 个)")
            t0 = time.time()
            _repos_before = self.checked_count
            _files_before = len(self.processed_file_shas)
            self._collect_seeds(main_queue)
            self._wait_queue_drain(main_queue, disc_queue)
            self._channel_stats["种子仓库"] = {
                "name": "种子仓库",
                "repos_checked": self.checked_count - _repos_before,
                "files_downloaded": len(self.processed_file_shas) - _files_before,
                "elapsed": f"{time.time()-t0:.0f}s",
                "nodes_new": self._channel_new_nodes.get("种子仓库", 0)}

        # ═══════════════════════════════════════════
        # 阶段 2: Code 文件搜索
        # ═══════════════════════════════════════════
        if CODE_STAGE_ENABLED and not self._runtime_exceeded():
            self._wlog(f"🔵 阶段 2: Code Search ({len(CODE_QUERIES)} 个词)")
            t0 = time.time()
            self._collect_code(main_queue)
            cn = self._channel_new_nodes.get("Code", 0)
            self._channel_stats["Code"] = {
                "name": "Code", "nodes_new": cn,
                "elapsed": f"{time.time()-t0:.0f}s",
                "api_calls": self.quota_mgr.total_calls,
                "files_found": getattr(self, '_code_files_found', 0),
                "repos_processed": getattr(self, '_code_repos_processed', 0)}
            self._wait_queue_drain(main_queue, disc_queue)

        # ═══════════════════════════════════════════
        # 阶段 3: 关键词搜索
        # ═══════════════════════════════════════════
        if KEYWORD_STAGE_ENABLED and not self._runtime_exceeded():
            self._wlog(f"🔵 阶段 3: 关键词搜索")
            t0 = time.time()
            _repos_before = self.checked_count
            _files_before = len(self.processed_file_shas)
            self._collect_keywords(main_queue)
            self._channel_stats["GitHub"] = {
                "name": "GitHub",
                "repos_checked": self.checked_count - _repos_before,
                "files_downloaded": len(self.processed_file_shas) - _files_before,
                "elapsed": f"{time.time()-t0:.0f}s",
                "nodes_new": self._channel_new_nodes.get("GitHub", 0),
                "api_calls": self.quota_mgr.total_calls,
                "api_report": self.quota_mgr.get_stats_report()}
            # 最后收尾阶段：等两个队列全清空再停 work
            self._wait_queue_drain(main_queue, disc_queue, wait_full=True)

        # ── 停止 Worker（优雅退出：sentinel 通知，处理完当前仓库即退出） ──
        for _ in workers: main_queue.put((float('inf'), -1, None))
        for _ in workers: disc_queue.put((float('inf'), float('inf'), -1, None))

        # ── 内存优化（两阶段持久化）收尾顺序：先等 Worker 停（60s 上限，
        # 处理完当前任务即退；卡死的任务（future.result/clone/下载）超时
        # 后放弃不等——081XX：08241 实测 300s join 拖到 GA 6h 上限被杀，
        # worker 是 daemon 线程，_finalize 后 os._exit(0) 兜底强杀）──
        deadline = time.time() + WORKER_JOIN_TIMEOUT_SECONDS
        for w in workers:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            w.join(timeout=remaining)
        self._task_queue = None

        # ── 08174 异步管道收尾：停下载线程（等队列清空不丢任务）→
        # 等解析池完成（已在池里的任务跑完；卡死的任务由看门狗转储定位）──
        self._stop_download_workers()
        try:
            self._parse_pool.shutdown(wait=True)
        except Exception:
            pass

        # ── 保存所有状态（Worker 已停，内存空出） ──
        with self._state_lock:
            self._sort_seeds(repo_seeds)
        self._save_seed_file(SEED_REPOS_FILE, "repos", repo_seeds)
        self._finalize(elapsed_seconds=time.time() - self._start_time)

    def _wait_queue_drain(self, main_queue, disc_queue, wait_full: bool = False):
        """等待队列清空（阶段切换条件）。

        默认（wait_full=False）：主队列空 AND 发现队列剩余 < work 数——
        剩余量一个 work 就能快速消费完，直接进入下一阶段，work 继续处理
        遗留 disc（不再死等 disc 清空，避免阶段 1 无限拖长）。
        wait_full=True（最后收尾阶段 3）：等两个队列全清空再停 work。

        配额耗尽则等待恢复。
        """
        self._wlog(f"⏳ 等待队列清空 ({self._qs()})...")
        while True:
            if wait_full:
                done = main_queue.qsize() == 0 and disc_queue.qsize() == 0
            else:
                done = (main_queue.qsize() == 0
                        and disc_queue.qsize() < SHARED_POOL_WORKERS)
            if done:
                break
            if self.limiter.should_stop() or self._runtime_exceeded():
                self._wlog(f"⚠️ 停止信号，放弃剩余任务")
                break
            if self.quota_mgr.exceeded:
                if not self._wait_reset():
                    break
                self._wlog(f"🔄 配额恢复，继续处理 | {self._qs()}")
                continue
            time.sleep(3)
        self._wlog(f"队列处理完毕 ({self._qs()})")

    # ── 搜集实现 ──

    def _collect_seeds(self, task_queue: Queue):
        """种子仓库阶段：主线程纯供应（零查询零判断）。

        所有种子去重后入主队列（不带仓库信息，pushed_at 留空——
        强制 work 消费时补查 repo info，判断全部基于最新信息）。
        是否追踪/是否解析全部由 work 在 process_repo 内判断。
        """
        repo_seeds = self._repo_seeds
        seed_list = list(repo_seeds.keys())
        if not seed_list:
            return
        self._worker_local.prefix = "种子"
        self._wlog(f"🔵 种子仓库: {len(seed_list)} 个 → 队列")
        for _idx, repo in enumerate(seed_list, 1):
            if self._should_stop(): break
            # 种子入队受 80/20 阈值控制（主队列 ≥80 暂停，等 work 消费到 <20
            # 再继续）——与搜索阶段一致的背压，主队列水位保持在阈值内。
            if not self._wait_queue_slot(task_queue): break
            _prefix = f"[种子 {_idx}/{len(seed_list)}]"
            self._seed_progress = f"{_idx}/{len(seed_list)}"  # 监控显示
            if not self._main_put(("种子仓库", repo,
                                   {"seed_key": repo,
                                    "tag": "[种子仓库]",
                                    "pos": _prefix})):
                self._wlog(f"🗑️ 主队列满，丢弃种子 {repo}")
                continue
            self._add_seen(repo)  # 标记去重，防止 fork/用户追踪重复发现
            self.checked_count += 1
            self._main_queue_total += 1
            self._wlog(f"🔵 {_prefix} {repo} | {self._qt()} | {self._qs()}")
            self._update_seed_entry(repo_seeds, repo, 0)
            # 种子纯入队（零 API），无需 sleep；背压由 _wait_queue_slot 控制
        self._worker_local.prefix = ""

    def _collect_keywords(self, task_queue: Queue):
        """关键词搜索阶段：Topic + README + BASE_QUERIES 全部关键词。"""
        self._worker_local.prefix = "关键词"
        _time_sfx = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime(
            '%Y-%m-%dT%H:%M:%SZ')

        # 构建搜索列表
        all_queries = list(self.queries)
        if TOPIC_SEARCH_ENABLED and TOPIC_QUERIES:
            for t in TOPIC_QUERIES:
                q = f"topic:{t} pushed:>{_time_sfx}"
                if SEARCH_FORK: q += " fork:true"
                all_queries.append(q)
        if README_SEARCH_ENABLED and README_QUERIES:
            for rq in README_QUERIES:
                q = f"{rq} in:readme pushed:>{_time_sfx}"
                if SEARCH_FORK: q += " fork:true"
                all_queries.append(q)

        self._wlog(f"🔵 关键词: {len(all_queries)} 个")
        self._kw_total = len(all_queries)  # 进度显示用
        for idx, query in enumerate(all_queries, 1):
            nb = self._total_batch_nodes  # 内存优化：批次累计节点数（含重复）
            qs = time.time()
            if self._should_stop(): break
            try:
                self._search_query_to_queue(query, task_queue, idx)
            except RuntimeError:
                break
            self._wlog(f"⏱️ [{idx}/{len(all_queries)}] "
                  f"{query[:60]} | {time.time()-qs:.0f}s | "
                  f"+{self._total_batch_nodes-nb} | "
                  f"{self._qt()}")

        # 保存统计
        self._worker_local.prefix = ""
        self._save_seed_file(SEED_REPOS_FILE, "repos", self._repo_seeds)

    def _try_take_main(self, main_queue: Queue, disc_queue: PriorityQueue,
                       endgame: bool = False):
        """原子取主队列：互斥锁 + 冷却 + disc 阈值 + 源头并发限制。

        锁只保护"取"的动作（毫秒级），不持有到处理完。
        每个 Worker 取完后进入冷却期（MAIN_TAKE_COOLDOWN），
        冷却结束且 disc 低于阈值才补充下一个源头。
        源头并发：Semaphore(MAIN_SOURCE_LIMIT) 限制同时处理的源头仓库数。

        Args:
            endgame: 配额末段（_quota_endgame）→ 豁免 disc 阈值检查
                     （08171 bug：外部 1775 允许取主队列，但内部此检查
                     无末段豁免，disc>DISC_MAIN_OK_AT 时照样拒绝——
                     末段"打破阈值取主队列消化配额"从未生效，
                     08171 实测 02:00-02:26 主队列积压 526→1026）。

        Returns:
            (item, True, source_held) 取到并持有源头令牌；
            (None, False, False) 未取。
        """
        if not self._main_take_lock.acquire(blocking=False):
            return None, False, False  # 其他 Worker 正在取
        try:
            tn = threading.current_thread().name
            last = self._worker_last_main.get(tn, 0)
            # disc 非空时保持冷却（防源头过快补充导致 disc 爆炸）；
            # disc 空时跳过冷却（无追踪活动 → 源头补充无风险，
            # work 全速取主队列，避免 disc 空 + 冷却导致的 work 空转）
            if disc_queue.qsize() > 0 and time.time() - last < MAIN_TAKE_COOLDOWN:
                return None, False, False  # 冷却中
            if not endgame and disc_queue.qsize() > DISC_MAIN_OK_AT:
                return None, False, False  # disc 未低于阈值（末段豁免）
            if not self._source_sem.acquire(blocking=False):
                return None, False, False  # 源头并发已满
            try:
                _, _, item = main_queue.get(timeout=5)
                self._worker_last_main[tn] = time.time()  # 记录取的时间
                return item, True, True   # 取到并持有源头令牌（处理完 release）
            except Empty:
                self._source_sem.release()
                return None, False, False  # 主队列空
        finally:
            self._main_take_lock.release()

    # ═══════════════ 08174 异步下载管道 ═══════════════

    def _start_download_workers(self):
        """启动固定数量下载线程（DOWNLOAD_WORKER_THREADS=48）。

        下载线程从待下载队列取 raw 链接执行下载+解析提交，worker 不再
        阻塞在下载/解析（08181 实测 worker 卡 1300s 的根因之一）。
        """
        self._dl_workers = [
            threading.Thread(target=self._download_worker_loop,
                             name=f"Dl-{i}", daemon=True)
            for i in range(DOWNLOAD_WORKER_THREADS)]
        for w in self._dl_workers:
            w.start()

    def _download_worker_loop(self):
        """下载线程主循环：取任务 → 下载 → 提交解析（不阻塞继续取下一个）。

        任务元组: (repo, branch, path, sha, has_nodes, raw_depth, stats, tag, size)
        结束条件：收到停止信号且队列清空（收尾时不丢任务）。
        """
        while True:
            if self._dl_stop.is_set() and self._dl_queue.empty():
                break
            # 限流超限（累计等待超上限）→ 停止下载（不空转抛异常）
            if self.limiter.should_stop():
                break
            try:
                task = self._dl_queue.get(timeout=1)
            except Empty:
                continue
            if task is None:  # 哨兵（保留，未使用）
                self._dl_queue.task_done()
                break
            # 08174 背压：取到任务后检查内存预算（下载中+解析中+本文件预估
            # ≤ 预算-余量）——不够则等待（content 还没进内存就拦住，不会
            # 出现"下载完才发现超了"的延迟）。size=0（partial clone）不
            # 预估（_est=0），下载完成后由解析预算按实际接管。
            _est = (task[8] or 0) * 3  # size 字节 → 解码 str 约 ×3（UTF-8）
            _mem_limit = ((DOWNLOAD_MEMORY_BUDGET_MB
                           - DOWNLOAD_MEMORY_BACKPRESSURE_MB)
                          * 1024 * 1024)
            _skip = False
            while True:
                with self._state_lock:
                    if self._downloading_bytes + self._parsing_bytes + _est \
                            <= _mem_limit:
                        self._downloading_bytes += _est
                        break
                # 收尾中放弃（不无限等预算）
                if self._dl_stop.is_set():
                    _skip = True
                    break
                time.sleep(0.5)
            if _skip:
                self._dl_queue.task_done()
                continue  # 放弃本任务（收尾中）
            try:
                # 081XX：任务第 10 位 local_path——非 None = fclone 本地文件，
                # 读盘预载走解析（不走网络下载；读盘 IO 本地磁盘，不占 raw
                # 连接配额）
                _lp = task[9] if len(task) > 9 else None
                if _lp:
                    try:
                        with open(_lp, "rb") as _fh:
                            _data = _fh.read()
                    except OSError:
                        self._dl_queue.task_done()
                        continue
                    self._handle_one_file(
                        task[0], task[1], task[2], task[3], task[4],
                        task[5], task[6], task[7], size=task[8],
                        content_bytes_preloaded=_data, async_mode=True)
                else:
                    # async_mode=True：解析提交共享池后立即返回（不阻塞下载线程）
                    self._handle_one_file(*task[:9], async_mode=True)
            except Exception as e:
                log_sink.emit(f"[{now_str()}] [Dl] ⚠️ 下载线程异常: {e}")
            finally:
                with self._state_lock:
                    self._downloading_bytes = max(
                        0, self._downloading_bytes - _est)  # 释放预估
                self._dl_queue.task_done()

    # ═══════════════ 081XX 第 3 批：仓库记账器 + 结果队列 ═══════════════

    def _tracker_register(self, repo: str, n_files: int, mode: str = "raw",
                          tmp_dir: str = None, stats: List[int] = None,
                          tag: str = "", branch: str = ""):
        """仓库文件入队时登记（total 累加；同 repo 多来源入队合并）。

        文件列表不驻留：total 是数字，列表在 worker 同步段（取样→入队）
        后释放——目录列表不会在内存堆积（几千仓库 × 几 MB 会 OOM）。
        """
        if n_files <= 0:
            return
        with self._trackers_lock:
            t = self._trackers.get(repo)
            if t is None:
                t = {"total": 0, "done": 0, "has_node": False,
                     "extracted": 0, "added": 0, "mode": mode,
                     "tmp_dir": tmp_dir, "stats": stats, "tag": tag,
                     "branch": branch,
                     "raw_links": set(), "repo_links": set(),
                     "sub_urls": set()}
                self._trackers[repo] = t
            t["total"] += n_files

    def _tracker_file_done(self, repo: str, has_node: bool = False,
                           extracted: int = 0, added: int = 0):
        """单个文件终结（解析完成/下载失败/超时跳过都算）。

        done==total → 发"仓库完成"事件进结果队列（work 消费后做后续）。
        同步路径（取样）的文件不注册过 tracker → 直接返回（不误统计）。
        """
        ev = None
        with self._trackers_lock:
            t = self._trackers.get(repo)
            if t is None:
                return
            t["done"] += 1
            t["has_node"] = t["has_node"] or has_node
            t["extracted"] += extracted
            t["added"] += added
            if t["done"] >= t["total"]:
                self._trackers.pop(repo, None)
                ev = ("repo_done", repo, dict(t))
        if ev is not None:
            try:
                self._result_queue.put_nowait(ev)
            except Full:
                # 结果队列满：阻塞放（仓库完成事件量小，几乎不会满；
                # 即使满也不能丢——丢了 tmp 不会被删、仓库不统计）
                self._result_queue.put(ev)

    def _backpressure(self) -> bool:
        """源头背压（081XX）：worker 取仓库前检查。

        触发（任一）：下载队列待处理 ≥ DOWNLOAD_BACKPRESSURE_QUEUE 或
                     下载中+解析中内存 ≥ DOWNLOAD_BACKPRESSURE_MEM_MB
        恢复（两个都满足）：队列 < RESUME_QUEUE 且 内存 < RESUME_MEM_MB
        ——081XX 修正：恢复条件必须"且"。若用"或"，内存本来就低
        （<500MB）时 OR 恒真 → 背压触发后立即恢复，背压形同虚设
        （测试 _test_batch3 抓到的真实逻辑缺陷）。
        滞回：触发值 > 恢复值，防临界点反复横跳（worker 停/启有开销）。
        只停"取新仓库"，结果队列照常消费（仓库完成事件不积压）。
        """
        q = self._dl_queue.qsize()
        mem = (self._downloading_bytes + self._parsing_bytes) / 1024 / 1024
        if self._backpressure_active:
            if q < DOWNLOAD_BACKPRESSURE_RESUME_QUEUE \
                    and mem < DOWNLOAD_BACKPRESSURE_RESUME_MEM_MB:
                self._backpressure_active = False
                self._wlog(f"🟢 背压解除（队列 {q} / 内存 {mem:.0f}MB）")
            # 081XX：返回"是否处于背压"（_backpressure_active）——
            # 此前误写 not active，背压中恒返回 False（恢复分支逻辑反转，
            # 测试 _test_batch3 抓到的真实 bug）
            return self._backpressure_active
        else:
            if q >= DOWNLOAD_BACKPRESSURE_QUEUE \
                    or mem >= DOWNLOAD_BACKPRESSURE_MEM_MB:
                self._backpressure_active = True
                self._wlog(f"🔴 背压触发：下载队列 {q} 个 / 内存 {mem:.0f}MB，"
                      f"暂停取新仓库（恢复：队列<{DOWNLOAD_BACKPRESSURE_RESUME_QUEUE}"
                      f" 且 内存<{DOWNLOAD_BACKPRESSURE_RESUME_MEM_MB}MB）")
            return self._backpressure_active

    def _handle_repo_result(self, ev: tuple):
        """work 处理"仓库完成"事件（081XX 第 3 批）。

        分类动作（按仓库模式）：
          - fclone（全量下载）：tmp 目录在磁盘，done==total 后删除；
            文件内容从不在内存堆积（逐文件读入解析后释放）
          - partial/tree：文件内容在远端（blob:none 未下载），本地无
            数据可清理，只做黑名单/统计
          有节点 → 更新统计 + 递归发现 + 订阅嗅探（慢操作全在此，
          解析回调只做提取——解析回调瘦身）
        """
        try:
            _, repo, t = ev
            # 1. fclone tmp 清理（仓库全部文件处理完才能删）
            if t.get("mode") == "fclone" and t.get("tmp_dir"):
                try:
                    shutil.rmtree(t["tmp_dir"], ignore_errors=True)
                except Exception:
                    pass
            # 2. 统计：fclone 有节点仓库数（raw/partial/tree 由 process_repo
            #    的 repo_stats[0]>0 统计——fclone 取样节点不进 repo_stats，
            #    只在此计，两处不重复）
            #    黑名单不在本处做：取样失败的黑名单已在同步路径
            #    （_process_file_list / _full_clone_local_parse），取样通过
            #    的仓库不黑名单；候选 ≤ 阈值未取样的仓库全无节点也不黑名单
            #    （保持原行为：黑名单只针对取样失败）
            if t.get("mode") == "fclone" and t.get("has_node"):
                self._repos_with_nodes_total += 1
            # 3. 递归发现（原 _discover_recursive 的入队执行部分，work 做）
            _tag = t.get("tag", "")
            for url in list(t.get("raw_links", set())):
                self._discover_url(url, _tag, is_raw=True)
            for url in list(t.get("repo_links", set())):
                self._discover_url(url, _tag, is_raw=False)
            # 4. 订阅嗅探（原 _postprocess 的 HTTP 部分，work 做，限流）
            for url in list(t.get("sub_urls", set()))[:SUB_URL_MAX_PER_FILE]:
                if url in self._sub_urls_seen:
                    continue
                self._sub_urls_seen.add(url)
                try:
                    _resp = self.http.get(url, timeout=(8, 15),
                                          operation_name=f"订阅链接 {url[:60]}")
                    if _resp and _resp.text:
                        _sub_proxies = extract_all_strategies(_resp.text)
                        for _p in _sub_proxies:
                            if _p.is_valid():
                                with self._state_lock:
                                    _uri = _p.to_uri()
                                    if _uri in self._batch_dedup:
                                        continue
                                    self._batch_dedup.add(_uri)
                                    self.batch_buffer.append(_uri)
                                    self.all_links.append(url)
                except Exception:
                    pass
        except Exception:
            pass

    def _extract_links(self, content: str) -> tuple:
        """从解析内容提取 raw 链接/仓库链接/订阅 URL（081XX 第 3 批）。

        解析回调只做提取（纯 regex，快）；入队/HTTP 由 work 在仓库完成
        事件时做（_handle_repo_result）——原在 _postprocess 里入队/拉取
        占解析资源（订阅嗅探 HTTP 8-15s 占解析回调线程）。
        """
        raw_links, repo_links, sub_urls = set(), set(), set()
        try:
            for m in re.finditer(
                    r'https://raw\.githubusercontent\.com/'
                    r'([^/]+/[^/]+)/([^/]+)/([^\s"\'`#]+)', content):
                raw_links.add(m.group(0).rstrip('.,;:!?)\'"]'))
        except Exception:
            pass
        try:
            for m in re.finditer(
                    r'https://github\.com/([a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+)'
                    r'(?!/blob/|/tree/|/raw/|/issues|/pull|/releases|/wiki)',
                    content):
                repo_links.add(m.group(0).rstrip('.,;:!?)\'"]'))
        except Exception:
            pass
        try:
            for m in re.finditer(
                    r'(?:https?://)'
                    r'(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}'
                    r'(?:/\S*)?\b(?:sub|subscribe|link|token|node|proxy|v2ray|clash'
                    r'|ssr|vless|trojan|hysteria|tuic|singbox|shadowrocket'
                    r'|quantumult|surge|loon|stash)\b[^\s"\']{0,200}',
                    content, re.IGNORECASE):
                _url = m.group(0).rstrip('.,;:!?)"\']')
                _black = ('google.com', 'star-history.com', 'play.google.com',
                          'apple.com', 'microsoft.com', 'facebook.com', 'twitter.com',
                          'youtube.com', 'reddit.com', 'wikipedia.org')
                if ('github.com' not in _url
                        and 'raw.githubusercontent.com' not in _url
                        and not any(d in _url for d in _black)):
                    sub_urls.add(_url)
        except Exception:
            pass
        return raw_links, repo_links, sub_urls

    def _discover_url(self, url: str, tag: str = "[种子仓库]",
                      is_raw: bool = True):
        """递归发现单个链接（081XX：从 _discover_recursive 拆出的执行部分）。

        原 _discover_recursive 在解析回调里对 content 全量匹配 + 入队，
        第 3 批改为：解析回调只提取链接（_extract_links），work 在仓库
        完成事件时逐个调用本方法（repo info → 入队发现队列）。
        """
        try:
            if self.recursive_count >= MAX_RECURSIVE_REPOS:
                return
            if is_raw:
                m = re.match(
                    r'https://raw\.githubusercontent\.com/'
                    r'([^/]+/[^/]+)/([^/]+)/([^\s"\'`#]+)', url)
                if not m:
                    return
                full_name = m.group(1)
                path = m.group(3)
                if os.path.splitext(path)[1].lower() not in ALLOWED_EXTENSIONS:
                    return
            else:
                m = re.match(r'https://github\.com/([a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+)', url)
                if not m:
                    return
                full_name = m.group(1)
            if not self._check_and_add_seen(full_name) \
                    or self._is_repo_dead(full_name):
                return
            self._wlog(f"🔗 {tag} 递归发现仓库 {full_name} (来源 {url[:80]})")
            self.recursive_count += 1
            time.sleep(REPO_SLEEP_SECONDS)
            rl = full_name.lower()
            if rl in self._repo_checking:
                time.sleep(0.3)
                if self._is_repo_dead(full_name):
                    return
            self._repo_checking.add(rl)
            try:
                if self.quota_mgr.exceeded:
                    link_tag_q = f"[raw{min(self._tag_depth(tag) + 1, MAX_TRACE_DEPTH)}]"
                    if getattr(self, '_disc_queue', None):
                        self._disc_put(("GitHub", full_name,
                                        {"branch": "main", "size": -1,
                                         "disabled": False, "pushed_at": "",
                                         "raw_depth": 0, "language": "",
                                         "tag": link_tag_q}),
                                       label="链接")
                    return
                repo_info = self.http.get_json(
                    f"https://api.github.com/repos/{full_name}",
                    timeout=FILE_DOWNLOAD_TIMEOUT,
                    operation_name=f"repo info ({full_name})")
                if not repo_info or repo_info.get('disabled', False):
                    if not repo_info:
                        if f"repo info ({full_name})" in self.http.last_404:
                            self._mark_repo_not_found(full_name)
                            if USER_REPOS_ENABLED \
                                    and self._tag_kind(tag) not in ("user", "404user"):
                                self._trace_user_repos(full_name, "main", tag,
                                                       depth_offset=0)
                    return
                branch = repo_info.get("default_branch", "main")
                self._branch_cache[full_name] = branch
                raw_tag = f"[raw{min(self._tag_depth(tag) + 1, MAX_TRACE_DEPTH)}]"
                if self._is_traced(full_name, repo_info.get("pushed_at", ""),
                                   self._tag_depth(raw_tag)):
                    return
                if getattr(self, '_disc_queue', None):
                    self._disc_put(("GitHub", full_name,
                                    {"branch": branch,
                                     "size": repo_info.get("size", -1),
                                     "disabled": False,
                                     "pushed_at": repo_info.get("pushed_at", ""),
                                     "raw_depth": 0, "language":
                                         repo_info.get("language", ""),
                                     "tag": raw_tag}),
                                   label="raw 递归")
            finally:
                self._repo_checking.discard(rl)
        except Exception:
            pass

    def _enqueue_downloads(self, repo, branch, files_to_check, has_nodes,
                           raw_depth, stats, tag,
                           local_paths: List[str] = None,
                           mode: str = "raw", tmp_dir: str = None) -> bool:
        """把取样通过后的剩余文件全部入待下载队列（08174）。

        local_paths（081XX 第 3 批）：fclone 本地文件路径列表——任务带
        本地路径标记，下载线程读盘解析（不走网络下载）。
        任务元组第 10 位 local_path：None=raw 网络下载；str=本地文件。
        入队锁：同仓库文件连成一段（防不同仓库交错、某仓库文件被挤散）。
        队列满 → 阻塞等待（可中断：收尾/限流超限时放弃，不死等）。
        入库前注册记账器（total）——文件列表只在这里用一次，随后释放
        （列表不驻留内存；done==total 由 _tracker_file_done 聚合）。
        Returns: True=全部入队；False=放弃（收尾中）。
        """
        n_ok = 0
        with self._dl_enqueue_lock:
            for _i, (file_path, sha, _size) in enumerate(files_to_check):
                _lp = local_paths[_i] if local_paths else None
                while True:
                    try:
                        self._dl_queue.put_nowait(
                            (repo, branch, file_path, sha, has_nodes,
                             raw_depth, stats, tag, _size, _lp))
                        break
                    except Full:
                        if self._dl_stop.is_set() or self.limiter.should_stop():
                            # 部分入队也算登记（已入队的会走 done 计数）
                            self._tracker_register(
                                repo, n_ok, mode=mode, tmp_dir=tmp_dir,
                                stats=stats, tag=tag, branch=branch)
                            return False
                        time.sleep(0.5)
                n_ok += 1
        self._tracker_register(repo, n_ok, mode=mode, tmp_dir=tmp_dir,
                               stats=stats, tag=tag, branch=branch)
        return True

    def _stop_download_workers(self):
        """收尾：停止下载线程并等队列清空（不丢任务）。

        流程：置停止信号 → 等队列空（下载线程持续消费）→ join。
        081XX：等队列空加超时——08241 积压 5.5 万时 join() 无超时，
        收尾卡 ~25 分钟拖到 GA 6h 上限被杀，统计/缓存被截断。
        超时后放弃剩余任务：已解析分片已落盘（不丢），未解析文件
        SHA 未写缓存（下次重抓）。
        """
        self._dl_stop.set()
        # 等队列清空（下载线程消费完剩余任务自然退出），超时兜底
        try:
            _deadline = time.time() + DOWNLOAD_DRAIN_TIMEOUT_SECONDS
            while self._dl_queue.unfinished_tasks > 0 \
                    and time.time() < _deadline:
                time.sleep(0.5)
        except Exception:
            pass
        # 下载线程处理完在途任务后退出（30s 上限——下载中任务大多 <30s；
        # 超时则后台继续（daemon），主流程收尾不阻塞）
        for w in self._dl_workers:
            try:
                w.join(timeout=30)
            except Exception:
                pass

    def _pool_worker(self, main_queue: Queue, disc_queue: PriorityQueue):
        """共用线程池 Worker — 阈值调度模式。

        策略：
          1. 永远优先消费发现队列（PriorityQueue，旧的优先处理）
          2. 发现队列 ≥ DISC_FORCE_CONSUME_AT → 强制消费发现队列
          3. 发现队列 ≤ DISC_MAIN_OK_AT → 原子取主队列（互斥+冷却）
          4. 次级限流时自动降级等待
        """
        quota_skip_count = 0  # 连续遇到缺信息任务计数（配额耗尽零 API 模式，跨循环保持）
        while True:
            item = None
            from_queue = None
            source_held = False

            # ═══ 阶段 0: 停止检查（运行时超时/限流 → 退出） ═══
            if self._runtime_exceeded() or self.limiter.should_stop():
                break

            # ═══ 阶段 0.1: 处理仓库完成事件（081XX 第 3 批） ═══
            # 非阻塞轮询结果队列——仓库全部文件处理完的事件（黑名单/
            # 统计/删 tmp/递归入队/订阅嗅探全在此做；解析回调只提取，
            # 慢操作不占解析资源）。每轮最多 8 条防单 worker 积压。
            try:
                for _ in range(8):
                    _ev = self._result_queue.get_nowait()
                    self._handle_repo_result(_ev)
            except Empty:
                pass

            # ═══ 阶段 0.2: 源头背压（081XX） ═══
            # 下载 >> 解析（08241 队列积压 5.5 万-10 万、后期 CPU 3-5%
            # 空转的根因）→ 暂停取新仓库，让积压消化；滞回恢复防抖。
            # 背压期间继续轮询结果队列（上一步已处理），不阻塞。
            if self._backpressure():
                time.sleep(1)
                continue

            # ═══ 阶段 0.5: 阈值+冷却 → 强制取主队列（优先补充源头） ═══
            # disc 低于阈值且冷却结束 → 先尝试取主队列（原子+源头并发），
            # 防止 Worker 只消费 disc 到 0 才取主队列（扩展队列长期低值）。
            # 08141：配额末段（_quota_endgame）打破 disc 阈值——允许取
            # 主队列消化 API 配额（平时 disc≥阈值不取主队列）。
            if disc_queue.qsize() <= DISC_MAIN_OK_AT or self._quota_endgame():
                item, took, source_held = self._try_take_main(
                    main_queue, disc_queue,
                    endgame=self._quota_endgame())
                if took:
                    from_queue = main_queue
                    if main_queue.qsize() < MAIN_QUEUE_RESUME_AT:
                        self._search_resume.set()
                    # 取到源头 → 直接进入阶段 3 处理（不消费 disc）
                    return_to_phase1 = False
                else:
                    # 未取到（冷却中/源头满/主队列空）→ 继续消费 disc
                    return_to_phase1 = True
            else:
                # disc ≥ 阈值 → 不取主队列，消费 disc
                return_to_phase1 = True

            if return_to_phase1:
                # ═══ 阶段 1: 消费发现队列 ═══
                # disc 元素: (priority, ts, seq, item)，priority 0=不追踪先消费
                try:
                    _, _, _, item = disc_queue.get_nowait()
                    from_queue = disc_queue
                except Empty:
                    # ═══ 阶段 2: 等待发现队列（disc 空 + 未取到主队列） ═══
                    try:
                        _, _, _, item = disc_queue.get(timeout=5)
                        from_queue = disc_queue
                    except Empty:
                        tn = threading.current_thread().name
                        last = self._worker_idle_since.get(tn, 0)
                        if time.time() - last > 120:
                            self._worker_idle_since[tn] = time.time()
                            self._wlog(f"⏳ 等待任务中...")
                        continue

            # ═══ 阶段 3: 处理任务 ═══
            try:
                if item is None:
                    break
                # API 速率门：速率接近上限 → 暂停取新任务（削峰）
                if self.api_gate.should_pause():
                    time.sleep(5)
                    continue
                # 次级限流降级检查
                if SECONDARY_RATE_LIMIT_DEGRADE and self.quota_mgr.secondary_limited:
                    time.sleep(10)
                    continue
                # 停止信号检查
                if self.limiter.should_stop() or self._runtime_exceeded():
                    break
                if self.quota_mgr.exceeded:
                    # 配额耗尽 → 零 API 模式（08141 判断升级）：
                    # 只处理"零 API 仓库" = 有完整 info（pushed_at/language/
                    # size 已知）且 层级 == MAX_TRACE_DEPTH（不需要追踪）
                    # 且 size < SMALL_REPO_CLONE_MB（clone 路径，不走 tree）。
                    # 需 API 任务（缺 info / 非最深层 / ≥50MB）→ requeue
                    # 放回队列尾（不阻塞——_wait_reset 死等已取消）；
                    # 连续取到需 API 任务 → 队列已无可零 API 任务，
                    # 短暂 sleep 后重试（配额恢复由整点驱动，不自旋）。
                    _zero_api = False
                    try:
                        _s, _r, _kw = item
                        _size_kb = _kw.get("size", -1)
                        _depth = self._tag_depth(_kw.get("tag", ""))
                        _zero_api = (
                            _kw.get("pushed_at") and _kw.get("language")
                            and isinstance(_size_kb, int) and _size_kb >= 0
                            and _depth >= MAX_TRACE_DEPTH
                            and _size_kb < SMALL_REPO_CLONE_MB * 1024)
                    except (ValueError, TypeError):
                        _zero_api = False
                    if not _zero_api:
                        self._requeue_for_quota(item, from_queue)
                        quota_skip_count += 1
                        if quota_skip_count >= 5:
                            quota_skip_count = 0
                            time.sleep(2)  # 队列已无可零 API 任务，短暂等待
                        else:
                            time.sleep(0.3)  # 让其他 worker 先取零 API 任务
                        continue
                    # 零 API 处理路径（process_repo 内部 API 调用在配额
                    # 耗尽时均有保护，见 _trace_repo/_discover_recursive）
                    quota_skip_count = 0
                source, repo, kwargs = item
                tag = kwargs.get("tag", "")
                pos = kwargs.get("pos", "")      # 位置信息（种子序号/关键词页码）
                pos_txt = f" {pos}" if pos else ""
                self.http = HttpClient(token=self.token, rate_limiter=None,
                                       quota_manager=self.quota_mgr,
                                       api_gate=self.api_gate)
                _t0 = time.time()
                self._set_worker_state(f"处理 {tag} {repo}")
                self._wlog(f"🔧 开始处理 {tag} {repo}{pos_txt}")
                extracted, added = self.process_repo(repo, **kwargs)
                self._wlog(f"✅ 完成 {tag} {repo}{pos_txt} | 提取 {extracted} | 新增 {added} "
                      f"| 耗时 {time.time()-_t0:.0f}s | {self._qt()} | {self._qs()}")
                # 08141：仓库完成统计（近60s 窗口 + 累计；种子/非种子分开）
                _rk = kwargs.get("size", -1)
                _rk_mb = _rk / 1024 if _rk is not None and _rk >= 0 else 0
                self._repos_done_60s.append((time.time(), _rk_mb))
                self._repos_done_total += 1
                if _rk_mb > 0:
                    self._repos_done_size_total += _rk_mb
                if "种子" in str(tag):
                    self._seed_repos_done_total += 1
                    if _rk_mb > 0:
                        self._seed_repos_done_size_total += _rk_mb
                tn = threading.current_thread().name
                self._worker_repo_count[tn] = self._worker_repo_count.get(tn, 0) + 1
                ch = self._channel_new_nodes
                ch[source] = ch.get(source, 0) + added
            except Exception as e:
                import traceback
                traceback.print_exc()
                self._wlog(f"⚠️ Worker 异常: {repo if 'repo' in dir() else '?'}: {e}")
            finally:
                self._set_worker_state("空闲")
                if source_held:
                    self._source_sem.release()
                if from_queue is not None:
                    try:
                        from_queue.task_done()
                    except Exception:
                        pass

    def _search_query_to_queue(self, query: str, task_queue: Queue, q_idx: int = 0):
        """搜索单个关键词，结果直接放进线程池队列。"""
        has_cjk = bool(re.search(r'[一-鿿]', query))
        max_p = (MAX_PAGES * MAX_PAGES_ZH_MULTIPLIER) if has_cjk else MAX_PAGES

        for page in range(1, max_p + 1):
            if self._should_stop(): return
            # 配额耗尽 → 暂停搜索（等恢复再继续，不发必失败的请求）
            if self.quota_mgr.exceeded:
                if not self._wait_reset():
                    return
            if not self._wait_queue_slot(task_queue): return
            # 08171：进度格式改"已完成 X/N 词"（原"第N/M页"显示的是当前
            # 词页码，最后一个词停在页 1 时误导成"没翻页"）
            self._kw_progress = f"已完成 {q_idx}/{self._kw_total} 词"  # 监控显示
            url = (f"https://api.github.com/search/repositories"
                   f"?q={query}&sort=updated&order=desc"
                   f"&per_page={PER_PAGE}&page={page}")
            resp = self.http.get(url, timeout=SEARCH_TIMEOUT,
                                 operation_name=f"搜索第{page}页")
            if not resp:
                break
            data = resp.json()
            items = data.get("items", [])
            if not items:
                break
            self._wlog(f"  第{page}页 items:{len(items)} | {self._qt()} | {self._qs()}")
            for item in items:
                repo = item.get("full_name")
                if not repo: continue
                if not self._check_and_add_seen(repo):
                    continue
                pushed = item.get("pushed_at", "")
                self.checked_count += 1
                self._main_queue_total += 1
                if not self._main_put(("GitHub", repo,
                                       {"branch": item.get("default_branch", "main"),
                                        "size": item.get("size", 0),
                                        "disabled": item.get("disabled", False),
                                        "pushed_at": pushed,
                                        "is_source": True,
                                        "language": item.get("language", ""),
                                        "tag": f"[kw{KEYWORD_TRACE_DEPTH}]",
                                        "pos": f"[关键词 {q_idx}/{self._kw_total} 第{page}/{max_p}页]"})):
                    self._wlog(f"🗑️ 主队列满，丢弃 {repo}")
            time.sleep(PAGE_SLEEP_SECONDS)

    def _collect_code(self, task_queue: Queue):
        """GitHub Code Search：搜索文件内容中的节点 URI（方案 a：主线程纯供应）。

        与 _collect_github 的区别：
          - API: /search/code（搜文件内容）vs /search/repositories（搜仓库名）
          - 策略: 主线程只做搜索 + 把命中仓库带全信息入主队列，由 work 整仓解析
                  （不再直接下载/解析文件，架构与[种子]/[关键词]主线程一致）

        流程：
          1. 遍历 CODE_QUERIES，构建搜索词（加 24h 时间限定）
          2. 翻页调用 /search/code API
          3. 所有命中仓库 → 入主队列（tag 层级由 CODE_TRACE_DEPTH 决定：
             0 = 追踪到最大层级；= MAX_TRACE_DEPTH = 只解析不追踪）
        """
        if not CODE_QUERIES:
            return

        self._worker_local.prefix = "Code"
        time_sfx = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime(
            '%Y-%m-%dT%H:%M:%SZ')

        code_files = 0
        repos_found = set()

        for idx, query in enumerate(CODE_QUERIES, 1):
            qs = time.time()
            full_query = f"{query} pushed:>{time_sfx}"

            for page in range(1, CODE_MAX_PAGES + 1):
                if self._should_stop(): break
                # 配额耗尽 → 暂停搜索（等恢复再继续，不发必失败的请求）
                if self.quota_mgr.exceeded:
                    if not self._wait_reset():
                        return
                if not self._wait_queue_slot(task_queue): return
                # 08171：进度格式改"已完成 X/N 词"（同关键词，见 _kw_progress）
                self._cd_progress = f"已完成 {idx}/{len(CODE_QUERIES)} 词"  # 监控显示

                url = (f"https://api.github.com/search/code"
                       f"?q={quote(full_query)}&sort=indexed&order=desc"
                       f"&per_page=100&page={page}")

                resp = self.http.get(url, timeout=SEARCH_TIMEOUT,
                                     operation_name=f"Code搜索第{page}页")
                if not resp:
                    break

                data = resp.json()
                items = data.get("items", [])
                if not items:
                    break

                self._wlog(f"  Code第{page}页 items:{len(items)} | {self._qt()} | {self._qs()}")
                for item in items:
                    if self.limiter.should_stop():
                        break

                    repo_data = item.get("repository", {})
                    repo_name = repo_data.get("full_name", "")
                    if not repo_name:
                        continue

                    code_files += 1  # 统计搜索命中数（在过滤之前）
                    repos_found.add(repo_name)

                    # 获取分支：优先从搜索结果 → 缓存 → "main"
                    default_branch = repo_data.get("default_branch", "")
                    if not default_branch:
                        default_branch = self._branch_cache.get(repo_name, "main")
                    else:
                        self._branch_cache[repo_name] = default_branch

                    # 主线程纯供应：命中仓库带全信息入主队列，由 work 整仓解析。
                    # 不直接下载/解析文件；查询已限 pushed:>24h，
                    # work 侧年龄判断自然通过，不会跳过。
                    if self._check_and_add_seen(repo_name):
                        cd_tag = f"[cd{CODE_TRACE_DEPTH}]"
                        if self._main_put(("Code", repo_name,
                                           {"branch": default_branch,
                                            "size": repo_data.get("size", -1),
                                            "disabled": False,
                                            "pushed_at": repo_data.get("pushed_at", ""),
                                            "is_source": True,
                                            "language": repo_data.get("language", ""),
                                            "tag": cd_tag,
                                            "pos": f"[Code {idx}/{len(CODE_QUERIES)} 第{page}/{CODE_MAX_PAGES}页]"})):
                            self.checked_count += 1
                            self._main_queue_total += 1
                        else:
                            self._wlog(f"🗑️ 主队列满，丢弃 Code 仓库 {repo_name}")

                time.sleep(PAGE_SLEEP_SECONDS)

            self._wlog(f"⏱️ Code [{idx}/{len(CODE_QUERIES)}] "
                  f"{query[:60]} | {time.time() - qs:.0f}s | "
                  f"{self._qt()}")

        # 记录统计（主线程不解析，节点计数由 work 侧 _channel_new_nodes 累加）
        self._code_files_found = code_files
        self._code_repos_processed = len(repos_found)
        self._worker_local.prefix = ""

    # ── 搜索辅助 ──

    def _print_distributions(self):
        """输出候选文件数与仓库大小分布（08113：校准分流阈值的依据）。

        两分布的累计百分比对齐处 = 合理分流阈值候选。
        例：候选文件 ≤99 的仓库占 75%，大小 ≤49MB 的仓库占 75% →
        50MB 阈值覆盖 75% 仓库走 clone（候选文件少，下载量可控）。
        08121：同时写 stats_distribution.txt——手动结束/取消时 stdout
        进 devnull，文件不丢（随 git 提交）。
        """
        def _dump(title, hist):
            total = sum(hist.values())
            if not total:
                return []
            out = [f"=== {title}（共 {total} 个）==="]
            acc = 0
            for k in sorted(hist, key=lambda s: int(s.split('-')[0])):
                c = hist[k]
                acc += c
                line = (f"  {k:>10s}: {c:>6d} ({c / total * 100:5.1f}%) "
                        f"[累计 {acc / total * 100:5.1f}%]")
                out.append(line)
                print(line, flush=True)
            return out
        _all = []
        _all += _dump("候选文件数量分布", self._candidate_hist)
        _all += _dump("仓库大小分布", self._repo_size_hist)
        if _all:
            try:
                with open("stats_distribution.txt", "w", encoding="utf-8") as f:
                    f.write("\n".join(_all) + "\n")
            except Exception as e:
                self._wlog(f"⚠️ stats_distribution.txt 写入失败: {e}")

    def _finalize(self, elapsed_seconds: float = 0):
        """最终保存和统计。即使之前发生错误也能安全调用。"""
        if self._max_runtime and elapsed_seconds > self._max_runtime:
            self._wlog(f"⚠️ 运行时间 {elapsed_seconds:.0f}s 超出上限 "
                  f"{self._max_runtime}s（已提前停止搜集）")
        try:
            self._flush_batch(force=True)  # 收尾：无条件写残留（数据不丢）
        except Exception as e:
            self._wlog(f"⚠️ buffer 刷盘异常: {e}")
        # 内存优化（两阶段持久化）：读全部 batches → 全量去重 → 写 no/ 分片。
        # Worker 已停（run() 先 join 后 _finalize），内存空出，去重安全。
        try:
            self._dedup_batches_write_no()
        except Exception as e:
            self._wlog(f"⚠️ 收尾去重写 no/ 异常: {e}")
        # 081XX：统计落盘提前到收尾去重后立即写——08241 的 _finalize 被
        # GA 6h 上限杀进程时，统计（原最后一步）从没成功落盘过。收尾去重
        # 后数据已定，提前写保证 GA 杀进程前统计不丢。
        try:
            self._write_run_stats(elapsed_seconds)
        except Exception:
            pass
        try:
            self.save_results()
        except Exception as e:
            self._wlog(f"⚠️ save_results 异常: {e}")
        try:
            self.save_sha_cache()
        except Exception as e:
            self._wlog(f"⚠️ SHA 缓存保存异常: {e}")
        try:
            self._save_clone_stats(elapsed_seconds)
        except Exception as e:
            self._wlog(f"⚠️ clone_stats 保存异常: {e}")
        try:
            self._print_distributions()  # 08113：候选文件数/仓库大小分布（校准分流阈值）
        except Exception as e:
            self._wlog(f"⚠️ 分布统计输出异常: {e}")
        # 多进程解析池收尾：不等待卡住的任务（wait=False），进程退出释放内存
        try:
            if self._extract_pool is not None:
                self._extract_pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass

        # ── 分渠道统计 ──
        print(f"\n{'='*60}", flush=True)
        print(f"  搜集完成 — 总耗时 {elapsed_seconds:.0f}s", flush=True)
        print(f"{'='*60}", flush=True)
        for name, st in sorted(self._channel_stats.items()):
            print(f"  [{name}]", flush=True)
            elapsed = st.get('elapsed', '?')
            if name == "GitHub":
                print(f"    检查仓库: {st.get('repos_checked', 0)}, "
                      f"下载文件: {st.get('files_downloaded', 0)}, "
                      f"耗时: {elapsed}", flush=True)
            elif name == "种子仓库":
                print(f"    检查仓库: {st.get('repos_checked', 0)}, "
                      f"下载文件: {st.get('files_downloaded', 0)}, "
                      f"耗时: {elapsed}", flush=True)
            elif name == "Code":
                print(f"    文件匹配: {st.get('files_found', 0)}, "
                      f"仓库处理: {st.get('repos_processed', 0)}, "
                      f"耗时: {elapsed}", flush=True)
            print(f"    新增节点: {st.get('nodes_new', 0)}, "
                  f"API 调用: {st.get('api_calls', 0)}", flush=True)
            if st.get('api_report'):
                print(f"    API 详情:\n{st['api_report']}", flush=True)

        # ── API 速率门观测 ──
        print(f"\n{self.api_gate.get_stats_report()}", flush=True)

        # ── 汇总 ──
        total_new = sum(s.get("nodes_new", 0) for s in self._channel_stats.values())
        qs = self.quota_mgr.get_stats()
        print(f"  ─────────────────────────", flush=True)
        print(f"  节点总数: {self._final_node_count or len(self.unique_nodes)}, "
              f"批次: {len(self.batch_file_paths)}, "
              f"主队列仓库: {self._main_queue_total}, "
              f"源链接: {len(self.all_links)}", flush=True)
        print(f"  新增节点: {total_new}, 总API: {qs['total']}", flush=True)
        print(f"  配额剩余: {qs['remaining']}/{QUOTA_MAX_PER_HOUR}"
              f"{' ⚠️已耗尽' if qs['exceeded'] else ''}", flush=True)
        print(f"  主动限速: {qs['throttled']} 次, "
              f"失败请求: {qs['failed']}", flush=True)
        # 08103 监测：文件耗时分布（下载 vs 解析，定位慢在哪一步）
        if self._file_times_total > 0:
            _ft = list(self._file_times)
            _dl_avg = sum(t[0] for t in _ft) / len(_ft)
            _ex_avg = sum(t[1] for t in _ft) / len(_ft)
            _dl_max = max(t[0] for t in _ft)
            _ex_max = max(t[1] for t in _ft)
            print(f"  文件耗时(近{len(_ft)}条/累计{self._file_times_total}): "
                  f"下载 平均{_dl_avg:.0f}s/最大{_dl_max:.0f}s | "
                  f"解析 平均{_ex_avg:.0f}s/最大{_ex_max:.0f}s", flush=True)
        fc = len(self.failed_candidates_buffer)
        if fc > 0:
            print(f"  解析失败文件: {fc} 个 → 详见 failed_candidates.txt", flush=True)

        # ── 系统数据 ──
        net = self._net_status()
        try:
            load = os.getloadavg()[0]
        except Exception:
            load = -1
        used_gb, total_gb = self._read_mem_gb()
        print(f"\n===== 系统数据 =====", flush=True)
        print(f"  运行时长: {elapsed_seconds:.0f}s", flush=True)
        print(f"  CPU 平均负载: {load:.2f}（2核满=2.0）", flush=True)
        print(f"  内存: {used_gb:.1f}/{total_gb:.1f}GB", flush=True)
        print(f"  网络: 总下载 {net['total_mb']/1024:.2f}GB | "
              f"平均 {net['avg_mb']:.2f}MB/s | "
              f"峰值 {net['peak_mb']:.2f}MB/s(10秒采样)", flush=True)
        raw_agg = getattr(self.http, '_raw_total', 0) if hasattr(self, 'http') else 0
        raw_cnt = getattr(self.http, '_raw_count', 0) if hasattr(self, 'http') else 0
        print(f"  raw 下载: {raw_cnt} 文件 / {raw_agg/1024/1024:.1f}MB", flush=True)
        if self._quota_exhausted_times:
            print(f"  配额耗尽: {len(self._quota_exhausted_times)} 次 "
                  f"({', '.join(self._quota_exhausted_times)}) UTC", flush=True)

        # ── 跳过/失败统计 ──
        # 404 仓库持久化（去重 + 上限，跨运行跳过死链接）
        try:
            nf = sorted(self._repo_not_found)
            if len(nf) > NOT_FOUND_REPOS_MAX:
                nf = nf[-NOT_FOUND_REPOS_MAX:]
            with open(NOT_FOUND_REPOS_FILE, "w", encoding="utf-8") as f:
                f.write("\n".join(nf) + "\n")
        except Exception:
            pass
        # 08142：无节点黑名单持久化（repo + 检查时间戳，30 天重试）
        try:
            with open(NO_NODE_REPOS_FILE, "w", encoding="utf-8") as f:
                for r, ts in sorted(self._repo_no_node.items()):
                    f.write(f"{r}\t{ts:.0f}\n")
        except Exception:
            pass

        print(f"\n===== 仓库处理统计 =====", flush=True)
        print(f"  404 仓库: {len(self._repo_not_found)} 个 | 403 仓库: {len(self._repo_forbidden)} 个", flush=True)
        sk = self._skip_counts
        print(f"  语言过滤跳过: {sk['lang']} | 空仓库: {sk['size0']} | "
              f"已禁用: {sk['disabled']} | 超龄跳过解析: {sk['stale']}", flush=True)
        print(f"  信息补查: {self._backfill_count} 次"
              f"（约 {self._backfill_count / QUOTA_MAX_PER_HOUR:.1%} 配额）"
              if self._backfill_count else "  信息补查: 0 次", flush=True)

        # ── 仓库处理结果汇总（clone 相关） ──
        cok = self._repos_by_result.get("clone_ok", [])
        cfail = self._repos_by_result.get("clone_fail", [])
        print(f"\n===== Partial Clone 结果 =====", flush=True)
        print(f"  clone 成功: {len(cok)} 个", flush=True)
        print(f"  clone 失败(放弃): {len(cfail)} 个", flush=True)
        for u in cfail[:50]:
            print(f"    {u}", flush=True)
        if len(cfail) > 50:
            print(f"    ... 其余 {len(cfail)-50} 个略", flush=True)
        print(f"{'='*60}", flush=True)

        # 08171：收尾统计落盘 stats/run_stats.txt——stdout 通道死亡
        # （LogSink 消费者异常/GA 取消日志进 devnull）时统计不丢。
        # 081XX：调用已提前到 _dedup_batches_write_no 之后（GA 杀进程时
        # 统计不再丢失），此处保留注释说明位置变更。

    def _write_run_stats(self, elapsed_seconds: float = 0):
        """08171：运行统计落盘（_finalize 末尾调用，stdout 兜底）。"""
        sk = self._skip_counts
        _tags = " ".join(f"{k[1:-1]}={v}" for k, v in
                         sorted(self._tag_counts.items(),
                                key=lambda kv: -kv[1]))
        _lines = [
            f"运行统计 {now_str()}",
            f"总耗时: {elapsed_seconds:.0f}s",
            f"节点: 唯一{self._final_node_count} 提取{self._total_parsed_nodes}"
            f" 批次{len(self.batch_file_paths)}",
            f"仓库: 累计{self._repos_done_total}"
            f" 解析过{self._repos_parsed_total}"
            f" 有节点{self._repos_with_nodes_total}",
            f"分流: 全量{self._full_clone_total}"
            f"({self._full_clone_size_total:.0f}MB)"
            f" partial{self._repos_partial_total}"
            f" tree{self._repos_tree_total}",
            f"跳过: 取样{self._sample_skipped_total}"
            f" 无候选{self._repos_no_cand_total}"
            f" 黑名单{self._repos_black_hit_total}"
            f" 缓存{self._repos_cached_total}"
            f" 未更新{sk.get('stale', 0)}"
            f" 禁用{sk.get('disabled', 0)}"
            f" 大小0{sk.get('size0', 0)}"
            f" 语言{sk.get('lang', 0)}",
            f"文件: 解析{self._total_parsed_files}"
            f"({self._total_parsed_mb/1024:.1f}GB)"
            f" sha跳过{self._files_sha_skip_total}"
            f" 有节点{self._files_with_nodes_total}"
            f" 无节点{self._files_no_nodes_total}"
            f" 404:{self._files_404_total}"
            f" 超时{self._files_timeout_total}",
            f"按标志: {_tags}",
            f"API: 总{self.quota_mgr.total_calls}"
            f" 剩余{self.quota_mgr.remaining()}"
            f" 失败{self.quota_mgr.failed_calls}"
            f" 限速{self.quota_mgr.throttle_count}",
            f"配额耗尽: {len(self._quota_exhausted_times)} 次"
            f" ({', '.join(self._quota_exhausted_times)}) UTC"
            if self._quota_exhausted_times else "配额耗尽: 0 次",
        ]
        try:
            os.makedirs("stats", exist_ok=True)
            with open("stats/run_stats.txt", "w", encoding="utf-8") as f:
                f.write("\n".join(_lines) + "\n")
        except Exception:
            pass
        # 081XX：解析耗时分布追加写（5% 区间 + 最慢 10 个明细）——定位
        # 慢解析的直接依据（08241 的 45MB 文件 21s 全被平均掩盖）
        try:
            with open("stats/run_stats.txt", "a", encoding="utf-8") as f:
                f.write("\n" + self._parse_time_distribution() + "\n")
        except Exception:
            pass

    def _parse_time_distribution(self) -> str:
        """解析耗时分布（081XX）：按耗时排序分 5% 区间 + 最慢 10 个明细。

        目的：发现解析慢的文件——08241 实测 45MB 文件解析 21s+ 被
        "平均/最大"统计掩盖（大量快文件拉低平均），分位分布能看到
        最慢 5% 的耗时区间；最慢 10 个明细可直接复现改进解析算法。
        数据源 _file_times（081XX 改为无上限 list，收尾时有全部记录）。
        """
        ft = self._file_times
        if not ft:
            return "解析耗时分布: 无数据"
        exs = sorted((t[1], t[2]) for t in ft)  # (解析耗时s, 大小MB)
        n = len(exs)
        n_pct = max(1, PARSE_TIME_PERCENTILES)
        step = max(1, n // n_pct)
        lines = [f"解析耗时分布（{n} 条，每 {100 / n_pct:.0f}% 一个区间）:"]
        for i in range(0, n, step):
            seg = exs[i:i + step]
            lo, hi = seg[0][0], seg[-1][0]
            sizes = sorted(s for _, s in seg)
            lines.append(
                f"  {i * 100 // n:3d}% - "
                f"{min(n, i + step) * 100 // n:3d}%: "
                f"{lo:.1f}s - {hi:.1f}s"
                f"（{len(seg)} 个，中位 {sizes[len(sizes) // 2]:.0f}MB）")
        lines.append("最慢 10 个解析文件（耗时/大小）:")
        for ex_s, mb in reversed(exs[-10:]):
            lines.append(f"  {ex_s:.1f}s / {mb:.1f}MB")
        return "\n".join(lines)

    def search_query(self, query: str):
        """搜索单个关键词，遍历结果页。

        ⚠️ 遗留接口：当前主流程（run → _collect_keywords）已改用
        _search_query_to_queue（主线程纯供应入队），本方法仅为兼容旧
        调用保留（直接串行处理仓库，不经队列/worker 池）。

        Args:
            query: GitHub 搜索查询字符串
        """
        # 中文关键词翻更多页（前几页广告多）
        has_cjk = bool(re.search(r'[一-鿿]', query))
        max_p = (MAX_PAGES * MAX_PAGES_ZH_MULTIPLIER) if has_cjk else MAX_PAGES

        for page in range(1, max_p + 1):
            # 每次翻页前检查限流和运行时间
            if self._should_stop():
                return

            url = (f"https://api.github.com/search/repositories"
                   f"?q={query}&sort=updated&order=desc"
                   f"&per_page={PER_PAGE}&page={page}")

            resp = self.http.get(url, timeout=SEARCH_TIMEOUT,
                                 operation_name=f"搜索第{page}页")
            if not resp:
                return  # 网络错误或限流超限

            data = resp.json()
            items = data.get("items", [])
            self._wlog(f"第{page}页 "
                  f"total_count={data.get('total_count', 0)}, "
                  f"items={len(items)}")

            if not items:
                break

            for idx, item in enumerate(items, 1):
                if self.limiter.should_stop():
                    return

                repo = item.get("full_name")
                if not repo:
                    continue

                github_url = f"https://github.com/{repo}"
                self._wlog(f"检查仓库 #{idx}: {github_url}")

                # 去重检查
                if not self._check_and_add_seen(repo):
                    self._wlog(f"⏭️ 跳过已处理仓库 {github_url}")
                    continue

                self.checked_count += 1
                self._wlog(f"开始处理仓库 {github_url}")

                try:
                    # 使用搜索结果的字段替代 Repo Info API
                    _extracted, added = self.process_repo(
                        repo=repo,
                        branch=item.get("default_branch", "main"),
                        size=item.get("size", 0),
                        disabled=item.get("disabled", False),
                        pushed_at=item.get("pushed_at", ""),
                    )
                except RuntimeError:
                    self._wlog(f"⚠️ 限流超限，停止处理仓库")
                    return
                except Exception as e:
                    self._wlog(f"⚠️ 处理仓库异常 {github_url}: {e}")
                    added = 0

                # 自动种子追踪：搜索发现的仓库也记录产出
                if AUTO_SEED_ENABLED and added > 0:
                    self._update_seed_entry(self._repo_seeds, repo, added)

                time.sleep(REPO_SLEEP_SECONDS)

            time.sleep(PAGE_SLEEP_SECONDS)

    # ── 标志位系统 ──

    @staticmethod
    def _tag_depth(tag: str) -> int:
        """从标志位提取层数（源头为 0）。"""
        m = re.match(r'\[(?:user|fork|raw|kw|cd)(\d+)\]', tag)
        return int(m.group(1)) if m else 0

    @staticmethod
    def _tag_kind(tag: str) -> str:
        """从标志位提取来源类型（user/404user/fork/raw/kw/cd/空=源头）。"""
        m = re.match(r'\[(404user|user|fork|raw|kw|cd)(\d+)\]', tag)
        return m.group(1) if m else ""

    def _should_trace(self, tag: str) -> bool:
        """根据标志位判断是否需继续追踪（行为与层级解耦）。

        统一规则：层数 N < MAX_TRACE_DEPTH → 追踪；N >= MAX_TRACE_DEPTH → 不追踪。
        - 源头 [种子仓库]/[kw0]/[cd0]（depth 0）→ 追踪（0 < MAX 恒成立）
        - 源头配置为最大层级 [kw2]/[cd2]（MAX=2）→ 只解析不追踪
        - [userN]/[404userN]/[forkN]/[rawN] → 按层数判断，
          防止 [user1]→[fork2]→[user2]→[fork3]... 无底洞
        """
        return self._tag_depth(tag) < MAX_TRACE_DEPTH

    def _child_tag(self, tag: str, kind: str, depth_offset: int = 1) -> str:
        """生成子仓库标志位（层数 = 父层数 + depth_offset）。

        depth_offset 默认 1（子 = 父层+1）；404 补偿路径用 0
        （子 = 父层——"死仓库的同层顶上"，源头 404 → [user0] 成为新源头）。
        """
        depth = self._tag_depth(tag)
        return f"[{kind}{depth + depth_offset}]"

    def _trace_repo(self, repo: str, branch: str, pushed_at: str, tag: str):
        """追踪 fork/用户仓库（不受 has_nodes/已处理/超龄限制）。

        是否追踪由调用方根据标志位判定（_should_trace），
        此处只执行追踪动作。子仓库标志位 = 父层数 + 1。

        user 类仓库（[userN]/[404userN]）只追踪 fork/raw，
        不追踪 user（否则 user→user 无限递归）。

        同层（或低层覆盖）已追踪过 → 跳过（省 fork/用户 API）。
        """
        if self.quota_mgr.exceeded:
            return  # 配额耗尽：追踪需 forks/users API，零 API 模式跳过
        depth = self._tag_depth(tag)
        if self._is_traced(repo, pushed_at, depth):
            return  # 该层数已追踪过（低层覆盖高层）
        kind = self._tag_kind(tag)
        if FORK_CHAIN_ENABLED:
            self._trace_child_forks(repo, branch, tag)
            if FORK_PARENT_TRACE_ENABLED:
                self._trace_fork_chain(repo, branch, pushed_at, tag)
        if USER_REPOS_ENABLED and kind not in ("user", "404user"):
            self._trace_user_repos(repo, branch, tag)
        # 统一记录：所有渠道执行追踪都记录层数（关键词/Code 按配置为 0 层，
        # 与种子共享 0 层覆盖判断，30 天内不重复追踪未更新的仓库）
        self._mark_traced(repo, pushed_at, depth)

    def process_repo(self, repo: str, branch: str = "main",
                     size: int = -1, disabled: bool = False,
                     pushed_at: str = "", raw_depth: int = 0,
                     seed_key: str = None, is_source: bool = False,
                     language: str = "", tag: str = "[种子仓库]",
                     pos: str = ""):
        """处理单个仓库。

        使用搜索结果的字段代替 GET /repos/{repo} 调用，
        消除了一次不必要的 API 请求。

        Args:
            repo: 仓库全名 (owner/name)
            branch: 默认分支（从搜索结果获取）
            size: 仓库大小（从搜索结果获取）
            disabled: 是否已禁用（从搜索结果获取）
            pushed_at: 最后推送时间（从搜索结果获取）
            raw_depth: raw 递归发现深度
            is_source: 是否源头仓库（种子/搜索/Code 直接发现）
            language: 主要语言（SKIP_LANGUAGES 过滤用，空则放行）
            pos: 位置信息（种子序号/关键词页码，仅日志透传，处理逻辑不用）
        """
        github_url = f"https://github.com/{repo}"

        # 08171：按标志位统计仓库处理总数（种子/关键词/Code/fork/用户/raw）
        self._tag_counts[tag] = self._tag_counts.get(tag, 0) + 1

        # 08161：黑名单命中（404/403/无节点，跳过原因细分监控）——
        # 已知死仓库直接跳过（原逻辑在 resolve_branch 被动跳过，此处
        # 显式化并计数）
        if self._is_repo_dead(repo):
            self._repos_black_hit_total += 1
            return (0, 0)

        # ── 信息补全（判断前置）：缺字段 → 补查一次 repo info ──
        # 语言过滤/已解析跳过/年龄判断都需完整信息，缺什么补什么（1 次 API）。
        # 查不到（404/网络错误）→ 不跳过：用已有信息继续处理，
        # 缺字段的对应判断自动退化（pushed_at 空 → 不跳过解析/年龄），
        # 后续解析尝试失败自然返回（不再做"追踪用户"动作）。
        if INFO_BACKFILL_ENABLED and (not pushed_at or not language):
            self._backfill_count += 1
            ri = self.http.get_json(
                f"https://api.github.com/repos/{repo}",
                timeout=FILE_DOWNLOAD_TIMEOUT,
                operation_name=f"repo info ({repo})")
            if ri:
                pushed_at = ri.get("pushed_at", pushed_at)
                branch = ri.get("default_branch", branch)
                size = ri.get("size", size)
                disabled = ri.get("disabled", disabled)
                language = ri.get("language", language)
            elif f"repo info ({repo})" in self.http.last_404:
                # 404（仓库不存在/被隐藏）→ 记录跳过（持久化，下轮不再查）
                # + 追踪该用户的其他仓库（补偿损失，无条件触发）。
                # 用户仓库层级 = 当前层 + 1（[404userN]），后续按层级规则处理。
                # user/404user 类不追踪 user（防 user→user 递归）。
                self._mark_repo_not_found(repo)
                if USER_REPOS_ENABLED \
                        and self._tag_kind(tag) not in ("user", "404user"):
                    self._wlog(f"🔍 仓库 {repo} 不存在（404），追踪用户")
                    # 404 补偿：同层顶上（depth_offset=0，子 = 父层）。
                    # 源头 404 → [user0] 成为新源头可追踪扩展；[raw1] 404 → [user1]。
                    # 传父 tag（内部自动 child 一层，防双重 child 绕过层级）
                    self._trace_user_repos(repo, "main", tag, depth_offset=0)
                return (0, 0)
            # 网络错误/其他 → 不跳过：用已有信息继续处理

        # ── 语言过滤（HTML 等无价值仓库跳过）── 统一在补查后判断 ──
        if language and language in SKIP_LANGUAGES:
            self._skip_counts["lang"] += 1
            self._wlog(f"⏭️ {tag} 仓库 {github_url} 主要语言 {language}，跳过")
            return (0, 0)

        # 已处理仓库缓存检查（已解析 → 跳过解析，但按需追踪）
        if self._check_seen_cache(repo, pushed_at):
            self._skip_counts["cached"] += 1
            self._repos_cached_total += 1
            trace_txt = "（需追踪）" if (self._should_trace(tag)
                                         and not self._is_traced(
                                             repo, pushed_at,
                                             self._tag_depth(tag))) else "（无需追踪）"
            self._wlog(f"⏭️ {tag} 仓库 {github_url} 已解析，跳过解析{trace_txt}")
            if self._should_trace(tag):
                self._trace_repo(repo, branch, pushed_at, tag)
            return (0, 0)

        # 有效性检查
        if size == 0:
            self._skip_counts["size0"] += 1
            self._wlog(f"⚠️ {tag} 仓库 {github_url} 大小为 0，跳过")
            return (0, 0)
        if disabled:
            self._skip_counts["disabled"] += 1
            self._wlog(f"⚠️ {tag} 仓库 {github_url} 已禁用，跳过")
            return (0, 0)

        # 仓库年龄过滤（统一入口：搜索结果、fork链、用户仓库、raw递归）
        if pushed_at:
            try:
                pushed_time = datetime.fromisoformat(
                    pushed_at.replace("Z", "+00:00"))
                age_hours = (datetime.now(timezone.utc) - pushed_time).total_seconds() / 3600

                # 超过跳过阈值 → 不解析文件，但仍追踪 fork/用户仓库
                if SKIP_PROCESSING_AGE_HOURS > 0 and age_hours > SKIP_PROCESSING_AGE_HOURS:
                    self._mark_seen_cache(repo, pushed_at, parsed=False)
                    trace_txt = "（需追踪）" if (self._should_trace(tag)
                                                 and not self._is_traced(
                                                     repo, pushed_at,
                                                     self._tag_depth(tag))) else "（无需追踪）"
                    self._skip_counts["stale"] += 1
                    self._wlog(f"⏭️ {tag} 仓库 {github_url} "
                          f"{age_hours:.0f}h 未更新，跳过解析{trace_txt}")
                    if self._should_trace(tag):
                        self._trace_repo(repo, branch, pushed_at, tag)
                    return (0, 0)
            except Exception:
                pass  # 时间解析失败，放行
        if branch == "main" and repo in self._branch_cache and self._branch_cache[repo]:
            branch = self._branch_cache[repo]

        self._wlog(f"{tag} 仓库 {github_url} (分支: {branch}, "
              f"size: {size}KB, pushed: {pushed_at})")
        # 08113：仓库大小分布统计（收尾输出，校准 SMALL_REPO_CLONE_MB 阈值）
        if size is not None:
            _bk = self._hist_bucket_mb(size / 1024)
            self._repo_size_hist[_bk] = self._repo_size_hist.get(_bk, 0) + 1

        has_nodes_flag = [False]
        repo_stats = [0, 0]  # [extracted, added] 本仓库级统计

        # 主要路径：递归树 API
        if USE_RECURSIVE_TREE:
            success = self._process_with_recursive_tree(
                repo, branch, has_nodes_flag, raw_depth, repo_stats, tag,
                size_kb=size)
            if not success:
                # 树 API 404 可能是因为分支名不对（种子仓库进来默认是 main），
                # 懒查真实分支名，只消耗 1 次 API 调用，然后重试
                actual_branch = self._resolve_branch(repo, branch)
                if actual_branch and actual_branch != branch:
                    self._wlog(f"  分支名修正: {branch} → {actual_branch}")
                    success = self._process_with_recursive_tree(
                        repo, actual_branch, has_nodes_flag, raw_depth, repo_stats, tag,
                        size_kb=size)

            if not success:
                # 08113：去掉 Contents 回退（逐文件遍历是核心 API 配额黑洞），
                # 直接放弃——分流后 tree 失败在 _collect_files 内部已回退 clone
                self._wlog(f"树 API 失败，放弃（{repo}）")
        else:
            # USE_RECURSIVE_TREE=False 路径
            if CONTENTS_API_FALLBACK_ENABLED:
                try:
                    self.process_file_tree(repo, "", branch, has_nodes_flag,
                                           repo_stats, tag)
                except RuntimeError:
                    raise

        # 标记已处理（记录解析产出的节点数 + 主要语言）
        self._mark_seen_cache(repo, pushed_at,
                              nodes_extracted=repo_stats[0],
                              nodes_added=repo_stats[1],
                              language=language)

        # 08171：有节点仓库数（提取出 ≥1 节点的仓库）
        if repo_stats[0] > 0:
            self._repos_with_nodes_total += 1

        # 种子自动收录：唯一标准 = 提取出节点（extracted 含重复）。
        # 隐含 24h 内更新条件——超龄仓库在年龄分支已跳过解析（extracted=0）。
        if AUTO_SEED_ENABLED and has_nodes_flag[0]:
            repo_nodes = repo_stats[0]  # 提取出的有效节点数（extracted）
            seeds = getattr(self, '_repo_seeds', {})
            if repo_nodes and repo_nodes >= AUTO_SEED_MIN_NODES_FOR_SEED:
                self._update_seed_entry(seeds, repo, repo_nodes, pushed_at)
                self._wlog(f"🌱 加入种子: {repo} (提取 {repo_nodes} 节点)")

        # Fork 链追踪 + 用户仓库遍历（按标志位判定是否追踪）
        # 配额耗尽时跳过（追踪需 forks/users API）——零 API 模式下只做
        # clone/下载/解析；追踪等配额恢复后的任务自然触发。
        if self._should_trace(tag) and not self.quota_mgr.exceeded:
            self._trace_repo(repo, branch, pushed_at, tag)

        return (repo_stats[0], repo_stats[1])

    # ==================== Fork 链追踪 ====================

    def _trace_fork_chain(self, repo: str, branch: str, pushed_at: str,
                          tag: str = "[种子仓库]"):
        """追溯 fork 仓库的父仓库，遍历其所有 fork 仓库。

        触发条件：当前仓库产出了节点（has_nodes=True）。
        流程：
          1. 查询当前仓库的 parent
          2. 获取父仓库的 fork 列表
          3. 逐个处理未在 seen_repos 中的 fork 仓库
        """
        self._wlog(f"🔗 {tag} 开始 Fork 链追踪 ({repo})")

        # 1. 查父仓库
        repo_data = self.http.get_json(
            f"https://api.github.com/repos/{repo}",
            timeout=FILE_DOWNLOAD_TIMEOUT,
            operation_name=f"repo info ({repo})")
        if not repo_data:
            return
        parent = repo_data.get("parent")
        if not parent:
            return
        parent_name = parent.get("full_name")
        if not parent_name:
            return
        self._wlog(f"  {tag} 父仓库: {parent_name}")

        # 2. 如果父仓库还没处理过，先处理父仓库
        if not self._is_seen(parent_name):
            try:
                _pextracted, _padded = self.process_repo(
                    parent_name,
                    branch=parent.get("default_branch", branch),
                    size=parent.get("size", -1),
                    disabled=False,
                    pushed_at=parent.get("pushed_at", ""),
                    language=parent.get("language", ""),
                    tag=self._child_tag(tag, "fork"))
            except Exception as e:
                self._wlog(f"  ⚠️ 父仓库 {parent_name}: {e}")
                _padded = 0
            self._wlog(f"  父仓库 +{_padded} 个节点")

        # 3. 遍历父仓库的 fork 列表（兄弟仓库）
        qualified = []
        max_pages = (FORK_SIBLING_MAX // FORK_PER_PAGE) + 1
        for page in range(1, max_pages + 1):
            forks = self.http.get_json(
                f"https://api.github.com/repos/{parent_name}/forks"
                f"?sort=stargazers&per_page={FORK_PER_PAGE}&page={page}",
                timeout=FILE_DOWNLOAD_TIMEOUT,
                operation_name=f"forks of {parent_name} (p{page})")
            if not forks or not isinstance(forks, list):
                break
            for fork in forks:
                fn = fork.get("full_name")
                if not fn: continue
                if len(qualified) >= FORK_SIBLING_MAX: break
                qualified.append(fork)
            if len(qualified) >= FORK_SIBLING_MAX:
                break

        if qualified:
            self._run_fork_batch(qualified, branch, "🍴 兄弟仓库",
                                 self._child_tag(tag, "fork"))

    def _process_fork_repo(self, fork: dict, branch: str) -> tuple:
        """处理单个 fork/用户仓库（串行降级路径）。"""
        self.http = HttpClient(token=self.token, rate_limiter=None,
                               quota_manager=self.quota_mgr,
                               api_gate=self.api_gate)
        fork_name = fork.get("full_name")
        added = 0
        try:
            _extracted, added = self.process_repo(
                fork_name,
                branch=fork.get("default_branch", branch),
                size=fork.get("size", -1),
                disabled=fork.get("disabled", False),
                pushed_at=fork.get("pushed_at", ""),
                language=fork.get("language", ""))
        except Exception as e:
            self._wlog(f"  ⚠️ {fork_name}: {e}")
        return (fork_name, added)

    def _trace_child_forks(self, repo: str, branch: str,
                           tag: str = "[种子仓库]"):
        """遍历本仓库的直接 fork（子仓库），查其节点产出。"""
        self._wlog(f"🔗 {tag} 查子仓库: {repo}")
        qualified = []
        max_pages = (FORK_CHILD_MAX // FORK_PER_PAGE) + 1
        for page in range(1, max_pages + 1):
            forks = self.http.get_json(
                f"https://api.github.com/repos/{repo}/forks"
                f"?sort=stargazers&per_page={FORK_PER_PAGE}&page={page}",
                timeout=FILE_DOWNLOAD_TIMEOUT,
                operation_name=f"forks of {repo} (p{page})")
            if not forks or not isinstance(forks, list):
                break
            for fork in forks:
                fn = fork.get("full_name")
                if not fn: continue
                if len(qualified) >= FORK_CHILD_MAX: break
                qualified.append(fork)
            if len(qualified) >= FORK_CHILD_MAX:
                break

        if qualified:
            self._run_fork_batch(qualified, branch, "🍴 子仓库",
                                 self._child_tag(tag, "fork"))

    def _run_fork_batch(self, forks: list, branch: str, label: str,
                        tag: str = "[user1]"):
        """提交 fork/用户仓库到发现队列（PriorityQueue，优先级按需追踪）。

        tag 决定子仓库标志位（[userN]/[forkN]），
        Worker 按 _should_trace(tag) 判断是否继续追踪。
        入队时统一做：本轮去重（_check_and_add_seen）+ 追踪记录过滤（_is_traced）。
        """
        if not getattr(self, '_disc_queue', None):  # 降级：无共用池时串行
            for fork in forks:
                fn = fork.get("full_name")
                if not fn or not self._check_and_add_seen(fn): continue
                fn, nn = self._process_fork_repo(fork, branch)
                if nn > 0: self._wlog(f"  {label}: {fn} +{nn}")
                time.sleep(REPO_SLEEP_SECONDS)
            return

        # 有共用池 → 提交到发现队列
        # 去重标记（_check_and_add_seen）统一在这里做（防并发重复入队）——
        # 注意顺序：被 _is_traced 挡下的仓库不标记，本轮内它更新后可被重新发现；
        # 已追踪过（低层覆盖且未超期）的仓库不入扩展队列，避免 Worker 空转
        # （已解析跳过 + 无需追踪 = 0 价值）。仓库更新（pushed_at 不同）→
        # _is_traced False → 照常入队重新解析/追踪。
        depth = self._tag_depth(tag)
        enqueued = 0
        for fork in forks:
            fn = fork.get("full_name")
            if not fn or not self._check_and_add_seen(fn): continue
            if self._is_traced(fn, fork.get("pushed_at", ""), depth):
                continue
            self._disc_put(("GitHub", fn,
                            {"branch": fork.get("default_branch", branch),
                             "size": fork.get("size", -1),
                             "disabled": fork.get("disabled", False),
                             "pushed_at": fork.get("pushed_at", ""),
                             "language": fork.get("language", ""),
                             "tag": tag}),
                           label=label)
            enqueued += 1
        self._wlog(f"  {label}: {enqueued}/{len(forks)} 个 → {self._qs()}")

    def _trace_user_repos(self, repo: str, branch: str,
                          tag: str = "[种子仓库]", depth_offset: int = 1):
        """遍历同用户名下的所有公开仓库，查是否有节点产出。

        触发条件：仓库产出了节点（不管是否重复）。
        通过 GET /users/{owner}/repos API 获取仓库列表，逐个检查。

        Args:
            tag: 父仓库标志位（内部自动 _child_tag 生成子仓库层级）。
            depth_offset: 子仓库层数偏移（默认 1 = 父层+1）；
                404 补偿路径传 0（子 = 父层，"同层顶上"，
                源头 404 → [user0] 成为新源头，可追踪扩展）。
        """
        owner = repo.split("/")[0]
        self._wlog(f"👤 {tag} 遍历用户仓库: {owner}")

        repo_pattern = re.compile(
            r'https://github\.com/([a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+)'
        )
        found = set()
        qualified = []
        for page in range(1, 5):
            repos_data = self.http.get_json(
                f"https://api.github.com/users/{owner}/repos"
                f"?sort=updated&per_page=100&page={page}",
                timeout=FILE_DOWNLOAD_TIMEOUT,
                operation_name=f"user repos {owner} (p{page})")
            if not repos_data or not isinstance(repos_data, list):
                break
            for r in repos_data:
                fn = r.get("full_name")
                if not fn:
                    continue
                if fn in found: continue
                found.add(fn)
                if USER_REPOS_MAX_PER_USER and len(qualified) >= USER_REPOS_MAX_PER_USER:
                    break
                qualified.append(r)

        if qualified:
            self._run_fork_batch(qualified, branch, "👤 用户仓库",
                                 self._child_tag(tag, "user", depth_offset))
        self._wlog(f"  用户 {owner} 共查 {len(qualified)} 个仓库")

    # ==================== 递归树 API 处理 ====================

    def _process_with_recursive_tree(self, repo: str, branch: str,
                                     has_nodes: List[bool],
                                     raw_depth: int = 0,
                                     stats: List[int] = None,
                                     tag: str = "[种子仓库]",
                                     size_kb: int = -1) -> bool:
        """使用 git/trees API 获取递归文件树。

        一次 API 调用获取全仓库文件列表，然后过滤、下载、提取。

        Args:
            stats: [extracted, added] 本仓库级统计累加。
            tag: 仓库标志位（透传给 _handle_one_file 用于递归发现）。
            size_kb: 仓库大小（KB，clone 统计用；-1 = 未知）。

        Returns:
            True 表示处理完成（含失败放弃），False 表示处理失败
            （08113：Contents 回退已移除——逐文件遍历是核心 API 配额黑洞）
        """
        if self.limiter.should_stop():
            raise RuntimeError("限流超限")

        # ── 仓库大小分流（08113 + 08141 全量档）──
        # 极小仓库（size < FULL_CLONE_MB）→ 全量 clone + 本地解析
        #   （零 API、不占 raw 速率；08141 配额耗尽时的零 API 供给）。
        # 小仓库（[FULL_CLONE_MB, SMALL_REPO_CLONE_MB)）→ partial clone
        #   拿列表 + raw 下载候选。
        # 大仓库（≥ SMALL_REPO_CLONE_MB）→ tree API 拿列表（大仓库 clone
        #   元数据大，tree 响应可承受；且只下候选文件，不下载仓库内容）。
        # 回退链：全量 clone 失败 → partial clone（降级为小仓库路径）；
        # 小 clone 失败 → tree；大 tree 失败/截断 → partial clone；
        # 再失败 → 放弃 + 日志（去掉 Contents 回退——配额黑洞）。
        _full_clone = CLONE_FIRST_MODE and (
            size_kb is not None and 0 <= size_kb < FULL_CLONE_MB * 1024)
        _small_repo = CLONE_FIRST_MODE and (
            size_kb is None
            or (FULL_CLONE_MB * 1024 <= size_kb < SMALL_REPO_CLONE_MB * 1024))

        if _full_clone:
            if self._full_clone_local_parse(repo, size_kb):
                return True
            # 全量 clone 失败 → 降级 partial clone（继续下方小仓库路径）
            self._wlog(f"全量 clone 失败，降级 partial clone（{repo}）")

        if _small_repo:
            entries = self._partial_clone_file_list(repo, branch, size_kb=size_kb)
            if entries is not None:
                return self._process_file_list(repo, branch, entries,
                                               has_nodes, raw_depth, stats, tag)
            # 小仓库 clone 失败 → 回退 tree（小仓库 tree 响应小，API 成本低）
            self._wlog(f"小仓库 clone 失败，回退 tree API（{repo}）")

        tree_url = f"https://api.github.com/repos/{repo}/git/trees/{branch}?recursive=1"
        resp = self.http.get(tree_url, timeout=TREE_API_TIMEOUT,
                             operation_name="递归树")
        if not resp:
            # 大仓库 tree 失败（网络错误）→ 回退 partial clone
            if CLONE_FIRST_MODE:
                self._wlog(f"tree API 失败，回退 partial clone（{repo}）")
                entries = self._partial_clone_file_list(repo, branch, size_kb=size_kb)
                if entries is not None:
                    return self._process_file_list(repo, branch, entries,
                                                   has_nodes, raw_depth, stats, tag)
                self._wlog(f"tree + clone 均失败，放弃（{repo}）")
                self._repos_by_result["clone_fail"].append(
                    f"https://github.com/{repo}")
                return True  # 视为处理完成（无节点）
            return False

        data = resp.json()
        if data.get('truncated', False):
            # tree 截断（超大仓库树响应过大）→ 回退 partial clone
            if CLONE_FIRST_MODE:
                self._wlog(f"树数据被截断，回退 partial clone（{repo}）")
                entries = self._partial_clone_file_list(repo, branch, size_kb=size_kb)
                if entries is not None:
                    return self._process_file_list(repo, branch, entries,
                                                   has_nodes, raw_depth, stats, tag)
                self._wlog(f"tree（截断）+ clone 均失败，放弃（{repo}）")
                self._repos_by_result["clone_fail"].append(
                    f"https://github.com/{repo}")
                return True  # 视为处理完成（无节点）
            else:
                self._wlog(f"树数据被截断（clone 关闭），放弃（{repo}）")
                return True
        entries = data.get('tree', [])
        if not entries:
            return True
        self._repos_tree_total += 1  # 08171：tree API 仓库数
        return self._process_file_list(repo, branch, entries, has_nodes,
                                       raw_depth, stats, tag)

    def _process_file_list(self, repo: str, branch: str, entries: list,
                           has_nodes: List[bool], raw_depth: int,
                           stats: List[int], tag: str = "[种子仓库]") -> bool:
        """过滤文件列表 + 下载 + 提取（tree API 与 Partial Clone 共用）。

        Args:
            entries: [{path, sha, size, type}, ...] 文件列表。
            tag: 仓库标志位（透传给 _handle_one_file）。
        """
        # ---- 收集候选文件 ----
        files_to_check = []
        skipped_by_processed = 0
        skipped_by_cache = 0
        skipped_by_ext = 0
        for e in entries:
            if e.get('type') != 'blob':
                continue
            path = e.get('path', '')
            ext = os.path.splitext(path)[1].lower()
            if ext not in ALLOWED_EXTENSIONS:
                skipped_by_ext += 1
                continue
            sha = e.get('sha', '')
            if sha in self.processed_file_shas:
                skipped_by_processed += 1
                continue
            # SHA 缓存检查
            if self._sha_in_cache(sha):
                skipped_by_cache += 1
                self._files_sha_skip_total += 1  # 08171：SHA 跳过文件数
                continue
            files_to_check.append((path, sha, e.get('size', 0)))

        if not files_to_check:
            # 08161：无候选文件计数（跳过原因细分监控）
            self._repos_no_cand_total += 1
            self._wlog(f"仓库 https://github.com/{repo} 无候选文件 "
                  f"(总计 {len([e for e in entries if e.get('type')=='blob'])} blob"
                  f", 扩展名过滤 {skipped_by_ext}"
                  f", processed_shas 跳过 {skipped_by_processed}"
                  f", SHA 缓存跳过 {skipped_by_cache})")
            # 如果有被 SHA 缓存或 processed_shas 跳过的文件，说明已处理过
            if skipped_by_cache > 0 or skipped_by_processed > 0:
                has_nodes[0] = True  # 避免已处理过的仓库被加入黑名单
            return True

        # ---- 文件时间检查（阈值策略） ----
        # 关键洞察：raw 下载免费不计 API 配额，但大量下载耗时巨大。
        # 少量候选文件 → 直接下载（零 API 成本）。
        # 大量候选文件 → 先通过 commits API 确定 24h 内变更的文件，再下载。
        # Clone-First 模式：跳过 commits（零核心 API），符合后缀的全量下载解析，
        # SHA 缓存过滤在上方已生效，已处理过的文件不会重复下载。
        if not CLONE_FIRST_MODE and len(files_to_check) > MAX_RAW_DOWNLOADS_PER_REPO:
            self._wlog(f"仓库 https://github.com/{repo} "
                  f"候选文件较多 ({len(files_to_check)} 个)，"
                  f"先通过 commits API 过滤")
            changed = self._get_recently_changed_file_set(repo, branch)
            if changed is not None:
                before = len(files_to_check)
                files_to_check = [(p, s, sz) for p, s, sz in files_to_check if p in changed]
                self._wlog(f"  commits 过滤: {before} → {len(files_to_check)} "
                      f"(变更文件 {len(changed)} 个)")
            else:
                # commits API 失败 → 降级为直接下载（宁可多下不可漏掉）
                self._wlog(f"  commits API 失败，降级为直接下载")

        if not files_to_check:
            self._wlog(f"仓库 https://github.com/{repo} "
                  f"候选文件经时间过滤后为空")
            has_nodes[0] = True  # 24h 无新文件 ≠ 无节点，避免误加入黑名单
            return True

        # 限制处理数量（安全闸，防止极端情况）
        if MAX_COMMITS_PER_REPO is not None and len(files_to_check) > MAX_COMMITS_PER_REPO:
            self._wlog(f"⚠️ 候选文件过多 ({len(files_to_check)} 个)，"
                  f"仅处理前 {MAX_COMMITS_PER_REPO} 个")
            files_to_check = files_to_check[:MAX_COMMITS_PER_REPO]

        self._wlog(f"仓库 https://github.com/{repo} "
              f"候选文件 {len(files_to_check)} 个")
        # 08113：候选文件数分布统计（收尾输出，校准分流阈值用）
        _ck = self._hist_bucket_count(len(files_to_check))
        self._candidate_hist[_ck] = self._candidate_hist.get(_ck, 0) + 1
        # 08141：解析过文件的仓库统计（累计数 + 大小，调用方 _collect_files
        # 无 size 透传，用 _repo_size_hist 已覆盖大小分布——此处只计数）
        self._repos_parsed_total += 1
        # 08142 取样判断：候选 > SAMPLE_THRESHOLD 时先取样（raw 下载解析），
        # 全无节点 → 跳过仓库 + 无节点黑名单（不浪费剩余文件下载/解析）
        _cont, _sampled_ok = self._sample_judge_remote(
            repo, branch, files_to_check)
        if not _cont:
            self._mark_repo_no_node(repo)
            self._sample_skipped_60s.append(time.time())
            self._sample_skipped_total += 1
            self._wlog(f"⏭️ 取样全无节点，跳过并加入黑名单"
                  f"（{repo}，候选 {len(files_to_check)} 个）")
            has_nodes[0] = True  # 视为处理完成（不误入 404 黑名单）
            return True

        # ---- 08174 异步下载管道 ----
        # 取样已通过（有节点）→ 剩余文件全部入待下载队列（下载线程消费）。
        # worker 不再阻塞在下载/解析（08181 实测 worker 卡 1300s 的根因
        # 之一）；入队失败（收尾中断）视为处理完成，不重试不阻塞。
        # 081XX：取样确认有节点 → 仓库整体有节点（取样文件用独立 has_node，
        # 异步文件的 has_node 聚合会漏掉取样节点，置 True 防误判）
        if _sampled_ok:
            has_nodes[0] = True
        self._enqueue_downloads(repo, branch, files_to_check, has_nodes,
                                raw_depth, stats, tag)
        return True

    def _record_clone(self, repo: str, size_kb: int, time_s: float,
                      files: int, ok: bool, reason: str, detail: str,
                      traffic_bytes: int = 0):
        """记录一次 clone 结果（CLONE_FIRST 实验统计，_finalize 聚合写 JSON）。

        traffic_bytes: 本次 clone 的下载流量近似（tmp 目录大小）。
        """
        try:
            self._clone_stats.append((repo, size_kb, time_s, files, ok))
            now = time.time()
            # 监控近 60 秒窗口：成功入 ok_window；流量（成功+失败）入 traffic_window
            if ok:
                self._clone_ok_window.append((now, 1, files))
            else:
                self._clone_fail_count += 1
                self._clone_fail_breakdown[reason] = \
                    self._clone_fail_breakdown.get(reason, 0) + 1
                self._clone_fail_details.append(
                    {"repo": repo, "size_kb": size_kb, "time_s": round(time_s),
                     "reason": reason, "detail": detail})
                self._repos_by_result["clone_fail"].append(
                    f"https://github.com/{repo}")  # 汇总展示
                self._wlog(f"⚠️ Partial Clone 失败 [{reason}]: {repo} "
                      f"({time_s:.0f}s) {detail[:200]}")
            if traffic_bytes > 0:
                self._clone_traffic_window.append((now, traffic_bytes))
                # 滑窗裁剪（保留 60 秒）
                while self._clone_traffic_window \
                        and self._clone_traffic_window[0][0] < now - 60:
                    self._clone_traffic_window.popleft()
            while self._clone_ok_window \
                    and self._clone_ok_window[0][0] < now - 60:
                self._clone_ok_window.popleft()
        except Exception:
            pass

    @staticmethod
    def _dir_size_bytes(path: str) -> int:
        """递归统计目录真实占用字节数。

        不用 shutil.disk_usage(path).used——它返回整个文件系统的已用
        空间（08084 监控显示 clone 流量 58616MB 假数值的根因）。
        """
        total = 0
        try:
            for root, _dirs, files in os.walk(path):
                for f in files:
                    try:
                        total += os.path.getsize(os.path.join(root, f))
                    except OSError:
                        pass
        except Exception:
            pass
        return total

    @staticmethod
    def _classify_clone_error(err_text: str) -> str:
        """按 stderr 关键词分类 clone 失败原因（统计用）。

        顺序注意：disabled 检查在 auth 前——GitHub 封禁仓库的 git 输出
        含 "Access to this repository has been disabled" + "error: 403"，
        若 auth 的 "403" 裸匹配在前会误判（08084 alanbobs999 案例）。
        故去掉裸 "403" 匹配（403 语义太泛，auth/disabled 均可能）。
        """
        e = (err_text or "").lower()
        if "repository not found" in e:
            return "repo_not_found"
        if "rate limit" in e or "too many" in e or "429" in e:
            return "rate_limit"
        if "disabled" in e:
            return "disabled"
        if "authentication" in e or "invalid username" in e \
                or "access denied" in e:
            return "auth"
        if "ls-tree" in e:
            return "ls_tree"
        return "other"

    # ---- 08142：取样跳过无关仓库 ----
    # 背景：deepseek-harness 系列（80+ fork × 3700-4355 候选文件）是配置/
    # 数据文件（匹配后缀但无节点）——每仓库 30-90 分钟拖垮吞吐。
    # 机制：候选 > SAMPLE_THRESHOLD 时，各后缀取 ≤SAMPLE_PER_EXT 个不同
    # 目录的文件下载解析；取样全部无节点 → 跳过仓库 + 无节点黑名单
    # （NO_NODE_RETRY_DAYS 重试）；有节点 → 继续处理剩余候选。

    @staticmethod
    def _pick_samples(files):
        """候选文件取样：各后缀 ≤SAMPLE_PER_EXT 个，目录尽量不同（代表性）。

        Args:
            files: [(path, sha, size)] 候选文件列表。
        Returns:
            取样子列表（原始元素引用）。
        """
        if len(files) <= SAMPLE_THRESHOLD:
            return []
        by_ext = {}
        for item in files:
            ext = os.path.splitext(item[0])[1].lower()
            by_ext.setdefault(ext, []).append(item)
        samples = []
        for _ext, lst in by_ext.items():
            # 目录分层轮询：不同目录优先，目录不足 10 个则同目录补足
            by_dir = {}
            for item in lst:
                d = os.path.dirname(item[0])
                by_dir.setdefault(d, []).append(item)
            dirs = sorted(by_dir.keys())
            picked = []
            di = 0
            while len(picked) < SAMPLE_PER_EXT and len(picked) < len(lst):
                d = dirs[di % len(dirs)]
                if by_dir[d]:
                    picked.append(by_dir[d].pop(0))
                di += 1
            samples.extend(picked)
        return samples

    def _sample_judge_remote(self, repo: str, branch: str,
                             files_to_check) -> tuple:
        """远程取样判断（clone/tree 路径）：取样文件走 raw 下载解析。

        Returns: (是否继续处理, 取样是否确认有节点)。
        - (False, False)：取样全无节点（调用方跳过仓库 + 黑名单）
        - (True, True)：取样确认有节点（继续处理，仓库整体有节点）
        - (True, False)：候选 ≤ 阈值未取样（继续处理，有无节点未知）
        081XX：第二元素供调用方置外层 has_nodes——取样文件用独立
        has_node，异步文件的 has_node 聚合会漏掉取样节点。
        取样文件解析后 SHA 已标记（_handle_one_file 内），剩余处理时
        按 sha 排除避免重复下载。
        """
        samples = self._pick_samples(files_to_check)
        if not samples:
            return True, False  # 候选 ≤ 阈值 → 不取样，直接处理
        has_node = [False]
        _s_set = set(s[1] for s in samples)
        for path, sha, _sz in samples:
            if self.limiter.should_stop():
                break
            self._handle_one_file(repo, branch, path, sha, has_node,
                                  raw_depth=0, stats=[0, 0],
                                  tag="[取样]", size=_sz)
        if not has_node[0]:
            return False, False
        # 取样有节点：剩余处理时排除已解析的取样文件（sha 已标记）
        files_to_check[:] = [x for x in files_to_check if x[1] not in _s_set]
        return True, True

    def _sample_judge_local(self, repo: str, local_files) -> tuple:
        """本地取样判断（全量 clone 路径）：取样文件磁盘直读（不走 raw）。

        local_files: [绝对路径] 工作区候选文件。
        Returns: (是否继续处理, 取样是否确认有节点)。
        - (False, False)：取样全无节点（调用方删仓库 + 黑名单）
        - (True, True)：取样确认有节点（继续处理，仓库整体有节点）
        - (True, False)：候选 ≤ 阈值未取样（继续处理，有无节点未知）
        """
        if len(local_files) <= SAMPLE_THRESHOLD:
            return True, False
        # 按后缀目录分层取样（同 _pick_samples，但输入是路径列表）
        by_ext = {}
        for p in local_files:
            ext = os.path.splitext(p)[1].lower()
            by_ext.setdefault(ext, []).append(p)
        samples = []
        for _ext, lst in by_ext.items():
            by_dir = {}
            for p in lst:
                d = os.path.dirname(p)
                by_dir.setdefault(d, []).append(p)
            dirs = sorted(by_dir.keys())
            picked = []
            di = 0
            while len(picked) < SAMPLE_PER_EXT and len(picked) < len(lst):
                d = dirs[di % len(dirs)]
                if by_dir[d]:
                    picked.append(by_dir[d].pop(0))
                di += 1
            samples.extend(picked)
        has_node = [False]
        for p in samples:
            if self.limiter.should_stop():
                break
            try:
                with open(p, "rb") as fh:
                    data = fh.read()
            except OSError:
                continue
            self._handle_one_file(repo, "HEAD", p, "", has_node,
                                  raw_depth=0, stats=[0, 0],
                                  tag="[取样]", size=len(data),
                                  content_bytes_preloaded=data)
        return has_node[0], has_node[0]

    def _full_clone_local_parse(self, repo: str, size_kb: int = -1) -> bool:
        """08141：<FULL_CLONE_MB 仓库全量 clone → 本地遍历候选文件解析。

        全量 clone（checkout 工作区）→ os.walk 遍历符合后缀的文件 →
        逐个读入内存走 _handle_one_file(preloaded) 解析 → 处理完删 tmp。
        零 API、不占 raw 连接配额——配额耗尽时 work 的零 API 供给 +1 种。
        磁盘警戒：工作区可用 < FULL_CLONE_DISK_MIN_GB 时跳过。

        Returns: True 处理完成（含失败放弃，不重试）。
        """
        import tempfile
        import shutil as _shutil
        import signal
        try:
            # 磁盘警戒（全量 clone 产物瞬时占用，GA 磁盘 70GB+ 正常不会到）
            try:
                _du = _shutil.disk_usage(os.getcwd())
                if _du.free / 1024 ** 3 < FULL_CLONE_DISK_MIN_GB:
                    self._wlog(f"⚠️ 磁盘可用 < {FULL_CLONE_DISK_MIN_GB}GB，"
                          f"跳过全量 clone（{repo}）")
                    return True
            except Exception:
                pass
            tmp = tempfile.mkdtemp(prefix="fclone_")
        except Exception:
            return True
        _prev_state = self._worker_state.get(
            threading.current_thread().name, {}).get("what", "")
        _async_kept = False  # 081XX：取样通过入队后 tmp 交给仓库完成事件删
        try:
            token = self.token or GITHUB_TOKEN
            if not token:
                return True
            self._clone_sem.acquire()
            self._clone_active += 1
            if self._clone_active > self._clone_active_peak:
                self._clone_active_peak = self._clone_active
            self._set_worker_state(f"全量Clone {repo}")
            clone_url = f"https://x-access-token:{token}@github.com/{repo}.git"
            p = subprocess.Popen(
                ["git", "clone", "--depth", "1", "--single-branch",
                 clone_url, tmp],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                start_new_session=True)
            try:
                _, err_text = p.communicate(timeout=PARTIAL_CLONE_TIMEOUT)
            except subprocess.TimeoutExpired:
                os.killpg(p.pid, signal.SIGKILL)
                p.communicate()
                self._wlog(f"⚠️ 全量 clone 超时，放弃（{repo}）")
                return True
            if p.returncode != 0:
                self._wlog(f"⚠️ 全量 clone 失败，放弃（{repo}）: "
                      f"{(err_text or '')[-200:]}")
                return True

            # 遍历工作区收集候选后缀文件路径
            _size_mb = size_kb / 1024 if size_kb and size_kb > 0 else 0
            _cands = []
            for root, _dirs, files in os.walk(tmp):
                if self.limiter.should_stop():
                    break
                for fn in files:
                    ext = os.path.splitext(fn)[1].lower()
                    if ext not in ALLOWED_EXTENSIONS:
                        continue
                    _cands.append(os.path.join(root, fn))
            # 08142 取样判断：候选 > SAMPLE_THRESHOLD 时先取样（磁盘直读），
            # 全无节点 → 删仓库 + 无节点黑名单（不浪费剩余文件的读取/解析）
            _cont, _sampled_ok = self._sample_judge_local(repo, _cands)
            if not _cont:
                self._mark_repo_no_node(repo)
                self._sample_skipped_60s.append(time.time())
                self._sample_skipped_total += 1
                self._wlog(f"⏭️ 取样全无节点，跳过并加入黑名单（{repo}，"
                      f"候选 {len(_cands)} 个）")
                return True  # finally 删 tmp
            # 取样有节点（或候选 ≤ 阈值）→ 剩余文件入待下载队列（081XX
            # 第 3 批：fclone 也走异步管道——本地文件带 local_path 标记
            # 入队，下载线程读盘解析，worker 不再逐个同步解析（08181
            # worker 卡 1300s 的根因之一）。文件内容读入后立即释放，
            # 不驻留内存。tmp 目录由仓库完成事件删除（_handle_repo_result
            # 的 mode=fclone 分支）——此处不能删，文件还没处理完。
            _files = []
            _lps = []
            for _fpath in _cands:
                try:
                    _sz = os.path.getsize(_fpath)
                except OSError:
                    _sz = 0
                _files.append((os.path.basename(_fpath), "", _sz))
                _lps.append(_fpath)
            _has = [False]
            # 081XX：取样确认有节点 → 仓库整体有节点（取样文件用独立
            # has_node，异步文件的聚合会漏掉取样节点）
            if _sampled_ok:
                _has[0] = True
            _n = len(_lps)
            _dl_total = sum(f[2] for f in _files)
            self._enqueue_downloads(
                repo, "HEAD", _files, _has, raw_depth=0, stats=[0, 0],
                tag="[全量]", local_paths=_lps, mode="fclone", tmp_dir=tmp)
            _async_kept = True  # tmp 交给仓库完成事件删除
            _now = time.time()
            self._full_clone_60s.append((_now, _size_mb))
            self._full_clone_total += 1
            self._full_clone_size_total += _size_mb
            self._wlog(f"📦 全量下载: {repo} ({_size_mb:.1f}MB, "
                  f"入队 {_n} 候选文件/{_dl_total/1024/1024:.1f}MB，"
                  f"异步解析，tmp 由仓库完成事件删除)")
            return True
        except Exception:
            return True
        finally:
            self._clone_sem.release()
            self._clone_active -= 1
            self._set_worker_state(_prev_state)
            # 081XX：取样通过且入队成功 → tmp 由仓库完成事件删除（等全部
            # 文件处理完）；取样失败/异常路径 → 这里立即删（防残留）
            if not _async_kept:
                try:
                    _shutil.rmtree(tmp, ignore_errors=True)
                except Exception:
                    pass

    def _partial_clone_file_list(self, repo: str, branch: str,
                                 size_kb: int = -1):
        """用 git partial clone 获取完整文件列表（零 API 配额）。

        git clone --filter=blob:none 只下载 commit + tree 对象（路径名），
        不下载文件内容（blob）。git ls-tree -r HEAD 输出完整路径。

        并发限制：Semaphore(PARTIAL_CLONE_CONCURRENCY) 防资源竞争。
        超时处理：Popen(start_new_session) 独立进程组，killpg 只杀自己的 git
        （不误杀其他 Worker 的 clone）。结果记录到 _repos_by_result 供汇总展示。

        Returns:
            [{path, sha, size, type}, ...] 或 None（失败）
        """
        import tempfile
        import signal
        try:
            tmp = tempfile.mkdtemp(prefix="pclone_")
        except Exception:
            self._wlog("⚠️ mkdtemp 失败（磁盘空间不足？）")
            return None
        acquired = False
        _t0 = time.time()
        _size_kb = size_kb if size_kb is not None else -1
        # 记录调用前状态：clone 只是仓库处理的一个环节，完成后恢复
        # （08084 监控 bug：finally 无条件设"空闲"，把仍在处理中的
        # worker 状态覆盖，导致假"0忙/36闲"误导排查）。
        # 必须在 try 前定义（无 token 提前 return 时 finally 也要引用）。
        _prev_state = self._worker_state.get(
            threading.current_thread().name, {}).get("what", "")
        try:
            token = self.token or GITHUB_TOKEN
            if not token:
                self._wlog(f"⚠️ Partial Clone 无 token，跳过")
                return None
            # 并发限制（最多 PARTIAL_CLONE_CONCURRENCY 个 clone）
            self._clone_sem.acquire()
            acquired = True
            self._clone_active += 1  # 监控采样
            if self._clone_active > self._clone_active_peak:
                self._clone_active_peak = self._clone_active
            self._set_worker_state(f"PartialClone {repo}")
            # 大仓库标记日志（size > 1GB = 1024*1024 KB），实时观察大仓库影响
            if _size_kb > 1024 * 1024:
                self._wlog(f"📦 大仓库 [{_size_kb/1024/1024:.1f}GB] clone 开始: {repo} "
                      f"(并发 {self._clone_active}/{PARTIAL_CLONE_CONCURRENCY})")
            clone_url = f"https://x-access-token:{token}@github.com/{repo}.git"
            # 不带 --branch：用远端 HEAD（默认分支），避免分支名错误导致 clone 失败
            # （种子仓库默认传 main，实际分支可能是 master，--branch 错则直接失败）。
            p = subprocess.Popen(
                ["git", "clone", "--depth", "1", "--filter=blob:none",
                 "--no-checkout", "--single-branch",
                 clone_url, tmp],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                start_new_session=True)  # 独立进程组，killpg 只杀自己
            try:
                _, err_text = p.communicate(timeout=PARTIAL_CLONE_TIMEOUT)
            except subprocess.TimeoutExpired:
                os.killpg(p.pid, signal.SIGKILL)  # 只杀自己的 git 进程组
                p.communicate()
                self._record_clone(repo, _size_kb, time.time() - _t0, 0, False,
                                   "timeout", f"超时 {PARTIAL_CLONE_TIMEOUT}s",
                                   self._dir_size_bytes(tmp))
                return None
            if p.returncode != 0:
                reason = self._classify_clone_error(err_text or "")
                self._record_clone(repo, _size_kb, time.time() - _t0, 0, False,
                                   reason, (err_text or "")[-800:],
                                   self._dir_size_bytes(tmp))
                return None
            # 注意：ls-tree 不带 -l！partial clone（blob:none）下 blob 不在本地，
            # -l 要显示文件大小会触发"逐 blob 延迟获取"（每个文件一次网络请求）——
            # 大仓库几万文件 = 几万请求 → 300s 超时（08083 日志 135 次
            # "ls-tree timed out"、大仓库 175 个全部失败）。size 只用于下载排序
            # 启发式，放弃它不影响正确性（下载后以实际大小为准）。
            r2 = subprocess.run(
                ["git", "-C", tmp, "ls-tree", "-r", "HEAD"],
                capture_output=True, text=True, timeout=300)
            if r2.returncode != 0:
                self._record_clone(repo, _size_kb, time.time() - _t0, 0, False,
                                   "ls_tree", (r2.stderr or "")[-800:],
                                   self._dir_size_bytes(tmp))
                return None
            entries = []
            for line in r2.stdout.splitlines():
                parts = line.split("\t", 1)
                if len(parts) != 2:
                    continue
                meta, path = parts
                meta_parts = meta.split()
                if len(meta_parts) < 3:
                    continue
                _mode, etype, sha = meta_parts[:3]
                if etype != "blob":
                    continue
                entries.append({"path": path, "sha": sha, "size": 0,
                                "type": "blob"})
            self._repos_by_result["clone_ok"].append(
                f"https://github.com/{repo}")  # 汇总展示
            with self._state_lock:
                self._clone_repos += 1
                self._clone_files += len(entries)
            self._record_clone(repo, _size_kb, time.time() - _t0,
                               len(entries), True, "", "",
                               self._dir_size_bytes(tmp))
            self._repos_partial_total += 1  # 08171：partial clone 仓库数
            self._wlog(f"📦 Partial Clone: {len(entries)} 个文件（零 API 配额）"
                  f" | 耗时 {time.time()-_t0:.0f}s"
                  f"{f' | 大仓库 {_size_kb/1024/1024:.1f}GB' if _size_kb > 1024*1024 else ''}")
            return entries
        except Exception as e:
            self._record_clone(repo, _size_kb, time.time() - _t0, 0, False,
                               "exception", str(e)[-800:],
                               self._dir_size_bytes(tmp))
            self._wlog(f"⚠️ Partial Clone 异常: {e}")
            return None
        finally:
            if acquired:
                self._clone_sem.release()
                self._clone_active -= 1  # 监控采样
            # 恢复调用前状态（"处理 xxx"），而非无条件"空闲"
            self._set_worker_state(_prev_state or "空闲")
            shutil.rmtree(tmp, ignore_errors=True)

    # ==================== 懒分支名解析 ====================

    def _resolve_branch(self, repo: str, current_branch: str) -> Optional[str]:
        """懒获取仓库真实分支名（带缓存）。

        种子仓库和递归仓库都是传入默认 main，树 API 失败时调用此方法。
        结果按 repo 缓存，避免同组织下连续多次 repo info 调用触发次级限流。

        Returns:
            真实分支名，或 None（API 失败/new_branch 不存在）
        """
        if current_branch != "main":
            return None

        # 缓存命中 → 不用调 API
        if repo in self._branch_cache:
            return self._branch_cache[repo]
        # 已知死仓库 → 不调 API
        if self._is_repo_dead(repo):
            return None
        # 并发保护：等其他线程先出结果
        rl = repo.lower()
        if rl in self._repo_checking:
            time.sleep(0.3)
            if repo in self._branch_cache:
                return self._branch_cache[repo]
            if self._is_repo_dead(repo):
                return None
        self._repo_checking.add(rl)
        try:
            repo_data = self.http.get_json(
                f"https://api.github.com/repos/{repo}",
                timeout=FILE_DOWNLOAD_TIMEOUT,
                operation_name=f"repo info ({repo})")
            if not repo_data:
                self._branch_cache[repo] = ""  # 缓存失败，避免重复调 API
                self._mark_repo_not_found(repo)
                return None
            branch = repo_data.get("default_branch", "main")
            self._branch_cache[repo] = branch
            return branch
        finally:
            self._repo_checking.discard(rl)

    # ==================== 仓库级文件变更查询 ====================

    def _get_recently_changed_file_set(self, repo: str, branch: str) -> Optional[set]:
        """获取仓库 24h 内变更过的文件路径集合。

        使用 GitHub Compare API：只需 3 次 API 调用，与 commit 数量无关。
        5000 个 commit 和 1 个 commit 的处理成本相同。

        策略：
          1. GET /repos/{repo}/git/refs/heads/{branch}  → 获取分支最新 commit SHA
          2. GET /repos/{repo}/commits?until={24h前}&per_page=1 → 找到 24h 前的基准 commit
          3. GET /repos/{repo}/compare/{old_sha}...{latest_sha} → 一次返回所有变更文件

        固定 3 次 API 调用，零遗漏。失败则返回 None（调用者降级为直接下载）。

        Args:
            repo: 仓库全名 (owner/name)
            branch: 分支名

        Returns:
            变更文件路径集合（set of str）
            None — API 失败，调用者应降级
            空 set — 仓库 24h 内无 commit，调用者应跳过所有文件
        """
        # ── Step 1: 获取分支最新 commit SHA ──
        branch_url = f"https://api.github.com/repos/{repo}/git/refs/heads/{branch}"
        branch_data = self.http.get_json(branch_url, timeout=COMMITS_API_TIMEOUT,
                                         operation_name="分支 HEAD")
        if not branch_data:
            return None  # 失败 → 降级为直接下载

        latest_sha = branch_data.get("object", {}).get("sha", "")
        if not latest_sha:
            return None

        # ── Step 2: 找到 24h 前的基准 commit ──
        # until 参数：获取指定时间之前的最后一个 commit
        until = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime(
            '%Y-%m-%dT%H:%M:%SZ')
        old_url = (f"https://api.github.com/repos/{repo}/commits"
                   f"?sha={branch}&until={until}&per_page=1")
        old_data = self.http.get_json(old_url, timeout=COMMITS_API_TIMEOUT,
                                      operation_name="24h 前基准 commit")
        if old_data is None:
            return None  # 失败 → 降级

        if not old_data:
            # 空列表 = 仓库在 24h 前没有 commit（新仓库或新分支）
            # 无法确定基准点，所有候选文件都视为"可能变更过"
            self._wlog(f"  仓库无 24h 前 commit（新仓库/新分支），"
                  f"不跳过任何文件")
            return None  # 降级：全量下载

        old_sha = old_data[0].get("sha", "")
        if not old_sha or old_sha == latest_sha:
            return set()  # 24h 内无新 commit → 空集

        # ── Step 3: Compare API（最关键的一步） ──
        # 一次 API 调用返回两个 commit 之间 ALL 文件的变更
        # 响应中 files[] 数组包含每个文件的 filename、status 等
        compare_url = (f"https://api.github.com/repos/{repo}/compare/"
                       f"{old_sha}...{latest_sha}")
        compare_data = self.http.get_json(compare_url, timeout=COMMITS_API_TIMEOUT,
                                          operation_name="compare API")
        if not compare_data:
            return None  # 失败 → 降级

        changed_files = set()
        for f in compare_data.get("files", []):
            filename = f.get("filename", "")
            if filename:
                changed_files.add(filename)

        self._wlog(f"  Compare API 返回 {len(changed_files)} 个变更文件 "
              f"(3 次 API 调用)")
        return changed_files

    # ==================== 文件处理 ====================

    # ---- 进程池调度（08111）：大文件优先，小文件补位 ----
    # 计数语义：submit 时 +1，future 完成（done 回调）时 -1。
    # 小文件仅在"池中无大文件占满空位"时进池；大文件无条件进池，
    # 因此小文件永远不会挡在大文件前面（最多让大文件等一个几秒的
    # 小文件周期，FIFO 队列）。卡死进程的任务计数保持挂着不释放，
    # 与进程池真实状态一致（此时小文件全部回落线程解析，行为同旧版）。
    def _pool_submit(self, fn, is_big: bool):
        """提交进程池并维护占用计数。"""
        with self._state_lock:
            if is_big:
                self._pool_big_running += 1
            else:
                self._pool_small_running += 1
        future = self._extract_pool.submit(fn)
        future.add_done_callback(lambda f, b=is_big: self._pool_done(b))
        return future

    def _pool_done(self, is_big: bool):
        with self._state_lock:
            if is_big:
                self._pool_big_running -= 1
            else:
                self._pool_small_running -= 1

    def _parsing_cur_max_mb(self) -> float:
        """当前解析中文件的最大大小(MB)——实时值（08111 替代历史峰值）。"""
        with self._state_lock:
            return max(self._parsing_sizes, default=0.0)

    # ---- 08113：全局连接速率节流 + 分布统计 ----

    def _raw_fail_summary(self) -> str:
        """近 60s raw 下载失败分类汇总（08131：监控块显示，替代独立打印）。

        分类：404 / 4xx / 5xx / 连接错误(connect) / 超时(timeout) /
        无数据(idle) / 超总时长(max_total) / error:异常名（具体异常，
        08132：可追溯）。无失败时返回空串（含尾随 "| " 便于拼接）。
        08132 修复：每次调用清理 60s 窗口——失败停止后旧条目滑出，
        不再恒定显示过期数据。
        """
        _now = time.time()
        self._raw_fail_times = deque(
            (t, r) for t, r in self._raw_fail_times if _now - t <= 60)
        cats = {}
        for _t, _r in self._raw_fail_times:
            if _r.startswith("HTTP 404"):
                k = "404"
            elif _r.startswith("HTTP 4"):
                k = "4xx"
            elif _r.startswith("HTTP 5"):
                k = "5xx"
            elif _r == "connect":
                k = "连接错误"
            elif _r == "timeout":
                k = "超时"
            elif _r == "idle":
                k = "无数据"
            elif _r == "max_total":
                k = "超总时长"
            elif _r.startswith("error:"):
                k = _r[6:]  # 具体异常名（如 SSLError）
            else:
                k = "其他"
            cats[k] = cats.get(k, 0) + 1
        if not cats:
            return ""
        _order = ["404", "4xx", "5xx", "连接错误", "超时", "无数据",
                  "超总时长", "SSLError", "ChunkedEncodingError",
                  "ConnectionError", "其他"]
        return ("失败60s: "
                + " ".join(f"{k}x{cats[k]}" for k in _order if k in cats)
                + " | ")

    def _dl_rate_wait(self):
        """全局 raw 下载连接速率节流：1 秒滑动窗口内最多 N 个新连接。

        N = MAX_RAW_DOWNLOAD_CONNECTS_PER_SEC（30/s，08121：限速线 36/s
        的 80% 余量）；限速信号触发降级（_download_throttled_until 窗口）
        时减半（15/s）。只限 raw 下载（clone 走 git 协议不受此限）。

        必须全局共享（锁 + 时间戳列表）——不能用"每线程 sleep 间隔"：
        并发完成时各线程独立 sleep 不节流（实测会失效）。
        """
        _limit = (MAX_RAW_DOWNLOAD_CONNECTS_PER_SEC // 2
                  if self._download_throttled_until > time.time()
                  else MAX_RAW_DOWNLOAD_CONNECTS_PER_SEC)
        while True:
            with self._dl_gap_lock:
                now = time.time()
                self._dl_gap_times = [
                    t for t in self._dl_gap_times if now - t < 1.0]
                if len(self._dl_gap_times) < _limit:
                    self._dl_gap_times.append(now)
                    return
            time.sleep(0.05)

    @staticmethod
    def _hist_bucket_count(n: int) -> str:
        """候选文件数分桶：<200 每 10 一段；<1000 每 100 一段；≥1000 每 1000 一段。"""
        if n < 200:
            lo = n // 10 * 10
            return f"{lo}-{lo + 9}"
        if n < 1000:
            lo = n // 100 * 100
            return f"{lo}-{lo + 99}"
        lo = n // 1000 * 1000
        return f"{lo}-{lo + 999}"

    @staticmethod
    def _hist_bucket_mb(mb: float) -> str:
        """仓库大小分桶（MB）：<200 每 10MB；<1000 每 100MB；≥1000 每 1000MB。"""
        n = int(mb)
        if n < 200:
            lo = n // 10 * 10
            return f"{lo}-{lo + 9}MB"
        if n < 1000:
            lo = n // 100 * 100
            return f"{lo}-{lo + 99}MB"
        lo = n // 1000 * 1000
        return f"{lo}-{lo + 999}MB"

    def _handle_one_file(self, repo: str, branch: str, file_path: str,
                         sha: str, has_nodes: List[bool], raw_depth: int,
                         stats: List[int] = None, tag: str = "[种子仓库]",
                         size: int = 0,
                         content_bytes_preloaded: bytes = None,
                         async_mode: bool = False):
        """处理单个文件：下载（或本地预载）→ 提取节点 → 去重 → 入 buffer。

        使用 uri_parser 协议解析层提取 StandardProxy，
        按 (server, port, protocol) 全局去重后写入批次 buffer。

        content_bytes_preloaded（08141 全量 clone）：非 None 时跳过
        网络下载（文件已从本地工作区读出），直接走解析管线——零 API、
        不占 raw 连接配额。本地全量 clone 场景专用。

        async_mode（08174 异步下载管道）：True = 下载线程调用——解析
        提交共享线程池后立即返回（不阻塞下载线程）；False = 取样/全量
        clone 门禁——同步等待解析结果（需要 has_nodes 判断）。

        Args:
            stats: [extracted, added] 累加数组（本仓库级统计）。
                   extracted = 解析出的有效节点数（含与已有重复）
                   added     = server_port_protocol 去重后全局新增
        """
        if self.limiter.should_stop():
            raise RuntimeError("限流超限")

        raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{file_path}"

        if content_bytes_preloaded is not None:
            # 08141 全量 clone：本地文件直接解析（不走下载管道）
            content_bytes = content_bytes_preloaded
            dl_fail_reason = ""
            _dl_s = 0.0
        else:
            # 下载文件（总时长限制：CDN 慢速限速 0.1MB/s 持续送数据不触发
            # read timeout，100MB 文件能慢速下载 1000s+（08102 W-3 卡 2600s），
            # MAX_DOWNLOAD_SECONDS 超时即放弃，下次重试）
            # 08111：全局并发信号量封顶（36 worker×8 线程无上限 → 259 并发
            # 触发 CDN 限流）；idle_max_s：0 字节窗口快速放弃（限流挂起 30s
            # 无数据即弃，不再死等 240s）。
            # 08112 修复：acquire(timeout=10) 兜底 + release 严格配对
            #（此前退避窗口 acquire 2 次只 release 1 次 → 许可泄漏全卡死）。
            # 08121：退避（并发减半）已删，改动态降级——限速信号（近 60s
            # 网络类失败 ≥ 阈值）→ 连接速率减半 60s（_dl_rate_wait 内生效）。
            # 待下载计数（08121：含等连接配额——在 _dl_rate_wait 之前 +1，
            # 监控"待下载"= 等配额 + 下载中）
            self._pending_downloads += 1
            self._pending_download_bytes += size
            self._dl_rate_wait()  # 全局连接速率节流（限速信号时自动减半）
            if not self._download_sem.acquire(timeout=10):
                # 信号量异常兜底：不阻塞 worker；已计待下载回退
                self._pending_downloads -= 1
                self._pending_download_bytes -= size
                self._wlog(f"⚠️ 下载并发信号量获取超时，放弃下载 {file_path}")
                return
            # 08103 监测：下载中计数 + 耗时记录
            self._downloading_active += 1
            _dl_t0 = time.time()
            try:
                content_bytes, dl_fail_reason = self.http.download_with_timeout(
                    raw_url, FILE_DOWNLOAD_TIMEOUT, MAX_DOWNLOAD_SECONDS,
                    f"下载 {file_path}", idle_max_s=DOWNLOAD_IDLE_TIMEOUT)
            finally:
                self._download_sem.release()  # 08112：与 acquire 严格配对
                self._downloading_active -= 1
                self._pending_downloads -= 1
                self._pending_download_bytes -= size
            _dl_s = time.time() - _dl_t0
        if content_bytes is None:
            # 08131：失败记录 (time, reason) 到 60s 窗口——分类计数（监控块
            # 显示）+ 降级信号共用。降级信号 = 网络类失败（timeout/idle/
            # max_total/5xx）近 60s ≥ DOWNLOAD_STALL_THRESHOLD → 连接速率
            # 减半 60s（_dl_rate_wait 内生效）。
            # 排除：404/4xx（文件不存在）、connect（连接池/本地连接问题，
            # 08131 的 20:12 爆发 59 次）、error:*（通用异常，08132 的
            # 30 个偶发 error 误触发降级）——都不是"速率过快"，不触发。
            # 触发不再单独打日志——监控块"下载"行显示降级状态（剩 Ns）。
            _now = time.time()
            self._raw_fail_times.append((_now, dl_fail_reason))
            # 08171：文件下载失败细分计数（收尾统计）
            if dl_fail_reason.startswith("404"):
                self._files_404_total += 1
            elif dl_fail_reason in ("timeout", "idle", "max_total"):
                self._files_timeout_total += 1
            while self._raw_fail_times and \
                    _now - self._raw_fail_times[0][0] > 60:
                self._raw_fail_times.popleft()
            _net_fails = sum(
                1 for _t, _r in self._raw_fail_times
                if _r in ("timeout", "idle", "max_total")
                or _r.startswith("HTTP 5"))
            if (self._download_throttled_until <= _now
                    and _net_fails >= DOWNLOAD_STALL_THRESHOLD):
                self._download_throttled_until = _now + DOWNLOAD_THROTTLE_SECONDS
            # 081XX：下载失败也算仓库文件终结（done 计数，防仓库完成事件
            # 永远不发导致 tmp 不删/仓库不统计）
            self._tracker_file_done(repo, False, 0, 0)
            return  # 下载失败 → 不标记（下次重试）

        # 读取内容（surrogate 字符兼容）
        content = None
        try:
            content = content_bytes.decode('utf-8', errors='replace')
        except Exception:
            try:
                content = content_bytes.decode('latin-1', errors='replace')
            except Exception:
                pass

        if content is None:
            return  # 解码失败 → 不标记（下次重试）

        # 清洗 surrogate 字符：urllib.parse.quote() 无法处理 \ud800-\udfff
        # Python 3 正则引擎不匹配 surrogate（非法 Unicode），改用 encode/decode
        content = content.encode('utf-8', errors='surrogatepass').decode('utf-8', errors='replace')

        content_size_mb = len(content) / 1024 / 1024
        # 累计解析文件统计（监控显示）
        self._total_parsed_files += 1
        self._total_parsed_mb += content_size_mb
        # 08133：近 60s 解析窗口（监控读取时清理，防旧数据残留）
        self._parsed_60s.append((time.time(), content_size_mb))
        if MAX_FILE_SIZE is not None and len(content) > MAX_FILE_SIZE:
            self._wlog(f"📄 {raw_url} ⚠️ 文件过大 "
                  f"({content_size_mb:.1f}MB)，跳过")
            return  # 跳过但可能是间歇性问题 → 不标记

        # 提取节点（使用新的协议解析层）
        # OOM 诊断：记录正在解析的文件数与最大大小（监控显示，
        # 08084 内存 100% 时需确认是否解析线程持有大文件）。
        self._parsing_active += 1
        with self._state_lock:
            # 08111：实时集合替代历史峰值——监控"最大"不再误导
            self._parsing_sizes.add(content_size_mb)

        # 08174：解析 + 后续处理统一走 _submit_parse_task（提交共享解析
        # 线程池/进程池；wait=True 同步等结果（取样门禁），wait=False
        # 回调处理不阻塞（下载线程异步管道）。看门狗在提交时开始计时。
        self._submit_parse_task(
            repo, branch, file_path, sha, has_nodes, raw_depth, stats, tag,
            content, raw_url, _dl_s, size,
            content_bytes_preloaded=content_bytes_preloaded,
            wait=not async_mode)

        # （解析 + 后续处理已移至 _submit_parse_task，08174）

    def _submit_parse_task(self, repo, branch, file_path, sha, has_nodes,
                           raw_depth, stats, tag, content, raw_url,
                           _dl_s, size, content_bytes_preloaded=None,
                           wait: bool = True):
        """解析 + 后续处理（过滤/去重/入buffer/递归发现/订阅嗅探）。

        08174 异步管道改造：
          - 小文件解析提交共享线程池（PARSE_THREAD_POOL_SIZE=32）——
            原"每文件临时开 1 线程"无并发上限（08181 峰值 87），固定池
            积压排队，杜绝线程爆炸。
          - 大文件（>1MB）仍走进程池（绕 GIL 用多核）。
          - wait=True（取样/全量 clone 门禁）：等待结果后处理；
            wait=False（下载线程）：add_done_callback 处理，不阻塞。
          - 看门狗：提交即登记（进入 CPU 解析开始计时），超
            PARSE_WATCHDOG_SECONDS 未完成 → 监控线程信号转储线程栈
            （只打印不取消——大文件解析几分钟正常）。
        """
        content_size_mb = len(content) / 1024 / 1024
        # 累计解析文件统计（监控显示）
        self._total_parsed_files += 1
        self._total_parsed_mb += content_size_mb
        # 08133：近 60s 解析窗口（监控读取时清理，防旧数据残留）
        self._parsed_60s.append((time.time(), content_size_mb))
        if MAX_FILE_SIZE is not None and len(content) > MAX_FILE_SIZE:
            self._wlog(f"📄 {raw_url} ⚠️ 文件过大 "
                  f"({content_size_mb:.1f}MB)，跳过")
            return  # 跳过但可能是间歇性问题 → 不标记

        # 提取节点（使用新的协议解析层）
        # OOM 诊断：记录正在解析的文件数与最大大小（监控显示，
        # 08084 内存 100% 时需确认是否解析线程持有大文件）。
        self._parsing_active += 1
        with self._state_lock:
            # 08111：实时集合替代历史峰值——监控"最大"不再误导
            self._parsing_sizes.add(content_size_mb)

        # 08174：内存预算按字节估算——旧版用 len(str) 字符数当字节（中文
        # 1 字符占 3 字节，预算 2048 实际对应 4-6GB 内存，08191 实测 11GB
        # 的推手之一）。UTF-8 最坏 3 字节/字符，×3 保守估算（宁多算不少算）。
        _content_bytes = len(content) * 3

        # 内存预算控制（08104：64-120 文件并发解析，content 占满 11GB）：
        # 解析前检查"解析中"的 content 总大小，超预算则等待
        # （不发起新解析，下载线程被占住 → 自然不再下载新文件，
        # 排队的是 URL 而非 content，不占内存）。
        # （08111：已删除"首次提示"日志——预算满的状态由监控的
        # "等待解析/预算已用"显示，此日志曾因计数 bug 刷屏 98 万次）
        # 等待计数（监控显示：等待解析的文件数与大小）
        self._parsing_waiting += 1
        self._parsing_waiting_bytes += _content_bytes
        while True:
            with self._state_lock:
                if (self._parsing_bytes + _content_bytes
                        <= DOWNLOAD_MEMORY_BUDGET_MB * 1024 * 1024):
                    self._parsing_bytes += _content_bytes
                    self._parsing_waiting -= 1
                    self._parsing_waiting_bytes -= _content_bytes
                    break
            # 08174：收尾中放弃等预算（异步路径下下载线程不被死等）
            if self._dl_stop.is_set() and not wait:
                self._parsing_active -= 1
                self._parsing_waiting -= 1
                self._parsing_waiting_bytes -= _content_bytes
                return
            self._budget_wait_count += 1
            time.sleep(0.5)

        # 08174：看门狗登记移到 extract 内部——进入 CPU 解析才开始计时
        # （排队等待不算卡死；08191 积压 3249 任务排队 >300s 误触发的修复，
        # 你早就要求"文件进入 CPU 后开始计时"）
        _wd_key = f"{repo}|{file_path}"

        def extract():
            with self._parse_watchdog_lock:
                self._parse_watchdog[_wd_key] = time.time()
            # 081XX：进程池看门狗回报——子进程写 self._parse_watchdog 是
            # fork 内存副本，主进程读不到（进程池路径盲区）。通过 Queue
            # 把开始时间报给主进程监控线程（fork 继承同一 pipe 连接）。
            if self._pool_wd_queue is not None:
                try:
                    self._pool_wd_queue.put(("start", _wd_key, time.time()))
                except Exception:
                    pass
            return extract_all_strategies(content)

        def _release_parse_state(_ex_t0):
            """解析任务收尾：释放计数/预算/看门狗/耗时记录（两种路径共用）。"""
            self._parsing_active -= 1
            with self._state_lock:
                self._parsing_bytes -= _content_bytes  # 释放预算
                self._parsing_sizes.discard(content_size_mb)  # 08111 实时最大
            with self._parse_watchdog_lock:
                self._parse_watchdog.pop(_wd_key, None)
            # 081XX：进程池 done 回报（主进程监控据此清除该 key 的计时；
            # 线程池路径的 pop 已在上面生效，此回报对进程池路径必要）
            if self._pool_wd_queue is not None:
                try:
                    self._pool_wd_queue.put(("done", _wd_key))
                except Exception:
                    pass
            # 08103 监测：记录每文件耗时（下载/解析/大小），定位慢在哪一步
            self._file_times.append((round(_dl_s, 1),
                                     round(time.time() - _ex_t0, 1),
                                     round(content_size_mb, 1)))
            self._file_times_total += 1

        def _postprocess(proxies, _ex_t0):
            """解析完成后的后续处理（原 _handle_one_file 尾段）。"""
            _release_parse_state(_ex_t0)

            # ---- 过滤 + 批次内去重 + 入 buffer（线程安全） ----
            # 内存优化（08103：24.8 万节点/h × 2 个全局 set = 内存大头）：
            # 运行时只做"批次内去重"（≤5000 条小 set），批次间重复靠收尾
            # 全量去重（_finalize 读 batches → 去重 → 写 no/）。
            raw_count = len(proxies)
            # 08171：文件级有节点/无节点计数（解析出口统一统计——取样/全量/
            # clone/process_repo 四路径共用 _handle_one_file）
            if raw_count > 0:
                self._files_with_nodes_total += 1
            else:
                self._files_no_nodes_total += 1
            valid_count = 0
            new_count = 0
            for proxy in proxies:
                if not proxy.is_valid():
                    continue
                valid_count += 1

                with self._state_lock:
                    node_uri = proxy.to_uri()
                    if node_uri in self._batch_dedup:
                        continue
                    self._batch_dedup.add(node_uri)
                    self.batch_buffer.append(node_uri)
                    new_count += 1
                    ch = threading.current_thread().name
                    self._channel_new_nodes[ch] = self._channel_new_nodes.get(ch, 0) + 1

            # 监控：累计/近60秒提取节点数（valid_count = 有效节点）
            if valid_count > 0:
                self._total_parsed_nodes += valid_count
                _nw = time.time()
                self._nodes_60s.append((_nw, valid_count))
                while self._nodes_60s and self._nodes_60s[0][0] < _nw - 60:
                    self._nodes_60s.popleft()

            with self._state_lock:
                if valid_count > 0:
                    self.all_links.append(raw_url)
                    has_nodes[0] = True
                # 08141：全量 clone（preloaded）本地文件无 SHA，跳过缓存写入
                # （空 sha 会污染 processed_file_shas/sha_cache）
                if content_bytes_preloaded is None:
                    self.processed_file_shas.add(sha)
                    self.sha_cache[sha] = datetime.now(timezone.utc)  # 持久化：下载成功后才标记
                if VERBOSE_LOG:
                    self._wlog(f"📄 SHA 缓存: {sha[:8]}... ({len(content)}B)")

            # 解析失败记录：有候选但全验证失败 → 可能是新格式/变体，值得复盘
            if LOG_FAILED_CANDIDATES and raw_count > 0 and valid_count == 0:
                strategies_hit = []
                for _name, _func in [
                    ("uri", extract_embedded_uris),
                    ("clash", extract_clash_yaml),
                    ("singbox", extract_singbox_json),
                    ("surge", extract_surge_format),
                ]:
                    try:
                        _r = _func(content)
                        if _r:
                            strategies_hit.append(f"{_name}({len(_r)})")
                    except Exception:
                        pass
                # 样本：取第一个候选的字符串表示（截断 300 字符）
                sample = str(proxies[0])[:300] if proxies else ""
                self.failed_candidates_buffer.append(
                    f"{raw_url}|{repo}|{raw_count}|"
                    f"{','.join(strategies_hit) if strategies_hit else '?'}|"
                    f"{sample}")

            if VERBOSE_LOG:
                if valid_count == 0:
                    self._wlog(f"📄 {raw_url} ❌ 未提取出节点 "
                          f"(原始 {raw_count} 个候选全部验证失败)")
                elif new_count > 0:
                    self._wlog(f"📄 {raw_url} ✅ 解析 {raw_count} 候选 → "
                          f"{valid_count} 个有效节点 → 去重后新增 {new_count} 个")
                else:
                    self._wlog(f"📄 {raw_url} ⚪ 解析 {raw_count} 候选 → "
                          f"{valid_count} 个有效节点，全部重复")

            # 累加本仓库级统计（提取出 / 新增）——08174：异步多线程并发
            # 改同一 stats，state_lock 保护（+= 非原子）
            if stats is not None:
                with self._state_lock:
                    stats[0] += valid_count
                    stats[1] += new_count

            # ---- 自动刷盘（08174：异步多线程可同时触发，锁保护） ----
            if len(self.batch_buffer) >= BATCH_FLUSH_SIZE:
                with self._batch_flush_lock:
                    self._flush_batch()

            # ---- 081XX 第 3 批：递归发现/订阅嗅探改"提取到记账器" ----
            # 原在此处直接入队递归仓库 + HTTP 拉取订阅（占解析回调资源；
            # 嗅探 HTTP 8-15s 阻塞解析线程）。现只提取链接（纯 regex，
            # 毫秒级）到仓库记账器 set，入队/拉取由 work 在仓库完成事件
            # 时执行（_handle_repo_result）——解析回调瘦身。
            if ENABLE_RAW_RECURSIVE and self._should_trace(tag) \
                    and self.recursive_count < MAX_RECURSIVE_REPOS:
                _rl, _rpl, _sul = self._extract_links(content)
                with self._trackers_lock:
                    _t = self._trackers.get(repo)
                    if _t is not None:
                        _t["raw_links"].update(_rl)
                        _t["repo_links"].update(_rpl)
                        _t["sub_urls"].update(_sul)
            # 081XX：记账器文件完成（done==total → 发仓库完成事件 → work
            # 处理黑名单/统计/删 tmp/递归入队/订阅嗅探）。同步路径（取样）
            # 的文件没注册过 tracker → no-op。
            self._tracker_file_done(repo, has_node=has_nodes[0],
                                    extracted=valid_count, added=new_count)

        _ex_t0 = time.time()
        try:
            if self._extract_pool is not None:
                # 081XX：解析全走进程池（大小文件无条件进池）——线程池受
                # GIL 限制永远 1 核，且 45MB 级大文件 re 匹配在 C 层持有
                # GIL 会饿死其他解析线程（08241 实测 300s 超时 + 2 核用
                # 不满的根因）。进程池每个子进程独立 GIL，2 核可打满，
                # 看门狗计时也准确（注册=真在 CPU 跑，无 GIL 排队误差）。
                # 排队降级保留（08174 语义）：进程池任务 > 阈值时新大文件
                # 改线程池兜底（线程池仅兜底角色）——池被占死时让文件
                # 有地方跑，按进 CPU 的顺序处理。
                if (content_size_mb > EXTRACT_PROCESS_MIN_MB
                        and (self._pool_big_running + self._pool_small_running
                             >= EXTRACT_PROCESSES + PROCESS_QUEUE_MAX)):
                    future = self._parse_pool.submit(extract)
                else:
                    future = self._pool_submit(
                        extract, content_size_mb > EXTRACT_PROCESS_MIN_MB)
            elif FILE_PROCESS_TIMEOUT is not None and FILE_PROCESS_TIMEOUT > 0:
                # 兜底（进程池不可用，如本地 Windows 无 fork）→ 共享
                # 线程池（08174：固定 PARSE_THREAD_POOL_SIZE=32 池）
                future = self._parse_pool.submit(extract)
            else:
                # 无池可用 → 直接执行（同线程）
                proxies = extract()
                _postprocess(proxies, _ex_t0)
                return
        except Exception:
            _release_parse_state(_ex_t0)
            return

        if wait:
            # 同步路径（取样/全量 clone 门禁）：等待结果再后续处理
            try:
                proxies = future.result(timeout=FILE_PROCESS_TIMEOUT)
            except FutureTimeoutError:
                self._wlog(f"⚠️ 文件处理超时，跳过 {raw_url}")
                _release_parse_state(_ex_t0)
                # 081XX：超时跳过也算仓库文件终结（done 计数）
                self._tracker_file_done(repo, False, 0, 0)
                return
            except Exception:
                _release_parse_state(_ex_t0)
                self._tracker_file_done(repo, False, 0, 0)
                return
            _postprocess(proxies, _ex_t0)
        else:
            # 异步路径（下载线程）：回调处理，不阻塞下载线程
            def _done(fut):
                try:
                    _proxies = fut.result(timeout=0)
                except Exception:
                    _proxies = []
                _postprocess(_proxies, _ex_t0)
            try:
                future.add_done_callback(_done)
            except Exception:
                _release_parse_state(_ex_t0)

    # ==================== 回退路径：Contents API ====================

    def process_file_tree(self, repo: str, path: str, branch: str,
                          has_nodes: List[bool], stats: List[int] = None,
                          tag: str = "[种子仓库]", _files_acc: list = None):
        """回退路径：使用 Contents API 逐层遍历目录。

        仅在递归树 API 失败时使用。对每个文件/目录单独发请求。

        ⚠️ 配额黑洞：每目录 1 次核心 API，大仓库（上万目录）能把
        4800/h 配额吃光——因此入口由 CONTENTS_API_FALLBACK_ENABLED
        （默认 False）控制，tree + clone 都失败时直接放弃该仓库，
        不再走到这里（08113 决策，见 docs/DESIGN.md 决策 20）。

        081XX 第 3 批：候选文件统一收集（_files_acc 递归传递），顶层
        调用（path==""）结束后入待下载队列（异步管道）——不再逐文件
        同步 _handle_one_file（worker 不阻塞在下载/解析上）。

        Args:
            stats: [extracted, added] 本仓库级统计累加。
        """
        if _files_acc is None:
            _files_acc = []
        contents_url = (f"https://api.github.com/repos/{repo}/contents/{path}"
                        if path
                        else f"https://api.github.com/repos/{repo}/contents")
        resp = self.http.get(contents_url, timeout=CONTENTS_API_TIMEOUT,
                             operation_name=f"Contents {path or '根'}")
        if not resp:
            return

        items = resp.json()
        for item in items:
            if self.limiter.should_stop():
                raise RuntimeError("限流超限")

            item_path = item["path"]
            item_type = item["type"]
            item_sha = item["sha"]

            if item_type == "dir":
                if item_sha in self.processed_dir_shas:
                    continue

                # 只有 commits_per_file 策略才查目录修改时间
                # Contents API 回退路径：检查目录最后修改时间
                # 24h 无变更的目录跳过，避免递归进入旧目录
                commit_url = (f"https://api.github.com/repos/{repo}/commits"
                              f"?path={item_path}&per_page=1")
                c_resp = self.http.get(commit_url, timeout=COMMITS_API_TIMEOUT,
                                       operation_name=f"commit 查询目录 {item_path}")
                if not c_resp:
                    self.processed_dir_shas.add(item_sha)
                    continue
                try:
                    commit_list = c_resp.json()
                    if commit_list:
                        time_str = commit_list[0]["commit"]["committer"]["date"]
                        dir_time = datetime.fromisoformat(
                            time_str.replace("Z", "+00:00"))
                    else:
                        dir_time = None
                except Exception:
                    dir_time = None

                self.processed_dir_shas.add(item_sha)
                if dir_time is None or \
                   datetime.now(timezone.utc) - dir_time >= timedelta(hours=24):
                    continue

                self.process_file_tree(repo, item_path, branch, has_nodes,
                                       _files_acc=_files_acc)

            elif item_type == "file":
                ext = os.path.splitext(item_path)[1].lower()
                if ext not in ALLOWED_EXTENSIONS:
                    continue
                if item_sha in self.processed_file_shas:
                    continue
                if self._sha_in_cache(item_sha):
                    continue

                # 文件时间检查（Contents API 回退路径）
                commit_url = (f"https://api.github.com/repos/{repo}/commits"
                              f"?path={item_path}&per_page=1")
                c_resp = self.http.get(commit_url, timeout=COMMITS_API_TIMEOUT,
                                       operation_name=f"commit 查询文件 {item_path}")
                if not c_resp:
                    self.processed_file_shas.add(item_sha)
                    continue
                try:
                    commit_list = c_resp.json()
                    if commit_list:
                        time_str = commit_list[0]["commit"]["committer"]["date"]
                        file_time = datetime.fromisoformat(
                            time_str.replace("Z", "+00:00"))
                    else:
                        file_time = None
                except Exception:
                    file_time = None
                if file_time is None or \
                   datetime.now(timezone.utc) - file_time >= timedelta(hours=24):
                    self.processed_file_shas.add(item_sha)
                    continue

                # 081XX：收集候选（顶层递归结束后统一入队）
                _files_acc.append((item_path, item_sha, 0))

        # 081XX：顶层调用（path=="" 即根目录）递归结束后统一入队
        if path == "" and _files_acc:
            self._enqueue_downloads(repo, branch, _files_acc, has_nodes,
                                    raw_depth=0, stats=stats, tag=tag)

    # ==================== 最终输出 ====================

    def save_results(self):
        """保存输出文件：no_li.txt（源链接）。

        no/ 分片和 no_w_li.txt 已在 _flush_batch 中增量持久化，
        无需在此重建。
        """
        # 源链接（仅分片链接）
        self.all_links = list(dict.fromkeys(self.all_links))
        with open("no_li.txt", "w", encoding="utf-8", errors="replace") as f:
            f.write("\n".join(self.all_links))
        self._wlog(f"保存 no_li.txt ({len(self.all_links)} 条)")

    def _save_clone_stats(self, elapsed_seconds: float = 0):
        """写 clone_stats.json（CLONE_FIRST 实验数据，随结果提交）。

        内容：分桶聚合（仓库大小 × clone 耗时/成败）+ 失败明细 + 资源峰值。
        供跨轮对比（08083/08084...）确定 clone 并发/大仓库策略。
        """
        try:
            import json
            stats = self._clone_stats
            if not stats:
                return
            ok_stats = [s for s in stats if s[4]]
            times = sorted(s[2] for s in ok_stats)

            def _pct(p):
                if not times:
                    return 0
                return round(times[min(len(times) - 1, int(len(times) * p))], 1)

            # 仓库大小分桶（size_kb 单位是 KB：1GB = 1024*1024 KB）
            _G = 1024 * 1024
            _buckets = [
                ("0-100MB", lambda kb: kb <= 100 * 1024),
                ("100M-1GB", lambda kb: 100 * 1024 < kb <= _G),
                ("1-5GB", lambda kb: _G < kb <= 5 * _G),
                ("5-10GB", lambda kb: 5 * _G < kb <= 10 * _G),
                (">10GB", lambda kb: kb > 10 * _G),
            ]
            by_bucket = {}
            for name, cond in _buckets:
                bs = [s for s in stats if cond(s[1])]
                if not bs:
                    continue
                bs_ok = [s for s in bs if s[4]]
                bt = sorted(s[2] for s in bs_ok)
                by_bucket[name] = {
                    "count": len(bs), "ok": len(bs_ok),
                    "fail": len(bs) - len(bs_ok),
                    "avg_s": round(sum(bt) / len(bt), 1) if bt else 0,
                    "max_s": round(bt[-1], 1) if bt else 0,
                    "files_total": sum(s[3] for s in bs_ok),
                }

            data = {
                "run_id": datetime.now(timezone.utc).strftime("%Y%m%d-%H%M"),
                "mode": "clone_first" if CLONE_FIRST_MODE else "tree+commits",
                "clone_concurrency": PARTIAL_CLONE_CONCURRENCY,
                "duration_s": int(elapsed_seconds),
                "api_used": self.quota_mgr.total_calls,
                "api_remaining": self.quota_mgr.remaining(),
                "nodes_added": sum(s.get("nodes_new", 0)
                                   for s in self._channel_stats.values()),
                "resources": {
                    "cpu_load_max": round(self._cpu_load_peak, 2),
                    "disk_free_min_gb": round(self._disk_free_min, 1),
                    "clone_concurrency_peak": self._clone_active_peak,
                },
                "clone": {
                    "total": len(stats), "ok": len(ok_stats),
                    "fail": len(stats) - len(ok_stats),
                    "files_total": sum(s[3] for s in ok_stats),
                    "time_s": {"avg": round(sum(times) / len(times), 1)
                               if times else 0,
                               "max": round(times[-1], 1) if times else 0,
                               "p90": _pct(0.9)},
                    "fail_breakdown": dict(self._clone_fail_breakdown),
                },
                "by_size_bucket": by_bucket,
                "failures": self._clone_fail_details,
            }
            with open("clone_stats.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=1)
            self._wlog(f"📊 clone_stats.json 已保存 "
                  f"(clone {len(stats)}: ok {len(ok_stats)}/"
                  f"fail {len(stats)-len(ok_stats)} | "
                  f"fail_breakdown {self._clone_fail_breakdown})")
        except Exception as e:
            self._wlog(f"⚠️ clone_stats 保存异常: {e}")
