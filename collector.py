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
"""

import os
import time
import json
import random
import shutil
import re
import pickle
import threading
from queue import Queue
from urllib.parse import quote
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from typing import List, Set, Optional, Tuple, Dict

from config import (
    GITHUB_TOKEN, MAX_PAGES, PER_PAGE, REPO_SLEEP_SECONDS, PAGE_SLEEP_SECONDS, SEARCH_FORK,
    REPO_TIMEOUT_SECONDS, MAX_FILE_SIZE, FILE_PROCESS_TIMEOUT,
    ALLOWED_EXTENSIONS, BLACKLIST_FILE, README_SPAM_KEYWORDS,
    SEARCH_TIMEOUT, FILE_DOWNLOAD_TIMEOUT,
    CONTENTS_API_TIMEOUT, COMMITS_API_TIMEOUT, TREE_API_TIMEOUT,
    USE_RECURSIVE_TREE, MAX_COMMITS_PER_REPO,
    MAX_RAW_DOWNLOADS_PER_REPO, SEED_REPOS_FILE,
    PARALLEL_DOWNLOAD_THRESHOLD, PARALLEL_DOWNLOAD_WORKERS,
    FORK_CHAIN_ENABLED, FORK_CHAIN_MAX_FORKS,
    MAX_PARENT_TRACE_DEPTH, FORK_CHAIN_CHILD_DEPTH,
    AUTO_SEED_ENABLED,
    TOPIC_SEARCH_ENABLED, TOPIC_QUERIES, REPO_MAX_AGE_HOURS,
    README_SEARCH_ENABLED, README_QUERIES, README_MAX_PAGES,
    CODE_SEARCH_ENABLED, CODE_QUERIES, CODE_MAX_PAGES,
    MAX_PAGES_ZH_MULTIPLIER,
    USER_REPOS_ENABLED, USER_REPOS_MAX_PER_USER,
    VERBOSE_LOG, SHA_CACHE_DIR, SHA_CACHE_MAX_BYTES, SHA_CACHE_MAX_ENTRIES,
    SHARED_POOL_WORKERS,
    MAIN_QUEUE_SIZE, DISCOVERY_QUEUE_SIZE, MAIN_QUEUE_HALT,
    ENABLE_RAW_RECURSIVE, MAX_RECURSIVE_REPOS, MAX_RECURSIVE_DEPTH,
    AUTO_SEED_SORT_WINDOW_HOURS,
    SEEN_REPOS_PERSIST_ENABLED, SEEN_REPOS_DIR, SEEN_REPOS_MAX_BYTES,
    SUB_URL_MAX_PER_FILE, SAFE_WRITE_ENABLED,
    CHUNK_SIZE, DEDUP_STRATEGY, DEDUP_ENABLED, BATCH_DIR, BATCH_FLUSH_SIZE,
    SOURCE_STALE_DAYS, MAX_RUNTIME_SECONDS,
    GITHUB_SEARCH_ENABLED, QUOTA_MAX_PER_HOUR,
    SKIP_PROCESSING_AGE_HOURS,
    QUEUE_PUT_TIMEOUT_SECONDS,
    BLACKLIST_EVICTION_ENABLED, BLACKLIST_EVICTION_RATIO,
    LOG_FAILED_CANDIDATES,
    PARALLEL_DOWNLOAD_MB_HIGH, PARALLEL_DOWNLOAD_MB_MED,
)
from http_client import HttpClient, RateLimiter
from parsers import (
    extract_all_strategies, extract_embedded_uris, extract_clash_yaml,
    extract_singbox_json, extract_surge_format,
)
from utils import now_str
from quota_manager import QuotaManager


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

        # 主 HTTP 客户端 + 线程局部存储（并行 fork/用户仓库用）
        self._main_http = HttpClient(token=token, quota_manager=self.quota_mgr)
        self._http_local = threading.local()

        # ── 共享状态（线程安全保护） ──
        self._state_lock = threading.RLock()          # 保护下方所有 set/dict/list
        self.all_links: List[str] = []
        self.unique_nodes: Set[str] = set()            # 全局已收集节点 URI
        self.global_dedup_keys: Set[tuple] = set()     # (server, port, protocol) 去重
        self.seen_repos: Set[str] = set()  # 存储小写，大小写不敏感
        self.blacklist_repos: Set[str] = set()
        self._blacklist_order: List[str] = []      # 保留加载顺序（LRU 排序用）
        self._blacklist_touched: Set[str] = set()  # 本次运行命中过的条目
        self._blacklist_loaded: Set[str] = set()   # 启动时从文件加载的条目（vs 本次新加）
        self.checked_count: int = 0
        self.processed_dir_shas: Set[str] = set()
        self.processed_file_shas: Set[str] = set()
        self.sha_cache: Dict[str, datetime] = {}
        self._branch_cache: Dict[str, str] = {}        # repo → 真实分支名
        self._repo_not_found: Set[str] = set()         # 404 仓库（本次运行）
        self._repo_forbidden: Set[str] = set()         # 403 访问拒绝（本次运行）
        self._repo_checking: Set[str] = set()          # 正在 API 检查中的仓库
        self._main_queue_was_full = False              # 主队列满状态追踪
        self._worker_idle_since: Dict[str, float] = {} # Worker 闲置起始时间
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

        # ── 独立组件（每线程一份） ──
        self.limiter = RateLimiter()  # 仅 GitHub 线程使用
        self._max_runtime = MAX_RUNTIME_SECONDS or None
        self._start_time = 0.0

        # 分渠道统计（每线程一份，_finalize 时汇总）
        self._channel_stats = {}  # channel_name → dict
        self._channel_new_nodes = {}  # channel_name → 本渠道新增计数（线程安全）

        # 加载持久化状态
        self.load_blacklist()
        self.load_sha_cache()
        self.load_seen_cache()

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
        print(f"[{now_str()}] 加载已处理仓库缓存 {len(self.seen_cache)} 条", flush=True)

    def save_seen_cache(self):
        """保存已处理仓库缓存到分片 pickle。"""
        if not SEEN_REPOS_PERSIST_ENABLED or not self.seen_cache:
            return
        os.makedirs(SEEN_REPOS_DIR, exist_ok=True)
        for old in os.listdir(SEEN_REPOS_DIR):
            if old.endswith('.pkl'):
                os.remove(os.path.join(SEEN_REPOS_DIR, old))
        items = sorted(self.seen_cache.items())
        chunk, seq = {}, 0
        for k, v in items:
            chunk[k] = v
            if len(chunk) * 120 >= SEEN_REPOS_MAX_BYTES:
                with open(os.path.join(SEEN_REPOS_DIR, f"seen_{seq:04d}.pkl"), 'wb') as f:
                    pickle.dump(chunk, f)
                chunk.clear(); seq += 1
        if chunk:
            with open(os.path.join(SEEN_REPOS_DIR, f"seen_{seq:04d}.pkl"), 'wb') as f:
                pickle.dump(chunk, f)

    def _check_seen_cache(self, repo: str, pushed_at: str) -> bool:
        """检查已处理仓库缓存。True=可跳过，False=需要处理。"""
        if not SEEN_REPOS_PERSIST_ENABLED or not pushed_at:
            return False
        r = repo.lower()
        if r in self.seen_cache and self.seen_cache[r] == pushed_at:
            self.seen_cache[r] = pushed_at  # touch LRU
            return True
        return False

    def _mark_seen_cache(self, repo: str, pushed_at: str):
        """标记仓库已处理。"""
        if SEEN_REPOS_PERSIST_ENABLED and pushed_at:
            self.seen_cache[repo.lower()] = pushed_at

    def _check_blacklist(self, github_url: str) -> bool:
        """检查 URL 是否在黑名单中，命中时标记为"热"条目。"""
        if github_url in self.blacklist_repos:
            self._blacklist_touched.add(github_url)
            return True
        return False

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

    def _log_main_queue(self):
        """主队列满/恢复时打印日志（磁滞：180 停 / 20 启）。"""
        mq = getattr(self, '_task_queue', None)
        if not mq:
            return
        sz = mq.qsize()
        cap = mq.maxsize
        if sz >= MAIN_QUEUE_HALT and not self._main_queue_was_full:
            self._main_queue_was_full = True
            print(f"[{now_str()}] ⏸️  主队列 {sz}/{cap} ≥ {MAIN_QUEUE_HALT}，搜索暂停等待", flush=True)
        elif sz < 20 and self._main_queue_was_full:
            self._main_queue_was_full = False
            print(f"[{now_str()}] ▶️ 主队列 {sz}/{cap} < 20，搜索恢复", flush=True)

    def _is_repo_dead(self, repo: str) -> bool:
        """检查仓库是否已知不可达（404/403，大小写不敏感）。"""
        r = repo.lower()
        return r in self._repo_not_found or r in self._repo_forbidden

    def _mark_repo_not_found(self, repo: str):
        """标记仓库为 404（不存在/已删除），本次运行内不再重试。"""
        self._repo_not_found.add(repo.lower())

    def _mark_repo_forbidden(self, repo: str):
        """标记仓库为 403 访问拒绝（私有/被封），本次运行内不再重试。"""
        self._repo_forbidden.add(repo.lower())

    def _check_blacklist(self, github_url: str) -> bool:
        """检查 URL 是否在黑名单中，命中时标记为"热"条目。"""
        if github_url in self.blacklist_repos:
            self._blacklist_touched.add(github_url)
            return True
        return False

    def load_blacklist(self):
        """加载仓库黑名单文件（保留顺序用于 LRU 淘汰）。"""
        self._blacklist_order.clear()
        self._blacklist_loaded.clear()
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("https://github.com/"):
                        self.blacklist_repos.add(line)
                        self._blacklist_order.append(line)
                        self._blacklist_loaded.add(line)
            print(f"[{now_str()}] 已加载黑名单: {len(self.blacklist_repos)} 个", flush=True)

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
            print(f"[{now_str()}] 加载 SHA 缓存失败: {e}", flush=True)
            self.sha_cache = {}
            return
        print(f"[{now_str()}] 加载 SHA 缓存 {total} 条 "
              f"({len(os.listdir(SHA_CACHE_DIR))} 分片)", flush=True)

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
            print(f"[{now_str()}] SHA 缓存已保存: {len(self.sha_cache)} 条, {total_files} 分片", flush=True)

    @staticmethod
    def _write_sha_chunk(seq: int, chunk: dict):
        """写入单个 SHA 缓存分片。"""
        path = os.path.join(SHA_CACHE_DIR, f"sha_{seq:04d}.pkl")
        try:
            with open(path, 'wb') as f:
                pickle.dump(chunk, f)
        except Exception as e:
            print(f"[{now_str()}] SHA 分片写入失败: {e}", flush=True)

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

    def _flush_batch(self):
        """将当前 buffer 刷盘为批次文件。

        线程安全：_state_lock 保护 buffer/id/paths。
        刷盘后调用 on_batch_flush 回调（如果设置），用于投喂测速编排器。
        多个线程可并发调用，同一时刻只有一个线程执行写盘。
        """
        with self._state_lock:
            if not self.batch_buffer:
                return
            self.batch_id += 1
            seq = self.batch_id
            nodes_to_write = list(self.batch_buffer)
            node_count = len(nodes_to_write)
            self.batch_buffer.clear()

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
        print(f"[{now_str()}] 📦 批次 {seq:04d} 已持久化: "
              f"{filepath} ({node_count} 个节点, 累计 {len(self.unique_nodes)} 个)"
              f" | 主队列 {mq_sz}/{MAIN_QUEUE_SIZE}, 发现队列 {dq_sz}/{DISCOVERY_QUEUE_SIZE}", flush=True)
        # 批次刷盘时顺带保存持久化状态，防止中途崩溃丢失
        self.save_sha_cache()
        self.save_seen_cache()

        # 同步写入 no/ 分片和 no_w_li.txt（边搜集边持久化）
        no_dir = os.path.join(os.getcwd(), "no")
        os.makedirs(no_dir, exist_ok=True)
        no_filename = f"{seq:03d}.txt"
        no_filepath = os.path.join(no_dir, no_filename)
        with open(no_filepath, "w", encoding="utf-8") as f:
            f.write(text)
        repo_name = os.getenv("GITHUB_REPOSITORY", "2530ZZZ/cooo")
        branch_name = os.getenv("GITHUB_REF_NAME", "main")
        no_link = f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/no/{no_filename}"
        with open("no_w_li.txt", "a", encoding="utf-8") as f:
            f.write(no_link + "\n")
        # 覆写 no_li.txt（源链接去重）
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
                print(f"[{now_str()}] ⚠️ 批次回调异常: {e}", flush=True)

    def _add_node(self, node_uri: str, proxy=None):
        """添加节点到当前批次 buffer。

        如果 buffer 满了则自动刷盘。去重检查在调用前已完成。

        Args:
            node_uri: 原始 URI 字符串
            proxy: StandardProxy 实例（可选，用于去重 key 生成）
        """
        with self._state_lock:
            self.unique_nodes.add(node_uri)
            self.batch_buffer.append(node_uri)
            need_flush = len(self.batch_buffer) >= BATCH_FLUSH_SIZE

        if need_flush:
            self._flush_batch()

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
                    "pruning_days": SOURCE_STALE_DAYS}
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"[{now_str()}] ⚠️ 保存 {filepath} 失败: {e}", flush=True)

    @staticmethod
    def _update_seed_entry(seeds: dict, key: str, new_node_count: int):
        """更新种子条目：有节点则标记并记录时间。"""
        if key not in seeds:
            seeds[key] = {}
        if new_node_count > 0:
            seeds[key]["last_new_node"] = datetime.now(timezone.utc).isoformat()
            seeds[key]["_had_nodes"] = True

    @staticmethod
    def _sort_seeds(seeds: dict):
        """种子仓库重排序：24h内更新+有节点→最前，其余→靠后。"""
        cutoff = (datetime.now(timezone.utc) -
                  timedelta(hours=AUTO_SEED_SORT_WINDOW_HOURS))
        def _key(item):
            _repo, meta = item
            _last = meta.get("last_new_node", "")
            try:
                _dt = datetime.fromisoformat(_last)
                _recent = _dt > cutoff
            except Exception:
                _recent = False
            _nodes = meta.get("_had_nodes", False)
            return (0 if (_recent and _nodes) else 1, _last)
        sorted_items = sorted(seeds.items(), key=_key)
        seeds.clear()
        for k, v in sorted_items:
            v.pop("_had_nodes", None)  # 临时标记，写盘前清理
            seeds[k] = v

    @staticmethod
    def _prune_seeds(seeds: dict) -> dict:
        """淘汰超过 SOURCE_STALE_DAYS 天无产出的种子。"""
        cutoff = datetime.now(timezone.utc) - timedelta(days=SOURCE_STALE_DAYS)
        pruned = {}
        for key, meta in seeds.items():
            last = meta.get("last_new_node", "")
            if not last:
                pruned[key] = meta  # 首次加入，保留
                continue
            try:
                if datetime.fromisoformat(last) > cutoff:
                    pruned[key] = meta
            except Exception:
                pruned[key] = meta  # 解析失败保留
        removed = len(seeds) - len(pruned)
        if removed:
            print(f"[{now_str()}] 淘汰 {removed} 个过期种子", flush=True)
        return pruned

    # ==================== 主流程 ====================

    def _should_stop(self) -> bool:
        """综合停止检查：限流超限/运行超时→立即停，配额耗尽→等待恢复。"""
        if self.limiter.should_stop():
            return True
        if self._runtime_exceeded():
            return True
        if self.quota_mgr.exceeded:
            return not self.quota_mgr.wait_for_reset(self._runtime_exceeded)
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
        print(f"[{now_str()}] 🚀 程序启动 | 配额上限 {QUOTA_MAX_PER_HOUR}/小时", flush=True)
        self._start_time = time.time()

        # 加载种子文件
        repo_seeds = self._load_seed_file(SEED_REPOS_FILE)
        self._initial_seed_keys = set(repo_seeds.keys())

        # 清空上次运行的文件
        batch_dir = os.path.join(os.getcwd(), BATCH_DIR)
        if os.path.exists(batch_dir): shutil.rmtree(batch_dir)
        os.makedirs(batch_dir, exist_ok=True)
        no_dir = os.path.join(os.getcwd(), "no")
        if os.path.exists(no_dir): shutil.rmtree(no_dir)
        os.makedirs(no_dir, exist_ok=True)
        # 清空链接文件（从零开始增量写入）
        for _fname in ("no_w_li.txt", "no_li.txt", "failed_candidates.txt"):
            with open(_fname, "w", encoding="utf-8") as _f:
                pass

        # ── 创建共用线程池（双队列：主队列搜索来源，发现队列衍生来源） ──
        main_queue = Queue(maxsize=MAIN_QUEUE_SIZE)
        disc_queue = Queue(maxsize=DISCOVERY_QUEUE_SIZE)
        self._task_queue = main_queue   # fork 链引用（兼容旧路径）
        self._disc_queue = disc_queue
        workers = [threading.Thread(target=self._pool_worker,
                                    args=(main_queue, disc_queue),
                                    name=f"W-{i}", daemon=True)
                   for i in range(SHARED_POOL_WORKERS)]
        for w in workers: w.start()

        # ── GitHub 线程（关键词搜索） ──
        errors = {}
        if GITHUB_SEARCH_ENABLED:
            gh_http = HttpClient(token=self.token, rate_limiter=self.limiter,
                                 quota_manager=self.quota_mgr,
                                 pool_connections=20, pool_maxsize=20)
            t_gh = threading.Thread(
                target=self._run_github_thread,
                args=(gh_http, repo_seeds, main_queue, errors),
                name="GitHub", daemon=True)
            t_gh.start()

        # ── Code Search（主线程串行，先占 seen_repos） ──
        if CODE_SEARCH_ENABLED:
            try:
                code_http = HttpClient(token=self.token, rate_limiter=self.limiter,
                                       quota_manager=self.quota_mgr)
                self.http = code_http  # 主线程专用，不污染 _main_http
                t0 = time.time()
                self._collect_code(main_queue)
                cn = self._channel_new_nodes.get("Code", 0)
                self._channel_stats["Code"] = {
                    "name": "Code", "nodes_new": cn,
                    "elapsed": f"{time.time()-t0:.0f}s",
                    "api_calls": self.quota_mgr.total_calls,
                    "files_found": getattr(self, '_code_files_found', 0),
                    "repos_processed": getattr(self, '_code_repos_processed', 0)}
            except Exception as e:
                errors["Code"] = str(e)
                import traceback; traceback.print_exc()

        # ── 等待 GitHub 线程完成（超时 10 分钟防止死锁） ──
        if GITHUB_SEARCH_ENABLED:
            t_gh.join(timeout=600)
            if t_gh.is_alive():
                print(f"[{now_str()}] ⚠️ GitHub 搜索线程超时未完成，跳过", flush=True)

        # ── 等待队列清空 ──
        print(f"[{now_str()}] ⏳ 等待队列清空 (主队列 {main_queue.qsize()}, "
              f"发现队列 {disc_queue.qsize()})...", flush=True)
        wait_logged = 0
        while main_queue.qsize() > 0 or disc_queue.qsize() > 0:
            if self.limiter.should_stop() or self._runtime_exceeded():
                print(f"[{now_str()}] ⚠️ 停止信号，放弃剩余任务", flush=True)
                break
            if self.quota_mgr.exceeded:
                print(f"[{now_str()}] ⏳ 配额耗尽，等待重置（主队列 {main_queue.qsize()}, "
                      f"发现队列 {disc_queue.qsize()}）...", flush=True)
                if not self.quota_mgr.wait_for_reset(self._runtime_exceeded):
                    print(f"[{now_str()}] ⚠️ 运行超时，放弃剩余任务", flush=True)
                    break
                print(f"[{now_str()}] 🔄 配额恢复，继续处理", flush=True)
                continue
            if time.time() - wait_logged > 120:
                print(f"[{now_str()}]   ... 主队列 {main_queue.qsize()}, "
                      f"发现队列 {disc_queue.qsize()}", flush=True)
                wait_logged = time.time()
            time.sleep(3)
        print(f"[{now_str()}] 队列处理完毕", flush=True)
        for _ in workers: main_queue.put(None)
        for _ in workers: disc_queue.put(None)
        for w in workers: w.join()
        self._task_queue = None

        # ── 淘汰无产出来源并写回种子文件 ──
        with self._state_lock:
            self._sort_seeds(repo_seeds)
        self._save_seed_file(SEED_REPOS_FILE, "repos", repo_seeds)

        # 报告线程错误
        for name, err in errors.items():
            print(f"[{now_str()}] ⚠️ {name} 线程异常: {err}", flush=True)

        # ── 最终保存 ──
        self._finalize(elapsed_seconds=time.time() - self._start_time)

    # ── 线程入口（异常隔离 + 调用实际搜集逻辑） ──

    def _run_github_thread(self, http: HttpClient, repo_seeds: dict,
                           task_queue: Queue, errors: dict):
        """GitHub 线程入口。"""
        self.http = http  # 线程专用，不污染 _main_http
        t0 = time.time()
        _repos_start = self.checked_count
        _files_start = len(self.processed_file_shas)
        try:
            self._collect_github(repo_seeds, task_queue)
        except Exception as e:
            errors["GitHub"] = str(e)
            import traceback; traceback.print_exc()
        finally:
            elapsed = f"{time.time() - t0:.0f}s"
            gh_new = self._channel_new_nodes.get("GitHub", 0)
            seed_new = self._channel_new_nodes.get("种子仓库", 0)
            stats = {"name": "GitHub",
                     "repos_checked": self.checked_count - _repos_start,
                     "files_downloaded": len(self.processed_file_shas) - _files_start,
                     "elapsed": elapsed, "nodes_new": gh_new,
                     "api_calls": self.quota_mgr.total_calls,
                     "api_report": self.quota_mgr.get_stats_report()}
            r = getattr(self, '_seed_repos', 0)
            f = getattr(self, '_seed_files', 0)
            e = getattr(self, '_seed_elapsed', elapsed)
            seed_st = {"name": "种子仓库", "nodes_new": seed_new,
                       "repos_checked": r, "files_downloaded": f,
                       "elapsed": e} if seed_new else {}
            with self._state_lock:
                self._channel_stats["GitHub"] = stats
                if seed_st:
                    self._channel_stats["种子仓库"] = seed_st


    # ── 搜集实现 ──

    def _collect_github(self, repo_seeds: dict, task_queue: Queue):
        """种子仓库 + 关键词搜索，向共用线程池提交任务。"""
        _repos_before = self.checked_count
        _files_before = len(self.processed_file_shas)
        _stage_start = time.time()
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

        # 阶段 1: 种子仓库进队列
        seed_list = list(repo_seeds.keys())
        if seed_list:
            print(f"[{now_str()}] 🔵 种子仓库: {len(seed_list)} 个 → 队列", flush=True)
            for repo in seed_list:
                if self._should_stop(): break
                try:
                    ri = self.http.get_json(
                        f"https://api.github.com/repos/{repo}",
                        timeout=FILE_DOWNLOAD_TIMEOUT,
                        operation_name=f"repo info ({repo})")
                    if not ri or ri.get('disabled'): continue
                    br = ri.get("default_branch", "main")
                    self._branch_cache[repo] = br
                    self._add_seen(repo)  # 种子仓库标记已处理（大小写不敏感）
                    task_queue.put(("种子仓库", repo,
                                    {"branch": br, "size": ri.get("size", -1),
                                     "disabled": False, "pushed_at": ri.get("pushed_at", ""),
                                     "seed_key": repo}),
                                   timeout=QUEUE_PUT_TIMEOUT_SECONDS)
                except Exception as e:
                    print(f"[{now_str()}] ⚠️ {repo}: {e}", flush=True)
                self._update_seed_entry(repo_seeds, repo, 0)
                time.sleep(REPO_SLEEP_SECONDS)
            self._seed_repos = self.checked_count - _repos_before
            self._seed_elapsed = f"{time.time() - _stage_start:.0f}s"

        # 阶段 2: 关键词搜索 → 结果进队列
        print(f"[{now_str()}] 🔵 关键词: {len(all_queries)} 个", flush=True)
        for idx, query in enumerate(all_queries, 1):
            nb = len(self.unique_nodes)
            qs = time.time()
            if self._should_stop(): break
            try:
                self._search_query_to_queue(query, task_queue)
            except RuntimeError:
                break
            print(f"[{now_str()}] ⏱️ [{idx}/{len(all_queries)}] "
                  f"{query[:60]} | {time.time()-qs:.0f}s | "
                  f"+{len(self.unique_nodes)-nb} | "
                  f"配额 {self.quota_mgr.remaining()}/{QUOTA_MAX_PER_HOUR}", flush=True)

        # 保存统计（队列由 run() 统一管理）
        self._seed_files = len(self.processed_file_shas) - _files_before
        with self._state_lock:
            self._channel_new_nodes["种子仓库"] = self._channel_new_nodes.get("种子仓库", 0)

        # 自动收录：只要产生节点就标记（不限新节点数）
        if AUTO_SEED_ENABLED:
            for rk, m in repo_seeds.items():
                if m.get("_had_nodes") and rk not in self._initial_seed_keys:
                    print(f"[{now_str()}] 🌱 自动收录: {rk}", flush=True)
            # 排序：24h内更新+有节点 → 最前
            self._sort_seeds(repo_seeds)
            self._save_seed_file(SEED_REPOS_FILE, "repos", repo_seeds)

    def _pool_worker(self, main_queue: Queue, disc_queue: Queue):
        """共用线程池 Worker：优先消费发现队列，再消费主队列。

        每个 Worker 持有独立 RateLimiter（独立管理限流等待），
        但通过 QuotaManager 和主 limiter 共享停止信号。
        """
        while True:
            # 优先取发现队列（fork 链/用户仓库/raw 递归，高产）
            item = None
            from_queue = None  # 记录任务来源队列
            try:
                item = disc_queue.get_nowait()
                from_queue = disc_queue
            except Exception:
                try:
                    item = main_queue.get(timeout=30)
                    from_queue = main_queue
                except Exception:
                    tn = threading.current_thread().name
                    last = self._worker_idle_since.get(tn, 0)
                    if time.time() - last > 120:
                        self._worker_idle_since[tn] = time.time()
                        print(f"[{now_str()}] ⏳ {tn} 等待任务中...", flush=True)
                    continue
            try:
                if item is None:
                    break
                # 停止信号：限流超限/运行超时 → 退出；配额耗尽 → 等待恢复
                if self.limiter.should_stop() or self._runtime_exceeded():
                    break
                if self.quota_mgr.exceeded:
                    if not self.quota_mgr.wait_for_reset(self._runtime_exceeded):
                        break  # 运行超时
                    continue  # 配额恢复，重新取任务
                source, repo, kwargs = item
                self.http = HttpClient(token=self.token, rate_limiter=None,
                                       quota_manager=self.quota_mgr)
                before = len(self.unique_nodes)
                self.process_repo(repo, **kwargs)
                new_nodes = len(self.unique_nodes) - before
                ch = self._channel_new_nodes
                ch[source] = ch.get(source, 0) + new_nodes
                # 种子仓库产出也更新对应种子条目
                seed_key = kwargs.get("seed_key")
                if seed_key and new_nodes > 0:
                    pass  # _update_seed_entry already called in _collect_github
            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"[{now_str()}] ⚠️ Worker 异常: {repo}: {e}", flush=True)
            finally:
                if from_queue is not None:
                    from_queue.task_done()

    def _search_query_to_queue(self, query: str, task_queue: Queue):
        """搜索单个关键词，结果直接放进线程池队列。"""
        has_cjk = bool(re.search(r'[一-鿿]', query))
        max_p = (MAX_PAGES * MAX_PAGES_ZH_MULTIPLIER) if has_cjk else MAX_PAGES

        for page in range(1, max_p + 1):
            if self._should_stop():
                return
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
            for item in items:
                repo = item.get("full_name")
                if not repo: continue
                github_url = f"https://github.com/{repo}"
                if not self._check_and_add_seen(repo) or self._check_blacklist(github_url):
                    continue
                self.checked_count += 1
                try:
                    task_queue.put(("GitHub", repo,
                                    {"branch": item.get("default_branch", "main"),
                                     "size": item.get("size", 0),
                                     "disabled": item.get("disabled", False),
                                     "pushed_at": item.get("pushed_at", "")}),
                                   timeout=60)
                    self._log_main_queue()
                except Exception:
                    print(f"[{now_str()}] ⚠️ 主队列超时 60s 无空间，搜索中断", flush=True)
                    return
            time.sleep(PAGE_SLEEP_SECONDS)

    def _collect_code(self, task_queue: Queue):
        """GitHub Code Search：直接搜索文件内容中的节点 URI。

        与 _collect_github 的区别：
          - API: /search/code（搜文件内容）vs /search/repositories（搜仓库名）
          - 粒度: 直接定位到文件，不需要扫描整个仓库
          - 策略: 主线程 raw 下载（零 API 配额）+ 发现仓库进共用池

        流程：
          1. 遍历 CODE_QUERIES，构建搜索词（加 24h 时间限定）
          2. 翻页调用 /search/code API
          3. 对每个结果：下载 raw 文件 → 提取节点
          4. 对产出了节点的仓库 → task_queue.put() 触发 fork/用户遍历
        """
        if not CODE_QUERIES:
            return

        time_sfx = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime(
            '%Y-%m-%dT%H:%M:%SZ')

        code_nodes_before = len(self.unique_nodes)
        code_files = 0
        repos_found = set()

        for idx, query in enumerate(CODE_QUERIES, 1):
            qs = time.time()
            nb = len(self.unique_nodes)
            full_query = f"{query} pushed:>{time_sfx}"

            for page in range(1, CODE_MAX_PAGES + 1):
                if self._should_stop():
                    break

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

                for item in items:
                    if self.limiter.should_stop():
                        break

                    repo_data = item.get("repository", {})
                    repo_name = repo_data.get("full_name", "")
                    file_path = item.get("path", "")
                    file_sha = item.get("sha", "")

                    if not repo_name or not file_path:
                        continue

                    code_files += 1  # 统计搜索命中数（在过滤之前）
                    repos_found.add(repo_name)

                    github_url = f"https://github.com/{repo_name}"
                    if self._check_blacklist(github_url):
                        continue

                    # SHA 缓存检查：避免重复下载相同内容
                    if file_sha and self._sha_in_cache(file_sha):
                        continue

                    ext = os.path.splitext(file_path)[1].lower()
                    if ext not in ALLOWED_EXTENSIONS:
                        continue

                    # 获取分支：优先从搜索结果 → 缓存 → "main"
                    default_branch = repo_data.get("default_branch", "")
                    if not default_branch:
                        default_branch = self._branch_cache.get(repo_name, "main")
                    else:
                        self._branch_cache[repo_name] = default_branch

                    # 直接 raw 下载并提取节点（不计 API 配额）
                    has_nodes_flag = [False]
                    try:
                        self._handle_one_file(
                            repo_name, default_branch, file_path,
                            file_sha, has_nodes_flag, raw_depth=0)
                    except RuntimeError:
                        return  # 限流超限
                    except Exception:
                        pass

                    if has_nodes_flag[0]:
                        # 产出节点的仓库 → 进共用线程池触发 fork/用户遍历
                        if self._check_and_add_seen(repo_name):
                            self.checked_count += 1
                            try:
                                task_queue.put(("Code", repo_name,
                                                {"branch": default_branch,
                                                 "size": repo_data.get("size", -1),
                                                 "disabled": False,
                                                 "pushed_at": repo_data.get("pushed_at", "")}),
                                               timeout=60)
                            except Exception:
                                return  # 主队列超时，中断 code search

                time.sleep(PAGE_SLEEP_SECONDS)

            print(f"[{now_str()}] ⏱️ Code [{idx}/{len(CODE_QUERIES)}] "
                  f"{query[:60]} | {time.time() - qs:.0f}s | "
                  f"+{len(self.unique_nodes) - nb} 节点 | "
                  f"配额 {self.quota_mgr.remaining()}/{QUOTA_MAX_PER_HOUR}", flush=True)

        # 记录统计
        with self._state_lock:
            self._channel_new_nodes["Code"] = len(self.unique_nodes) - code_nodes_before
        self._code_files_found = code_files
        self._code_repos_processed = len(repos_found)

    # ── 搜索辅助 ──

    def _finalize(self, elapsed_seconds: float = 0):
        """最终保存和统计。即使之前发生错误也能安全调用。"""
        if self._max_runtime and elapsed_seconds > self._max_runtime:
            print(f"[{now_str()}] ⚠️ 运行时间 {elapsed_seconds:.0f}s 超出上限 "
                  f"{self._max_runtime}s（已提前停止搜集）", flush=True)
        try:
            self._flush_batch()
        except Exception as e:
            print(f"[{now_str()}] ⚠️ buffer 刷盘异常: {e}", flush=True)
        try:
            self.save_results()
        except Exception as e:
            print(f"[{now_str()}] ⚠️ save_results 异常: {e}", flush=True)
        try:
            self.save_sha_cache()
        except Exception as e:
            print(f"[{now_str()}] ⚠️ SHA 缓存保存异常: {e}", flush=True)

        # ── 分渠道统计 ──
        print(f"\n{'='*60}")
        print(f"  搜集完成 — 总耗时 {elapsed_seconds:.0f}s")
        print(f"{'='*60}")
        for name, st in sorted(self._channel_stats.items()):
            print(f"  [{name}]")
            elapsed = st.get('elapsed', '?')
            if name == "GitHub":
                print(f"    检查仓库: {st.get('repos_checked', 0)}, "
                      f"下载文件: {st.get('files_downloaded', 0)}, "
                      f"耗时: {elapsed}")
            elif name == "种子仓库":
                print(f"    检查仓库: {st.get('repos_checked', 0)}, "
                      f"下载文件: {st.get('files_downloaded', 0)}, "
                      f"耗时: {elapsed}")
            elif name == "Code":
                print(f"    文件匹配: {st.get('files_found', 0)}, "
                      f"仓库处理: {st.get('repos_processed', 0)}, "
                      f"耗时: {elapsed}")
            print(f"    新增节点: {st.get('nodes_new', 0)}, "
                  f"API 调用: {st.get('api_calls', 0)}")
            if st.get('api_report'):
                print(f"    API 详情:\n{st['api_report']}")

        # ── 汇总 ──
        total_new = sum(s.get("nodes_new", 0) for s in self._channel_stats.values())
        qs = self.quota_mgr.get_stats()
        print(f"  ─────────────────────────")
        print(f"  节点总数: {len(self.unique_nodes)}, "
              f"批次: {len(self.batch_file_paths)}, "
              f"源链接: {len(self.all_links)}")
        print(f"  新增节点: {total_new}, 总API: {qs['total']}")
        print(f"  配额剩余: {qs['remaining']}/{QUOTA_MAX_PER_HOUR}"
              f"{' ⚠️已耗尽' if qs['exceeded'] else ''}")
        print(f"  主动限速: {qs['throttled']} 次, "
              f"失败请求: {qs['failed']}")
        fc = len(self.failed_candidates_buffer)
        if fc > 0:
            print(f"  解析失败文件: {fc} 个 → 详见 failed_candidates.txt")
        print(f"{'='*60}", flush=True)

        # ── 黑名单 LRU 排序 + 自动淘汰 ──
        self._save_blacklist_lru()

    def _save_blacklist_lru(self):
        """黑名单 LRU 重排 + 淘汰末尾 1/30 的冷门条目。

        热条目（本次命中过的）排最前，冷条目排最后。
        本次新加入的条目不被淘汰。
        每次运行淘汰末尾 1/30 的旧冷条目，约 30 次运行后自然消失。
        """
        if not BLACKLIST_EVICTION_ENABLED or not self._blacklist_order:
            return
        # 区分本次新加 vs 旧条目
        new_entries = [u for u in self._blacklist_order
                       if u not in self._blacklist_loaded]
        old_touched = [u for u in self._blacklist_order
                       if u in self._blacklist_touched and u in self._blacklist_loaded]
        old_untouched = [u for u in self._blacklist_order
                         if u not in self._blacklist_touched and u in self._blacklist_loaded]
        # 新条目视为热（排在前面），旧条目 touched → untouched
        current_order = new_entries + old_touched + old_untouched
        # 仅淘汰旧冷条目：保留前 29/30
        evictable = len(old_untouched)
        ratio = max(5, BLACKLIST_EVICTION_RATIO)
        evict = max(0, evictable // ratio) if evictable > ratio else 0
        if evict > 0:
            keep = len(current_order) - evict
        else:
            keep = len(current_order)
        current_order = current_order[:keep]
        # 覆写 ljck.txt
        with open(BLACKLIST_FILE, "w", encoding="utf-8") as f:
            for url in current_order:
                f.write(url + "\n")
        if evict > 0:
            print(f"[{now_str()}] 黑名单淘汰 {evict} 条（保留 {len(current_order)} 条）",
                  flush=True)

    def search_query(self, query: str):
        """搜索单个关键词，遍历结果页。

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
            print(f"[{now_str()}] 第{page}页 "
                  f"total_count={data.get('total_count', 0)}, "
                  f"items={len(items)}", flush=True)

            if not items:
                break

            for idx, item in enumerate(items, 1):
                if self.limiter.should_stop():
                    return

                repo = item.get("full_name")
                if not repo:
                    continue

                github_url = f"https://github.com/{repo}"
                print(f"[{now_str()}] 检查仓库 #{idx}: {github_url}", flush=True)

                # 去重检查
                if not self._check_and_add_seen(repo):
                    print(f"[{now_str()}] ⏭️ 跳过已处理仓库 {github_url}", flush=True)
                    continue
                if self._check_blacklist(github_url):
                    print(f"[{now_str()}] ⏭️ 跳过黑名单仓库 {github_url}", flush=True)
                    continue

                self.checked_count += 1
                print(f"[{now_str()}] 开始处理仓库 {github_url}", flush=True)

                before_repo = len(self.unique_nodes)
                try:
                    # 使用搜索结果的字段替代 Repo Info API
                    self.process_repo(
                        repo=repo,
                        branch=item.get("default_branch", "main"),
                        size=item.get("size", 0),
                        disabled=item.get("disabled", False),
                        pushed_at=item.get("pushed_at", ""),
                    )
                except RuntimeError:
                    print(f"[{now_str()}] ⚠️ 限流超限，停止处理仓库", flush=True)
                    return
                except Exception as e:
                    print(f"[{now_str()}] ⚠️ 处理仓库异常 {github_url}: {e}", flush=True)

                # 自动种子追踪：搜索发现的仓库也记录产出
                if AUTO_SEED_ENABLED:
                    new_nodes = len(self.unique_nodes) - before_repo
                    if new_nodes > 0:
                        self._update_seed_entry(repo_seeds, repo, new_nodes)

                time.sleep(REPO_SLEEP_SECONDS)

            time.sleep(PAGE_SLEEP_SECONDS)

    def process_repo(self, repo: str, branch: str = "main",
                     size: int = -1, disabled: bool = False,
                     pushed_at: str = "", raw_depth: int = 0,
                     seed_key: str = None):
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
        """
        github_url = f"https://github.com/{repo}"

        # 已处理仓库缓存检查（pushed_at 未变→跳过）
        if self._check_seen_cache(repo, pushed_at):
            print(f"[{now_str()}] ⏭️ 仓库 {github_url} 未更新，跳过", flush=True)
            return

        # 黑名单检查
        if self._check_blacklist(github_url):
            print(f"[{now_str()}] 仓库在黑名单中: {github_url}", flush=True)
            return

        # 有效性检查
        if size == 0:
            print(f"[{now_str()}] ⚠️ 仓库 {github_url} 大小为 0，跳过", flush=True)
            return
        if disabled:
            print(f"[{now_str()}] ⚠️ 仓库 {github_url} 已禁用，跳过", flush=True)
            return

        # 仓库年龄过滤（统一入口：搜索结果、fork链、用户仓库、raw递归）
        if pushed_at:
            try:
                pushed_time = datetime.fromisoformat(
                    pushed_at.replace("Z", "+00:00"))
                age_hours = (datetime.now(timezone.utc) - pushed_time).total_seconds() / 3600

                # 废弃仓库 → 永久黑名单
                if REPO_MAX_AGE_HOURS > 0 and age_hours > REPO_MAX_AGE_HOURS:
                    print(f"[{now_str()}] ⚠️ 仓库 {github_url} "
                          f"{age_hours:.0f}h 未更新，废弃 → 加入黑名单", flush=True)
                    self.blacklist_repos.add(github_url)
                    self._blacklist_order.append(github_url)
                    with open(BLACKLIST_FILE, "a", encoding="utf-8") as bf:
                        bf.write(github_url + "\n")
                    return

                # 超过跳过阈值 → 不解析文件，但仍追踪 fork 链（fork 可能活跃）
                if SKIP_PROCESSING_AGE_HOURS > 0 and age_hours > SKIP_PROCESSING_AGE_HOURS:
                    print(f"[{now_str()}] ⏭️ 仓库 {github_url} "
                          f"{age_hours:.0f}h 未更新，跳过解析（追踪 fork 链）", flush=True)
                    # 追踪 fork/用户仓库，但不解析本仓库文件
                    if FORK_CHAIN_ENABLED and raw_depth < MAX_PARENT_TRACE_DEPTH:
                        if FORK_CHAIN_CHILD_DEPTH > 0:
                            self._trace_child_forks(repo, branch, raw_depth)
                        self._trace_fork_chain(repo, branch, pushed_at, raw_depth)
                    if USER_REPOS_ENABLED:
                        self._trace_user_repos(repo, branch, raw_depth)
                    return
            except Exception:
                pass  # 时间解析失败，放行

        # 缓存中有已知分支名，直接用，跳过初始 404
        if branch == "main" and repo in self._branch_cache and self._branch_cache[repo]:
            branch = self._branch_cache[repo]

        print(f"[{now_str()}] 仓库 {github_url} (分支: {branch}, "
              f"size: {size}KB, pushed: {pushed_at})", flush=True)

        has_nodes_flag = [False]

        # 主要路径：递归树 API
        if USE_RECURSIVE_TREE:
            success = self._process_with_recursive_tree(
                repo, branch, has_nodes_flag, raw_depth)
            if not success:
                # 树 API 404 可能是因为分支名不对（种子仓库进来默认是 main），
                # 懒查真实分支名，只消耗 1 次 API 调用，然后重试
                actual_branch = self._resolve_branch(repo, branch)
                if actual_branch and actual_branch != branch:
                    print(f"[{now_str()}]   分支名修正: {branch} → {actual_branch}", flush=True)
                    success = self._process_with_recursive_tree(
                        repo, actual_branch, has_nodes_flag, raw_depth)

            if not success:
                print(f"[{now_str()}] 树 API 失败，回退到 Contents API", flush=True)
                try:
                    if REPO_TIMEOUT_SECONDS is not None and REPO_TIMEOUT_SECONDS > 0:
                        with ThreadPoolExecutor(max_workers=1) as _exec2:
                            _future2 = _exec2.submit(
                                self.process_file_tree, repo, "", branch, has_nodes_flag)
                            _future2.result(timeout=REPO_TIMEOUT_SECONDS)
                    else:
                        self.process_file_tree(repo, "", branch, has_nodes_flag)
                except FutureTimeoutError:
                    print(f"[{now_str()}] ⚠️ Contents API 处理超时，跳过", flush=True)
                except RuntimeError:
                    raise
        else:
            # 回退路径：Contents API 逐层遍历
            # 用 ThreadPoolExecutor 超时替代 signal(SIGALRM)，后者在 Worker 线程中不可用
            try:
                if REPO_TIMEOUT_SECONDS is not None and REPO_TIMEOUT_SECONDS > 0:
                    with ThreadPoolExecutor(max_workers=1) as _exec:
                        _future = _exec.submit(
                            self.process_file_tree, repo, "", branch, has_nodes_flag)
                        _future.result(timeout=REPO_TIMEOUT_SECONDS)
                else:
                    self.process_file_tree(repo, "", branch, has_nodes_flag)
            except FutureTimeoutError:
                print(f"[{now_str()}] ⚠️ Contents API 处理超时，跳过", flush=True)
            except RuntimeError:
                raise

        # 未提取到节点 → 检查 README 广告（有广告才加黑名单）
        if not has_nodes_flag[0] and github_url not in self.blacklist_repos:
            # 配额耗尽或限流期间 → 不确定是否真的无节点 → 不拉黑
            if self.quota_mgr.exceeded or self.limiter.should_stop():
                print(f"[{now_str()}] ⚠️ 配额/限流期间跳过黑名单: {github_url}", flush=True)
            else:
                is_spam = self._check_readme_spam(repo, branch)
                if is_spam:
                    print(f"[{now_str()}] 仓库 {github_url} README 含广告词，加入黑名单", flush=True)
                else:
                    print(f"[{now_str()}] 仓库 {github_url} 未提取到节点，加入黑名单", flush=True)
                self.blacklist_repos.add(github_url)
                self._blacklist_order.append(github_url)
                with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
                    f.write(github_url + "\n")

        # 标记已处理（成功后写入缓存，下次 pushed_at 不变则跳过）
        self._mark_seen_cache(repo, pushed_at)

        # Fork 链追踪：有节点产出的仓库 → 查子仓库 + 回溯父仓库 + 查兄弟仓库
        if FORK_CHAIN_ENABLED and has_nodes_flag[0] and raw_depth < MAX_PARENT_TRACE_DEPTH:
            if FORK_CHAIN_CHILD_DEPTH > 0:
                self._trace_child_forks(repo, branch, raw_depth)
            self._trace_fork_chain(repo, branch, pushed_at, raw_depth)

        # 同用户仓库遍历：有节点产出 → 扫光该用户所有公开仓库
        if USER_REPOS_ENABLED and has_nodes_flag[0]:
            self._trace_user_repos(repo, branch, raw_depth)

    # ==================== Fork 链追踪 ====================

    def _trace_fork_chain(self, repo: str, branch: str,
                          pushed_at: str, raw_depth: int):
        """追溯 fork 仓库的父仓库，遍历其所有 fork 仓库。

        触发条件：当前仓库产出了节点（has_nodes=True）。
        流程：
          1. 查询当前仓库的 parent
          2. 获取父仓库的 fork 列表
          3. 逐个处理未在 seen_repos 中的 fork 仓库
        """
        print(f"[{now_str()}] 🔗 开始 Fork 链追踪 ({repo})", flush=True)

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
        print(f"[{now_str()}]   父仓库: {parent_name}", flush=True)

        # 2. 如果父仓库还没处理过，先处理父仓库
        if not self._is_seen(parent_name):
            before_parent = len(self.unique_nodes)
            try:
                self.process_repo(parent_name,
                                  branch=parent.get("default_branch", branch),
                                  size=parent.get("size", -1),
                                  disabled=False,
                                  pushed_at=parent.get("pushed_at", ""),
                                  raw_depth=raw_depth + 1)
            except Exception as e:
                print(f"[{now_str()}]   ⚠️ 父仓库 {parent_name}: {e}", flush=True)
            new_parent = len(self.unique_nodes) - before_parent
            print(f"[{now_str()}]   父仓库 +{new_parent} 个节点", flush=True)

        # 3. 遍历父仓库的 fork 列表
        qualified = []
        for page in range(1, 3):
            forks = self.http.get_json(
                f"https://api.github.com/repos/{parent_name}/forks"
                f"?sort=stargazers&per_page=30&page={page}",
                timeout=FILE_DOWNLOAD_TIMEOUT,
                operation_name=f"forks of {parent_name} (p{page})")
            if not forks or not isinstance(forks, list):
                break

            # 收集合格兄弟 fork
            for fork in forks:
                fn = fork.get("full_name")
                if not fn or not self._check_and_add_seen(fn): continue
                if len(qualified) >= FORK_CHAIN_MAX_FORKS: break
                qualified.append(fork)

        if qualified:
            self._run_fork_batch(qualified, branch, raw_depth, "🍴 兄弟仓库")

    def _process_fork_repo(self, fork: dict, branch: str, raw_depth: int) -> tuple:
        """处理单个 fork/用户仓库（串行降级路径）。"""
        self.http = HttpClient(token=self.token, rate_limiter=None,
                               quota_manager=self.quota_mgr)
        fork_name = fork.get("full_name")
        before = len(self.unique_nodes)
        try:
            self.process_repo(fork_name,
                              branch=fork.get("default_branch", branch),
                              size=fork.get("size", -1),
                              disabled=fork.get("disabled", False),
                              pushed_at=fork.get("pushed_at", ""),
                              raw_depth=raw_depth + 1)
        except Exception as e:
            print(f"[{now_str()}]   ⚠️ {fork_name}: {e}", flush=True)
        new_nodes = len(self.unique_nodes) - before
        return (fork_name, new_nodes)

    def _trace_child_forks(self, repo: str, branch: str, raw_depth: int):
        """遍历本仓库的直接 fork（子仓库），查其节点产出。"""
        print(f"[{now_str()}] 🔗 查子仓库: {repo}", flush=True)
        qualified = []
        for page in range(1, 3):
            forks = self.http.get_json(
                f"https://api.github.com/repos/{repo}/forks"
                f"?sort=stargazers&per_page=30&page={page}",
                timeout=FILE_DOWNLOAD_TIMEOUT,
                operation_name=f"forks of {repo} (p{page})")
            if not forks or not isinstance(forks, list):
                break

            # 收集合格 fork
            for fork in forks:
                fn = fork.get("full_name")
                if not fn or not self._check_and_add_seen(fn): continue
                if len(qualified) >= FORK_CHAIN_MAX_FORKS: break
                qualified.append(fork)

        if qualified:
            self._run_fork_batch(qualified, branch, raw_depth, "🍴 子仓库")

    def _run_fork_batch(self, forks: list, branch: str, raw_depth: int, label: str):
        """提交 fork/用户仓库到发现队列（优先消费，超时可丢弃）。"""
        tq = getattr(self, '_disc_queue', None)
        if not tq:  # 降级：无共用池时串行
            for fork in forks:
                fn, nn = self._process_fork_repo(fork, branch, raw_depth)
                if nn > 0: print(f"[{now_str()}]   {label}: {fn} +{nn}", flush=True)
                time.sleep(REPO_SLEEP_SECONDS)
            return

        # 有共用池 → 全部提交到发现队列
        for fork in forks:
            fn = fork.get("full_name")
            if not fn or not self._check_and_add_seen(fn): continue
            try:
                tq.put(("GitHub", fn,
                        {"branch": fork.get("default_branch", branch),
                         "size": fork.get("size", -1),
                         "disabled": fork.get("disabled", False),
                         "pushed_at": fork.get("pushed_at", "")}),
                       timeout=QUEUE_PUT_TIMEOUT_SECONDS)
            except Exception:
                print(f"[{now_str()}] 🗑️  发现队列满，丢弃 {fn}", flush=True)
        dq = getattr(self, '_disc_queue', None)
        dq_sz = dq.qsize() if dq else 0
        print(f"[{now_str()}]   {label}: {len(forks)} 个 → 发现队列 (队列: {dq_sz}/{DISCOVERY_QUEUE_SIZE})", flush=True)

    def _trace_user_repos(self, repo: str, branch: str, raw_depth: int):
        """遍历同用户名下的所有公开仓库，查是否有节点产出。

        触发条件：仓库产出了节点（不管是否重复）。
        通过 GET /users/{owner}/repos API 获取仓库列表，逐个检查。

        Args:
            repo: 仓库全名 (owner/name)
            branch: 分支名（用作新仓库的默认值）
            depth: 当前递归深度
        """
        owner = repo.split("/")[0]
        print(f"[{now_str()}] 👤 遍历用户仓库: {owner}", flush=True)

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
                if (not fn or self._is_seen(fn)
                        or self._check_blacklist(f"https://github.com/{fn}")):
                    continue
                if fn in found: continue
                found.add(fn)
                if USER_REPOS_MAX_PER_USER and len(qualified) >= USER_REPOS_MAX_PER_USER:
                    break
                if not self._check_and_add_seen(fn):
                    continue
                qualified.append(r)

        if qualified:
            self._run_fork_batch(qualified, branch, raw_depth, "👤 用户仓库")
        print(f"[{now_str()}]   用户 {owner} 共查 {len(qualified)} 个仓库", flush=True)

    def _check_readme_spam(self, repo: str, branch: str) -> bool:
        """下载仓库 README 并检查广告关键词。只在无节点时调用。

        Returns: True 表示 README 包含广告词（应加黑名单）。
        """
        if not README_SPAM_KEYWORDS:
            return False
        readme_url = f"https://raw.githubusercontent.com/{repo}/{branch}/README.md"
        resp = self.http.get(readme_url, timeout=FILE_DOWNLOAD_TIMEOUT,
                             operation_name=f"README ({repo})")
        if not resp:
            return False
        content = resp.text.lower()
        return any(kw.lower() in content for kw in README_SPAM_KEYWORDS)

    # ==================== 递归树 API 处理 ====================

    def _process_with_recursive_tree(self, repo: str, branch: str,
                                     has_nodes: List[bool],
                                     raw_depth: int = 0) -> bool:
        """使用 git/trees API 获取递归文件树。

        一次 API 调用获取全仓库文件列表，然后过滤、下载、提取。

        Returns:
            True 表示处理成功，False 表示需要回退到 Contents API
        """
        if self.limiter.should_stop():
            raise RuntimeError("限流超限")

        tree_url = f"https://api.github.com/repos/{repo}/git/trees/{branch}?recursive=1"
        resp = self.http.get(tree_url, timeout=TREE_API_TIMEOUT,
                             operation_name="递归树")
        if not resp:
            return False

        data = resp.json()
        if data.get('truncated', False):
            print(f"[{now_str()}] 树数据被截断，回退到 Contents API", flush=True)
            return False

        entries = data.get('tree', [])
        if not entries:
            return True

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
                continue
            files_to_check.append((path, sha, e.get('size', 0)))

        if not files_to_check:
            print(f"[{now_str()}] 仓库 https://github.com/{repo} 无候选文件 "
                  f"(总计 {len([e for e in entries if e.get('type')=='blob'])} blob"
                  f", 扩展名过滤 {skipped_by_ext}"
                  f", processed_shas 跳过 {skipped_by_processed}"
                  f", SHA 缓存跳过 {skipped_by_cache})", flush=True)
            # 如果有被 SHA 缓存或 processed_shas 跳过的文件，说明已处理过
            if skipped_by_cache > 0 or skipped_by_processed > 0:
                has_nodes[0] = True  # 避免已处理过的仓库被加入黑名单
            return True

        # ---- 文件时间检查（阈值策略） ----
        # 关键洞察：raw 下载免费不计 API 配额，但大量下载耗时巨大。
        # 少量候选文件 → 直接下载（零 API 成本）。
        # 大量候选文件 → 先通过 commits API 确定 24h 内变更的文件，再下载。
        if len(files_to_check) > MAX_RAW_DOWNLOADS_PER_REPO:
            print(f"[{now_str()}] 仓库 https://github.com/{repo} "
                  f"候选文件较多 ({len(files_to_check)} 个)，"
                  f"先通过 commits API 过滤", flush=True)
            changed = self._get_recently_changed_file_set(repo, branch)
            if changed is not None:
                before = len(files_to_check)
                files_to_check = [(p, s, sz) for p, s, sz in files_to_check if p in changed]
                print(f"[{now_str()}]   commits 过滤: {before} → {len(files_to_check)} "
                      f"(变更文件 {len(changed)} 个)", flush=True)
            else:
                # commits API 失败 → 降级为直接下载（宁可多下不可漏掉）
                print(f"[{now_str()}]   commits API 失败，降级为直接下载", flush=True)

        if not files_to_check:
            print(f"[{now_str()}] 仓库 https://github.com/{repo} "
                  f"候选文件经时间过滤后为空", flush=True)
            has_nodes[0] = True  # 24h 无新文件 ≠ 无节点，避免误加入黑名单
            return True

        # 限制处理数量（安全闸，防止极端情况）
        if MAX_COMMITS_PER_REPO is not None and len(files_to_check) > MAX_COMMITS_PER_REPO:
            print(f"[{now_str()}] ⚠️ 候选文件过多 ({len(files_to_check)} 个)，"
                  f"仅处理前 {MAX_COMMITS_PER_REPO} 个", flush=True)
            files_to_check = files_to_check[:MAX_COMMITS_PER_REPO]

        print(f"[{now_str()}] 仓库 https://github.com/{repo} "
              f"候选文件 {len(files_to_check)} 个", flush=True)

        # ---- 并行下载处理 ----
        # 小量文件串行（省线程开销），大量文件用线程池并发下载
        if len(files_to_check) <= PARALLEL_DOWNLOAD_THRESHOLD:
            for file_path, sha, _size in files_to_check:
                if self.limiter.should_stop():
                    raise RuntimeError("限流超限")
                self._handle_one_file(repo, branch, file_path, sha, has_nodes, raw_depth)
        else:
            # 按文件大小升序 → 小文件先跑完释放内存
            files_to_check.sort(key=lambda x: x[2])
            total_mb = sum(x[2] for x in files_to_check) / 1024 / 1024
            if PARALLEL_DOWNLOAD_MB_HIGH > 0 and total_mb > PARALLEL_DOWNLOAD_MB_HIGH:
                workers = 4
            elif PARALLEL_DOWNLOAD_MB_MED > 0 and total_mb > PARALLEL_DOWNLOAD_MB_MED:
                workers = 8
            else:
                workers = PARALLEL_DOWNLOAD_WORKERS
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(
                    self._handle_one_file, repo, branch, fp, s, has_nodes, raw_depth
                ): fp for fp, s, _sz in files_to_check}
                for future in as_completed(futures):
                    if self.limiter.should_stop():
                        executor.shutdown(wait=False, cancel_futures=True)
                        raise RuntimeError("限流超限")
                    try:
                        future.result()
                    except Exception as e:
                        print(f"[{now_str()}] ⚠️ 并行下载异常: "
                              f"{futures[future]}: {e}", flush=True)

        return True

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
            print(f"[{now_str()}]   仓库无 24h 前 commit（新仓库/新分支），"
                  f"不跳过任何文件", flush=True)
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

        print(f"[{now_str()}]   Compare API 返回 {len(changed_files)} 个变更文件 "
              f"(3 次 API 调用)", flush=True)
        return changed_files

    # ==================== 文件处理 ====================

    def _handle_one_file(self, repo: str, branch: str, file_path: str,
                         sha: str, has_nodes: List[bool], raw_depth: int):
        """处理单个文件：下载 → 提取节点 → 去重 → 入 buffer。

        使用 uri_parser 协议解析层提取 StandardProxy，
        按 (server, port, protocol) 全局去重后写入批次 buffer。
        """
        if self.limiter.should_stop():
            raise RuntimeError("限流超限")

        raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{file_path}"

        # 下载文件
        file_resp = self.http.get(raw_url, timeout=FILE_DOWNLOAD_TIMEOUT,
                                  operation_name=f"下载 {file_path}")
        if not file_resp:
            return  # 下载失败 → 不标记（下次重试）

        # 读取内容（surrogate 字符兼容）
        content = None
        try:
            content = file_resp.content.decode('utf-8', errors='replace')
        except Exception:
            try:
                content = file_resp.content.decode('latin-1', errors='replace')
            except Exception:
                pass

        if content is None:
            return  # 解码失败 → 不标记（下次重试）

        # 清洗 surrogate 字符：urllib.parse.quote() 无法处理 \ud800-\udfff
        # encode/decode 的 errors='replace' 在 Python 3.12 部分 surrogate 组合下不稳定
        content = re.sub(r'[\ud800-\udfff]', '�', content)

        content_size_mb = len(content) / 1024 / 1024
        if MAX_FILE_SIZE is not None and len(content) > MAX_FILE_SIZE:
            print(f"[{now_str()}] 📄 {raw_url} ⚠️ 文件过大 "
                  f"({content_size_mb:.1f}MB)，跳过", flush=True)
            return  # 跳过但可能是间歇性问题 → 不标记

        # 提取节点（使用新的协议解析层）
        def extract():
            return extract_all_strategies(content)

        try:
            if FILE_PROCESS_TIMEOUT is not None and FILE_PROCESS_TIMEOUT > 0:
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(extract)
                    proxies = future.result(timeout=FILE_PROCESS_TIMEOUT)
            else:
                proxies = extract()
        except FutureTimeoutError:
            print(f"[{now_str()}] ⚠️ 文件处理超时，跳过 {raw_url}", flush=True)
            return
        except Exception:
            return

        # ---- 过滤 + 去重 + 入 buffer（线程安全） ----
        raw_count = len(proxies)
        valid_count = 0
        new_count = 0
        for proxy in proxies:
            if not proxy.is_valid():
                continue
            valid_count += 1

            # 全局去重 + 写入 — 整个"检查-添加"操作必须原子
            with self._state_lock:
                if DEDUP_ENABLED:
                    dedup_key = proxy.dedup_key(DEDUP_STRATEGY)
                    if dedup_key in self.global_dedup_keys:
                        continue
                    self.global_dedup_keys.add(dedup_key)

                node_uri = proxy.to_uri()
                self.unique_nodes.add(node_uri)
                self.batch_buffer.append(node_uri)
                new_count += 1
                ch = threading.current_thread().name
                self._channel_new_nodes[ch] = self._channel_new_nodes.get(ch, 0) + 1

        with self._state_lock:
            if valid_count > 0:
                self.all_links.append(raw_url)
                has_nodes[0] = True
            self.processed_file_shas.add(sha)
            self.sha_cache[sha] = datetime.now(timezone.utc)  # 持久化：下载成功后才标记

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
                print(f"[{now_str()}] 📄 {raw_url} ❌ 未提取出节点 "
                      f"(原始 {raw_count} 个候选全部验证失败)", flush=True)
            elif new_count > 0:
                print(f"[{now_str()}] 📄 {raw_url} ✅ 解析 {raw_count} 候选 → "
                      f"{valid_count} 个有效节点 → 去重后新增 {new_count} 个", flush=True)
            else:
                print(f"[{now_str()}] 📄 {raw_url} ⚪ 解析 {raw_count} 候选 → "
                      f"{valid_count} 个有效节点，全部重复", flush=True)

        # ---- 自动刷盘 ----
        if len(self.batch_buffer) >= BATCH_FLUSH_SIZE:
            self._flush_batch()

        # ---- raw 链接递归发现 ----
        if ENABLE_RAW_RECURSIVE and raw_depth < MAX_RECURSIVE_DEPTH \
                and self.recursive_count < MAX_RECURSIVE_REPOS:
            self._discover_recursive(raw_url, content, raw_depth)

        # ---- 订阅链接自动发现（零 API 配额：raw HTTP，非 GitHub） ----
        _sub_urls = set()
        for _m in re.finditer(
            r'(?:https?://)'
            r'(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}'
            r'(?:/\S*)?\b(?:sub|subscribe|link|token|node|proxy|v2ray|clash'
            r'|ssr|vless|trojan|hysteria|tuic|singbox|shadowrocket'
            r'|quantumult|surge|loon|stash)\b[^\s"\']{0,200}',
            content, re.IGNORECASE):
            _url = _m.group(0).rstrip('.,;:!?)"\']')
            _black = ('google.com', 'star-history.com', 'play.google.com',
                       'apple.com', 'microsoft.com', 'facebook.com', 'twitter.com',
                       'youtube.com', 'reddit.com', 'wikipedia.org')
            if ('github.com' not in _url and 'raw.githubusercontent.com' not in _url
                    and not any(d in _url for d in _black)):
                _sub_urls.add(_url)
        for _url in list(_sub_urls):
            if _url in self._sub_urls_seen:
                continue
            self._sub_urls_seen.add(_url)
        _new_urls = [u for u in _sub_urls if u not in self._sub_urls_seen][:SUB_URL_MAX_PER_FILE]
        for _url in _new_urls:
            self._sub_urls_seen.add(_url)
            try:
                _resp = self.http.get(_url, timeout=(8, 15),
                                      operation_name=f"订阅链接 {_url[:60]}")
                if _resp and _resp.text:
                    _sub_proxies = extract_all_strategies(_resp.text)
                    for _p in _sub_proxies:
                        if _p.is_valid():
                            with self._state_lock:
                                if DEDUP_ENABLED:
                                    _dk = _p.dedup_key(DEDUP_STRATEGY)
                                    if _dk in self.global_dedup_keys:
                                        continue
                                    self.global_dedup_keys.add(_dk)
                                _uri = _p.to_uri()
                                self.unique_nodes.add(_uri)
                                self.batch_buffer.append(_uri)
                                self.all_links.append(_url)
                                has_nodes[0] = True
            except Exception:
                pass

    def _discover_recursive(self, source_url: str, content: str, raw_depth: int):
        """从下载文件中发现其他 GitHub 仓库链接和 raw 链接，递归处理。

        两种模式：
          1. raw 链接：https://raw.githubusercontent.com/user/repo/branch/file
          2. 仓库链接：https://github.com/user/repo（种子仓库的聚合资源）
        """
        # 模式 1：raw 文件链接
        raw_pattern = re.compile(
            r'https://raw\.githubusercontent\.com/([^/]+/[^/]+)/([^/]+)/([^\s"\'`#]+)'
        )
        # 模式 2：GitHub 仓库链接
        repo_pattern = re.compile(
            r'https://github\.com/([a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+)'
            r'(?!/blob/|/tree/|/raw/|/issues|/pull|/releases|/wiki)'
        )
        found = set()

        # ── 处理 raw 链接 ──
        for match in raw_pattern.finditer(content):
            full_name = match.group(1)
            ref = match.group(2)
            path = match.group(3)
            ext = os.path.splitext(path)[1].lower()

            if ext not in ALLOWED_EXTENSIONS:
                continue
            if self._is_seen(full_name) or \
               self._check_blacklist(f"https://github.com/{full_name}") or \
               self._is_repo_dead(full_name):
                continue
            if full_name in found:
                continue

            found.add(full_name)
            if self.recursive_count >= MAX_RECURSIVE_REPOS:
                break

            print(f"[{now_str()}] 🔗 递归发现仓库 {full_name} "
                  f"(来源 {source_url})", flush=True)
            self.recursive_count += 1
            time.sleep(REPO_SLEEP_SECONDS)
            # 并发保护：其他线程可能同时在查同一个 404 仓库
            rl = full_name.lower()
            if rl in self._repo_checking:
                time.sleep(0.3)
                if self._is_repo_dead(full_name):
                    continue
            self._repo_checking.add(rl)
            try:
                repo_info = self.http.get_json(
                    f"https://api.github.com/repos/{full_name}",
                    timeout=FILE_DOWNLOAD_TIMEOUT,
                    operation_name=f"repo info ({full_name})")
                if not repo_info or repo_info.get('disabled', False):
                    if not repo_info:
                        self._mark_repo_not_found(full_name)
                    continue
            finally:
                self._repo_checking.discard(rl)
            branch = repo_info.get("default_branch", "main")
            self._branch_cache[full_name] = branch
            self.process_repo(full_name, branch=branch,
                              size=repo_info.get("size", -1),
                              disabled=False,
                              pushed_at=repo_info.get("pushed_at", ""),
                              raw_depth=raw_depth + 1)

        # ── 处理仓库链接 ──
        for match in repo_pattern.finditer(content):
            full_name = match.group(1)
            if full_name in found or not self._check_and_add_seen(full_name):
                continue
            github_url = f"https://github.com/{full_name}"
            if self._check_blacklist(github_url) or self._is_repo_dead(full_name):
                continue
            found.add(full_name)
            if self.recursive_count >= MAX_RECURSIVE_REPOS:
                break

            print(f"[{now_str()}] 🔗 发现仓库链接 {full_name} "
                  f"(来源 {source_url})", flush=True)
            self.recursive_count += 1
            time.sleep(REPO_SLEEP_SECONDS)
            # 并发保护：其他线程可能同时在查同一个 404 仓库
            rl = full_name.lower()
            if rl in self._repo_checking:
                time.sleep(0.3)
                if self._is_repo_dead(full_name):
                    continue
            self._repo_checking.add(rl)
            try:
                repo_info = self.http.get_json(
                    f"https://api.github.com/repos/{full_name}",
                    timeout=FILE_DOWNLOAD_TIMEOUT,
                    operation_name=f"repo info ({full_name})")
                if not repo_info or repo_info.get('disabled', False):
                    if not repo_info:
                        self._mark_repo_not_found(full_name)
                    continue
            finally:
                self._repo_checking.discard(rl)
            branch = repo_info.get("default_branch", "main")
            self._branch_cache[full_name] = branch
            self.process_repo(full_name, branch=branch,
                              size=repo_info.get("size", -1),
                              disabled=False,
                              pushed_at=repo_info.get("pushed_at", ""),
                              raw_depth=raw_depth + 1)

    # ==================== 回退路径：Contents API ====================

    def process_file_tree(self, repo: str, path: str, branch: str,
                          has_nodes: List[bool]):
        """回退路径：使用 Contents API 逐层遍历目录。

        仅在递归树 API 失败时使用。对每个文件/目录单独发请求。
        """
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

                self.process_file_tree(repo, item_path, branch, has_nodes)

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

                self._handle_one_file(repo, branch, item_path, item_sha,
                                      has_nodes, raw_depth=0)

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
        print(f"[{now_str()}] 保存 no_li.txt ({len(self.all_links)} 条)", flush=True)
