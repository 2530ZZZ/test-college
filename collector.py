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
from urllib.parse import quote
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from typing import List, Set, Optional, Tuple, Dict

from config import (
    GITHUB_TOKEN, MAX_PAGES, PER_PAGE, REPO_SLEEP_SECONDS, PAGE_SLEEP_SECONDS,
    REPO_TIMEOUT_SECONDS, MAX_FILE_SIZE, FILE_PROCESS_TIMEOUT,
    ALLOWED_EXTENSIONS, BLACKLIST_FILE,
    SEARCH_TIMEOUT, FILE_DOWNLOAD_TIMEOUT,
    CONTENTS_API_TIMEOUT, COMMITS_API_TIMEOUT, TREE_API_TIMEOUT,
    USE_RECURSIVE_TREE, MAX_COMMITS_PER_REPO,
    MAX_RAW_DOWNLOADS_PER_REPO, SEED_REPOS_FILE,
    SHA_CACHE_FILE, SHA_CACHE_MAX_ENTRIES,
    ENABLE_RAW_RECURSIVE, MAX_RECURSIVE_REPOS, MAX_RECURSIVE_DEPTH,
    CHUNK_SIZE, DEDUP_STRATEGY, DEDUP_ENABLED, BATCH_DIR, BATCH_FLUSH_SIZE,
    SOURCE_STALE_DAYS, MAX_RUNTIME_SECONDS,
    GITHUB_SEARCH_ENABLED,
    WEB_SEARCH_ENABLED, WEB_SEARCH_ENGINES, WEB_MAX_PAGES, WEB_PER_PAGE,
    WEB_PAGE_SLEEP, WEB_DOWNLOAD_TIMEOUT, WEB_BLACKLIST_FILE, WEB_USER_AGENTS,
)
from http_client import HttpClient, RateLimiter
from parsers import extract_all_strategies
from utils import now_str, timeout_decorator


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

        # 默认 HTTP 客户端（单线程或主线程使用，各线程入口可覆盖）
        self.http = HttpClient(token=token)

        # ── 共享状态（线程安全保护） ──
        self._state_lock = threading.RLock()          # 保护下方所有 set/dict/list
        self.all_links: List[str] = []
        self.unique_nodes: Set[str] = set()            # 全局已收集节点 URI
        self.global_dedup_keys: Set[tuple] = set()     # (server, port, protocol) 去重
        self.seen_repos: Set[str] = set()
        self.blacklist_repos: Set[str] = set()
        self.checked_count: int = 0
        self.processed_dir_shas: Set[str] = set()
        self.processed_file_shas: Set[str] = set()
        self.sha_cache: Dict[str, datetime] = {}
        self._branch_cache: Dict[str, str] = {}        # repo → 真实分支名

        # 批次持久化（共享）
        self.batch_buffer: List[str] = []
        self.batch_id: int = 0
        self.batch_file_paths: List[str] = []
        self.on_batch_flush = on_batch_flush

        # 递归发现计数
        self.recursive_count = 0

        # ── 独立组件（每线程一份） ──
        self.limiter = RateLimiter()  # 仅 GitHub 线程使用
        self._max_runtime = MAX_RUNTIME_SECONDS or None
        self._start_time = 0.0

        # 分渠道统计（每线程一份，_finalize 时汇总）
        self._channel_stats = {}  # channel_name → dict

        # 加载持久化状态
        self.load_blacklist()
        self.load_sha_cache()

    # ==================== 持久化 IO ====================

    def load_blacklist(self):
        """加载仓库黑名单文件。"""
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("https://github.com/"):
                        self.blacklist_repos.add(line)
            print(f"[{now_str()}] 已加载黑名单: {len(self.blacklist_repos)} 个", flush=True)

    def load_sha_cache(self):
        """加载 SHA 缓存（pickle 格式）。"""
        if os.path.exists(SHA_CACHE_FILE):
            try:
                with open(SHA_CACHE_FILE, 'rb') as f:
                    self.sha_cache = pickle.load(f)
                print(f"[{now_str()}] 加载 SHA 缓存 {len(self.sha_cache)} 条", flush=True)
            except Exception as e:
                print(f"[{now_str()}] 加载 SHA 缓存失败: {e}", flush=True)
                self.sha_cache = {}

    def save_sha_cache(self):
        """保存 SHA 缓存，按数量上限清理。

        策略：
          1. 按时间戳排序，保留最近的 SHA_CACHE_MAX_ENTRIES 条
          2. 同时清理 30 天前的条目（安全兜底）
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        self.sha_cache = {sha: ts for sha, ts in self.sha_cache.items() if ts > cutoff}
        if len(self.sha_cache) > SHA_CACHE_MAX_ENTRIES:
            sorted_items = sorted(self.sha_cache.items(), key=lambda x: x[1], reverse=True)
            self.sha_cache = dict(sorted_items[:SHA_CACHE_MAX_ENTRIES])
        try:
            with open(SHA_CACHE_FILE, 'wb') as f:
                pickle.dump(self.sha_cache, f)
        except Exception as e:
            print(f"[{now_str()}] 保存 SHA 缓存失败: {e}", flush=True)

    def _is_file_updated(self, sha: str) -> bool:
        """检查文件 SHA 是否从未处理过。

        SHA 是 Git 内容哈希 — 相同 SHA 永远意味着相同内容。
        命中时将时间戳更新为当前时间（LRU），保证高频 SHA 不会被淘汰。

        Args:
            sha: Git blob SHA

        Returns:
            True 表示需要处理（新内容），False 表示已处理过可跳过
        """
        with self._state_lock:
            if sha in self.sha_cache:
                self.sha_cache[sha] = datetime.now(timezone.utc)  # LRU: 更新时间戳
                return False  # 已处理，跳过
            self.sha_cache[sha] = datetime.now(timezone.utc)
            return True  # 新内容，需要处理

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
        print(f"[{now_str()}] 📦 批次 {seq:04d} 已持久化: "
              f"{filepath} ({node_count} 个节点, "
              f"累计 {len(self.unique_nodes)} 个)", flush=True)

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
        """更新种子条目：有新节点则刷新时间戳。"""
        if key not in seeds:
            seeds[key] = {}
        if new_node_count > 0:
            seeds[key]["last_new_node"] = datetime.now(timezone.utc).isoformat()
            seeds[key]["total_new_nodes"] = (seeds[key].get("total_new_nodes", 0)
                                              + new_node_count)

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

    def _runtime_exceeded(self) -> bool:
        """检查是否超出最大运行时间。GA 6 小时超时前 30 分钟触发。

        线程安全：_start_time 只在 run() 中设置一次，此后只读，无需加锁。
        """
        if self._max_runtime is None:
            return False
        return time.time() - self._start_time > self._max_runtime

    def run(self):
        """主入口：并行启动三个搜集线程，所有来源同时运行，互不影响。

        架构：
          - GitHub 线程：搜索仓库 → Tree API → 下载 → 提取节点
          - Web 线程：搜索引擎 → 提取 URL → 下载 → 提取节点
          - TG 线程：Telegram 频道 → 抓取消息 → 提取节点

        每个线程持有独立 HttpClient，共享去重/缓存/批次状态。
        所有共享操作通过 _state_lock 保护。
        """
        print(f"[{now_str()}] 🚀 程序启动（并行三通道）", flush=True)
        self._start_time = time.time()

        # 加载种子文件
        repo_seeds = self._load_seed_file(SEED_REPOS_FILE)

        # 清空上次运行的批次文件
        batch_dir = os.path.join(os.getcwd(), BATCH_DIR)
        if os.path.exists(batch_dir):
            shutil.rmtree(batch_dir)
        os.makedirs(batch_dir, exist_ok=True)

        # ── 为每个线程创建独立 HttpClient ──
        gh_http = HttpClient(token=self.token, rate_limiter=self.limiter)
        web_http = HttpClient(token="", rate_limiter=None)  # 无令牌，不改 GitHub 配额

        # ── 启动并行线程 ──
        threads = []
        errors = {}

        # GitHub 线程（按开关）
        if GITHUB_SEARCH_ENABLED:
            t_gh = threading.Thread(
                target=self._run_github_thread,
                args=(gh_http, repo_seeds, errors),
                name="GitHub",
                daemon=True)
            threads.append(t_gh)

        # Web 线程（按开关）
        if WEB_SEARCH_ENABLED:
            t_web = threading.Thread(
                target=self._run_web_thread,
                args=(web_http, errors),
                name="Web",
                daemon=True)
            threads.append(t_web)

        print(f"[{now_str()}] 启动 {len(threads)} 个搜集线程", flush=True)
        for t in threads:
            t.start()

        # 等待所有线程完成
        for t in threads:
            t.join()

        # ── 淘汰无产出来源并写回种子文件 ──
        with self._state_lock:
            repo_seeds = self._prune_seeds(repo_seeds)
        self._save_seed_file(SEED_REPOS_FILE, "repos", repo_seeds)

        # 报告线程错误
        for name, err in errors.items():
            print(f"[{now_str()}] ⚠️ {name} 线程异常: {err}", flush=True)

        # ── 最终保存 ──
        self._finalize(elapsed_seconds=time.time() - self._start_time)

    # ── 线程入口（异常隔离 + 调用实际搜集逻辑） ──

    def _run_github_thread(self, http: HttpClient, repo_seeds: dict, errors: dict):
        """GitHub 线程入口。"""
        saved = self.http
        self.http = http
        t0 = time.time()
        stats = {"name": "GitHub", "repos_checked": 0, "files_downloaded": 0,
                 "nodes_before": len(self.unique_nodes)}
        try:
            self._collect_github(repo_seeds, stats)
        except Exception as e:
            errors["GitHub"] = str(e)
            import traceback; traceback.print_exc()
        finally:
            stats["elapsed"] = f"{time.time() - t0:.0f}s"
            stats["nodes_new"] = len(self.unique_nodes) - stats["nodes_before"]
            stats["api_calls"] = http.stats.get("total", 0)
            stats["api_report"] = http.get_stats_report()
            with self._state_lock:
                self._channel_stats["GitHub"] = stats
            self.http = saved

    def _run_web_thread(self, http: HttpClient, errors: dict):
        """Web 线程入口。"""
        saved = self.http
        self.http = http
        t0 = time.time()
        stats = {"name": "Web", "urls_checked": 0, "domains_blacklisted": 0,
                 "nodes_before": len(self.unique_nodes)}
        try:
            self._collect_web(stats)
        except Exception as e:
            errors["Web"] = str(e)
            import traceback; traceback.print_exc()
        finally:
            stats["elapsed"] = f"{time.time() - t0:.0f}s"
            stats["nodes_new"] = len(self.unique_nodes) - stats["nodes_before"]
            stats["api_calls"] = http.stats.get("total", 0)
            stats["api_report"] = http.get_stats_report()
            with self._state_lock:
                self._channel_stats["Web"] = stats
            self.http = saved


    # ── 搜集实现 ──

    def _collect_github(self, repo_seeds: dict, stats: dict = None):
        """GitHub 搜索收集。先处理种子仓库，再搜索关键词。"""
        if stats is None:
            stats = {}
        _repos_before = self.checked_count
        _files_before = len(self.processed_file_shas)

        # ── 处理种子仓库 ──
        seed_list = list(repo_seeds.keys())
        if seed_list:
            print(f"[{now_str()}] 种子仓库: {len(seed_list)} 个", flush=True)

        for repo in seed_list:
            if self.limiter.should_stop() or self._runtime_exceeded():
                break
            before = len(self.unique_nodes)
            try:
                repo_info = self.http.get_json(
                    f"https://api.github.com/repos/{repo}",
                    timeout=FILE_DOWNLOAD_TIMEOUT,
                    operation_name=f"repo info ({repo})")
                if not repo_info or repo_info.get('disabled', False):
                    continue
                branch = repo_info.get("default_branch", "main")
                self._branch_cache[repo] = branch
                self.process_repo(repo, branch=branch,
                                  size=repo_info.get("size", -1),
                                  disabled=False,
                                  pushed_at=repo_info.get("pushed_at", ""))
            except Exception as e:
                print(f"[{now_str()}] ⚠️ 种子仓库 {repo}: {e}", flush=True)
            new_nodes = len(self.unique_nodes) - before
            self._update_seed_entry(repo_seeds, repo, new_nodes)
            time.sleep(REPO_SLEEP_SECONDS)

        # 再搜索关键词
        print(f"[{now_str()}] 🔎 开始 GitHub 搜索 ({len(self.queries)} 个关键词)", flush=True)
        for idx, query in enumerate(self.queries, 1):
            if self.limiter.should_stop():
                print(f"[{now_str()}] ⚠️ 限流超限", flush=True)
                break
            if self._runtime_exceeded():
                print(f"[{now_str()}] ⏰ 接近 GA 超时，停止搜索", flush=True)
                break
            q_start = time.time()
            print(f"\n[{now_str()}] 🔎 搜索 {idx}/{len(self.queries)}: {query}", flush=True)
            try:
                self.search_query(query)
            except RuntimeError:
                print(f"[{now_str()}] ⚠️ 限流超限", flush=True)
                break
            print(f"[{now_str()}]   关键词耗时: {time.time() - q_start:.1f}s", flush=True)

        with self._state_lock:
            stats["repos_checked"] = self.checked_count - _repos_before
            stats["files_downloaded"] = len(self.processed_file_shas) - _files_before

    def _collect_web(self, stats: dict = None):
        """网页搜索收集。搜索关键词 → 下载结果 → 提取节点。
        域名黑名单在获取搜索结果后、下载前生效——和仓库黑名单一致。
        先获取列表，再检查黑名单跳过无效项，不浪费下载请求。"""
        from parsers import extract_all_strategies
        if stats is None:
            stats = {}
        print(f"[{now_str()}] 🌐 开始网页搜索 "
              f"(引擎={WEB_SEARCH_ENGINES}, 关键词={len(self.queries)})", flush=True)

        blacklist = set()
        if os.path.exists(WEB_BLACKLIST_FILE):
            with open(WEB_BLACKLIST_FILE, "r", encoding="utf-8") as f:
                blacklist = {line.strip() for line in f if line.strip()}

        engine_failures = {}  # engine → 连续失败次数，>=3 则跳过
        for engine in WEB_SEARCH_ENGINES:
            for query in self.queries[:5]:  # 每个引擎只用前 5 个关键词
                if self.limiter.should_stop() or self._runtime_exceeded():
                    return
                if engine_failures.get(engine, 0) >= 3:
                    print(f"[{now_str()}] ⚠️ [{engine}] 连续失败，切换到下一个引擎", flush=True)
                    break
                for page in range(1, WEB_MAX_PAGES + 1):
                    search_url = self._build_search_url(engine, query, page)
                    if not search_url:
                        continue
                    self.http.user_agent = random.choice(WEB_USER_AGENTS)
                    print(f"[{now_str()}]   搜索: [{engine}] {query[:40]} (第{page}页)", flush=True)
                    resp = self.http.get(search_url, timeout=WEB_DOWNLOAD_TIMEOUT,
                                         operation_name=f"web[{engine}]")
                    if not resp:
                        engine_failures[engine] = engine_failures.get(engine, 0) + 1
                        if page == 1:
                            break
                        else:
                            continue
                    engine_failures[engine] = 0  # 成功后重置
                    result_urls = self._parse_search_results(resp.text, engine)
                    for url in result_urls:
                        domain = url.split("/")[2] if "://" in url else url
                        if domain in blacklist:
                            continue
                        content_resp = self.http.get(url, timeout=WEB_DOWNLOAD_TIMEOUT,
                                                     operation_name=f"web dl: {url[:60]}")
                        if not content_resp:
                            continue
                        before = len(self.unique_nodes)
                        proxies = extract_all_strategies(content_resp.text)
                        for p in proxies:
                            if not p.is_valid():
                                continue
                            with self._state_lock:
                                dedup_key = p.dedup_key(DEDUP_STRATEGY)
                                if dedup_key in self.global_dedup_keys:
                                    continue
                                self.global_dedup_keys.add(dedup_key)
                                node_uri = p.to_uri()
                                self.unique_nodes.add(node_uri)
                                self.batch_buffer.append(node_uri)
                                if len(self.batch_buffer) >= BATCH_FLUSH_SIZE:
                                    self._flush_batch()
                        new_nodes = len(self.unique_nodes) - before
                        if new_nodes > 0:
                            print(f"[{now_str()}]     ✅ {url[:80]}: +{new_nodes} 个节点", flush=True)
                        else:
                            print(f"[{now_str()}]     ⚪ {url[:80]}: 无新节点", flush=True)
                            blacklist.add(domain)
                    time.sleep(WEB_PAGE_SLEEP)

        with open(WEB_BLACKLIST_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(blacklist))

    # ── 搜索辅助 ──

    @staticmethod
    def _build_search_url(engine: str, query: str, page: int) -> str:
        """构建搜索引擎查询 URL。

        各引擎翻页参数：
          DuckDuckGo: s=N  (N = 结果偏移，每页约 30 条)
          Bing:       first=N+1 (N = 偏移，1-based)
          Google:     start=(page-1)*10 (每页固定 10 条，Google 忽略自定义 num)
          Yandex:     p=N  (N = 页码，0-based)
        """
        from urllib.parse import quote
        q = quote(query)
        if engine == "duckduckgo":
            s = (page - 1) * WEB_PER_PAGE
            return f"https://html.duckduckgo.com/html/?q={q}&s={s}"
        elif engine == "bing":
            first = (page - 1) * WEB_PER_PAGE + 1
            return f"https://www.bing.com/search?q={q}&first={first}&count={WEB_PER_PAGE}"
        elif engine == "google":
            start = (page - 1) * 10
            return f"https://www.google.com/search?q={q}&start={start}&num={WEB_PER_PAGE}"
        elif engine == "yandex":
            return f"https://yandex.com/search?text={q}&p={page - 1}"
        print(f"[{now_str()}] ⚠️ 未知搜索引擎: {engine}", flush=True)
        return ""

    @staticmethod
    def _parse_search_results(html: str, engine: str) -> list:
        """从搜索引擎结果中提取 URL 列表。"""
        import re
        urls = set()
        # 通用方法：提取所有 http/https 链接
        url_pattern = re.compile(r'https?://[^\s"\'<>]+')
        for m in url_pattern.finditer(html):
            url = m.group(0).rstrip('.,;:!?)"\'')
            # 过滤搜索引擎自身的链接
            if any(skip in url for skip in
                   ['google.com', 'bing.com', 'duckduckgo.com',
                    'yandex.com', 'yandex.ru', '/search', 'microsoft.com']):
                continue
            if len(url) > 20 and '://' in url:
                urls.add(url)
        return list(urls)[:WEB_PER_PAGE]

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
            elif name == "Web":
                print(f"    检查URL: {st.get('urls_checked', 0)}, "
                      f"域名黑名单: {st.get('domains_blacklisted', 0)}, "
                      f"耗时: {elapsed}")
            elif name == "TG":
                ch_before = st.get('channels_before', 0)
                ch_after = st.get('channels_after', ch_before)
                ch_delta = ch_after - ch_before
                delta_str = f"+{ch_delta}" if ch_delta > 0 else str(ch_delta)
                print(f"    频道: {ch_before}→{ch_after} ({delta_str}), "
                      f"扫描: {st.get('channels_scanned', 0)}, "
                      f"消息: {st.get('messages_fetched', 0)}, "
                      f"耗时: {elapsed}")
            print(f"    新增节点: {st.get('nodes_new', 0)}, "
                  f"API 调用: {st.get('api_calls', 0)}")
            if st.get('api_report'):
                print(f"    API 详情:\n{st['api_report']}")

        # ── 汇总 ──
        total_new = sum(s.get("nodes_new", 0) for s in self._channel_stats.values())
        total_api = sum(s.get("api_calls", 0) for s in self._channel_stats.values())
        print(f"  ─────────────────────────")
        print(f"  节点总数: {len(self.unique_nodes)}, "
              f"批次: {len(self.batch_file_paths)}, "
              f"源链接: {len(self.all_links)}")
        print(f"  新增节点: {total_new}, 总API: {total_api}")
        print(f"  限流等待: {self.limiter.total_wait:.0f}s")
        print(f"{'='*60}", flush=True)

    def search_query(self, query: str):
        """搜索单个关键词，遍历结果页。

        Args:
            query: GitHub 搜索查询字符串
        """
        for page in range(1, MAX_PAGES + 1):
            # 每次翻页前检查限流和运行时间
            if self.limiter.should_stop() or self._runtime_exceeded():
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
                if repo in self.seen_repos:
                    print(f"[{now_str()}] ⏭️ 跳过已处理仓库 {github_url}", flush=True)
                    continue
                if github_url in self.blacklist_repos:
                    print(f"[{now_str()}] ⏭️ 跳过黑名单仓库 {github_url}", flush=True)
                    continue

                self.seen_repos.add(repo)
                self.checked_count += 1
                print(f"[{now_str()}] 开始处理仓库 {github_url}", flush=True)

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

                time.sleep(REPO_SLEEP_SECONDS)

            time.sleep(PAGE_SLEEP_SECONDS)

    def process_repo(self, repo: str, branch: str = "main",
                     size: int = -1, disabled: bool = False,
                     pushed_at: str = "", depth: int = 0):
        """处理单个仓库。

        使用搜索结果的字段代替 GET /repos/{repo} 调用，
        消除了一次不必要的 API 请求。

        Args:
            repo: 仓库全名 (owner/name)
            branch: 默认分支（从搜索结果获取）
            size: 仓库大小（从搜索结果获取）
            disabled: 是否已禁用（从搜索结果获取）
            pushed_at: 最后推送时间（从搜索结果获取）
            depth: 递归发现深度
        """
        github_url = f"https://github.com/{repo}"

        # 黑名单检查
        if github_url in self.blacklist_repos:
            print(f"[{now_str()}] 仓库在黑名单中: {github_url}", flush=True)
            return

        # 有效性检查（使用搜索结果数据，无需额外 API 调用）
        if size == 0:
            print(f"[{now_str()}] ⚠️ 仓库 {github_url} 大小为 0，跳过", flush=True)
            return
        if disabled:
            print(f"[{now_str()}] ⚠️ 仓库 {github_url} 已禁用，跳过", flush=True)
            return

        # 缓存中有已知分支名，直接用，跳过初始 404
        if branch == "main" and repo in self._branch_cache and self._branch_cache[repo]:
            branch = self._branch_cache[repo]

        print(f"[{now_str()}] 仓库 {github_url} (分支: {branch}, "
              f"size: {size}KB, pushed: {pushed_at})", flush=True)

        has_nodes_flag = [False]

        # 主要路径：递归树 API
        if USE_RECURSIVE_TREE:
            success = self._process_with_recursive_tree(
                repo, branch, has_nodes_flag, depth)
            if not success:
                # 树 API 404 可能是因为分支名不对（种子仓库进来默认是 main），
                # 懒查真实分支名，只消耗 1 次 API 调用，然后重试
                actual_branch = self._resolve_branch(repo, branch)
                if actual_branch and actual_branch != branch:
                    print(f"[{now_str()}]   分支名修正: {branch} → {actual_branch}", flush=True)
                    success = self._process_with_recursive_tree(
                        repo, actual_branch, has_nodes_flag, depth)

            if not success:
                print(f"[{now_str()}] 树 API 失败，回退到 Contents API", flush=True)
                try:
                    if REPO_TIMEOUT_SECONDS is not None and REPO_TIMEOUT_SECONDS > 0:
                        @timeout_decorator(REPO_TIMEOUT_SECONDS)
                        def _process():
                            self.process_file_tree(repo, "", branch, has_nodes_flag)
                        _process()
                    else:
                        self.process_file_tree(repo, "", branch, has_nodes_flag)
                except RuntimeError:
                    raise
        else:
            # 回退路径：Contents API 逐层遍历
            try:
                if REPO_TIMEOUT_SECONDS is not None and REPO_TIMEOUT_SECONDS > 0:
                    @timeout_decorator(REPO_TIMEOUT_SECONDS)
                    def _process():
                        self.process_file_tree(repo, "", branch, has_nodes_flag)
                    _process()
                else:
                    self.process_file_tree(repo, "", branch, has_nodes_flag)
            except RuntimeError:
                raise

        # 未提取到节点 → 加入黑名单
        if not has_nodes_flag[0] and github_url not in self.blacklist_repos:
            print(f"[{now_str()}] 仓库 {github_url} 未提取到节点，加入黑名单", flush=True)
            self.blacklist_repos.add(github_url)
            with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
                f.write(github_url + "\n")

    # ==================== 递归树 API 处理 ====================

    def _process_with_recursive_tree(self, repo: str, branch: str,
                                     has_nodes: List[bool],
                                     depth: int = 0) -> bool:
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
            if not self._is_file_updated(sha):
                skipped_by_cache += 1
                continue
            files_to_check.append((path, sha))

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
                files_to_check = [(p, s) for p, s in files_to_check if p in changed]
                print(f"[{now_str()}]   commits 过滤: {before} → {len(files_to_check)} "
                      f"(变更文件 {len(changed)} 个)", flush=True)
            else:
                # commits API 失败 → 降级为直接下载（宁可多下不可漏掉）
                print(f"[{now_str()}]   commits API 失败，降级为直接下载", flush=True)

        if not files_to_check:
            print(f"[{now_str()}] 仓库 https://github.com/{repo} "
                  f"候选文件经时间过滤后为空", flush=True)
            return True

        # 限制处理数量（安全闸，防止极端情况）
        if MAX_COMMITS_PER_REPO is not None and len(files_to_check) > MAX_COMMITS_PER_REPO:
            print(f"[{now_str()}] ⚠️ 候选文件过多 ({len(files_to_check)} 个)，"
                  f"仅处理前 {MAX_COMMITS_PER_REPO} 个", flush=True)
            files_to_check = files_to_check[:MAX_COMMITS_PER_REPO]

        print(f"[{now_str()}] 仓库 https://github.com/{repo} "
              f"候选文件 {len(files_to_check)} 个", flush=True)

        # ---- 处理文件 ----
        for file_path, sha in files_to_check:
            if self.limiter.should_stop():
                raise RuntimeError("限流超限")
            self._handle_one_file(repo, branch, file_path, sha, has_nodes, depth)

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

        repo_data = self.http.get_json(
            f"https://api.github.com/repos/{repo}",
            timeout=FILE_DOWNLOAD_TIMEOUT,
            operation_name=f"repo info ({repo})")
        if not repo_data:
            self._branch_cache[repo] = ""  # 缓存失败，避免重复调 API
            return None
        branch = repo_data.get("default_branch", "main")
        self._branch_cache[repo] = branch
        return branch

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
                         sha: str, has_nodes: List[bool], depth: int):
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
            self.processed_file_shas.add(sha)
            return

        # 读取内容
        content = None
        try:
            content = file_resp.text
        except UnicodeDecodeError:
            try:
                content = file_resp.content.decode('latin-1')
            except Exception:
                pass

        if content is None:
            self.processed_file_shas.add(sha)
            return

        content_size_mb = len(content) / 1024 / 1024
        if MAX_FILE_SIZE is not None and len(content) > MAX_FILE_SIZE:
            print(f"[{now_str()}] 📄 {raw_url} ⚠️ 文件过大 "
                  f"({content_size_mb:.1f}MB)，跳过", flush=True)
            self.processed_file_shas.add(sha)
            return

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
            self.processed_file_shas.add(sha)
            return
        except Exception:
            self.processed_file_shas.add(sha)
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

        with self._state_lock:
            if valid_count > 0:
                self.all_links.append(raw_url)
                has_nodes[0] = True
            self.processed_file_shas.add(sha)

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
        if ENABLE_RAW_RECURSIVE and depth < MAX_RECURSIVE_DEPTH \
                and self.recursive_count < MAX_RECURSIVE_REPOS:
            self._discover_recursive(raw_url, content, depth)

    def _discover_recursive(self, source_url: str, content: str, depth: int):
        """从下载文件中发现其他 GitHub raw 链接，递归处理。"""
        raw_pattern = re.compile(
            r'https://raw\.githubusercontent\.com/([^/]+/[^/]+)/([^/]+)/([^\s"\'`#]+)'
        )
        found = set()

        for match in raw_pattern.finditer(content):
            full_name = match.group(1)
            ref = match.group(2)
            path = match.group(3)
            ext = os.path.splitext(path)[1].lower()

            if ext not in ALLOWED_EXTENSIONS:
                continue
            if full_name in self.seen_repos or \
               f"https://github.com/{full_name}" in self.blacklist_repos:
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
            # 递归仓库也先调 repo_info 拿分支名，避免 main→404 模式触发次级限流
            repo_info = self.http.get_json(
                f"https://api.github.com/repos/{full_name}",
                timeout=FILE_DOWNLOAD_TIMEOUT,
                operation_name=f"repo info ({full_name})")
            if not repo_info or repo_info.get('disabled', False):
                continue
            branch = repo_info.get("default_branch", "main")
            self._branch_cache[full_name] = branch
            self.process_repo(full_name, branch=branch,
                              size=repo_info.get("size", -1),
                              disabled=False,
                              pushed_at=repo_info.get("pushed_at", ""),
                              depth=depth + 1)

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
                if not self._is_file_updated(item_sha):
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
                                      has_nodes, depth=0)

    # ==================== 最终输出 ====================

    def save_results(self):
        """保存所有输出文件。

        包括：no.txt（全量）、no/ 分片、no_li.txt（源链接）、
        no_w_li.txt（分片链接）、ljck.txt（黑名单）、SHA 缓存。
        """
        if self.unique_nodes:
            with open("no.txt", "w", encoding="utf-8", errors="replace") as f:
                text = "\n".join(self.unique_nodes).encode("utf-8", errors="replace").decode("utf-8")
                f.write(text)
            print(f"[{now_str()}] 保存 no.txt ({len(self.unique_nodes)} 条)", flush=True)

        # 分片文件（节点 URI 同样需要清除非法字符）
        no_dir = "no"
        if os.path.exists(no_dir):
            shutil.rmtree(no_dir)
        os.makedirs(no_dir, exist_ok=True)
        nodes_list = list(self.unique_nodes)
        file_count = 0
        no_w_links = []
        repo_name = os.getenv("GITHUB_REPOSITORY", "2530ZZZ/cooo")
        branch_name = os.getenv("GITHUB_REF_NAME", "main")
        for i in range(0, len(nodes_list), CHUNK_SIZE):
            chunk = nodes_list[i:i + CHUNK_SIZE]
            file_count += 1
            filename = f"{file_count}.txt"
            filepath = os.path.join(no_dir, filename)
            chunk_text = "\n".join(chunk).encode("utf-8", errors="replace").decode("utf-8")
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(chunk_text)
            no_w_links.append(
                f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/no/{filename}"
            )

        # no_w_li.txt 中的链接不含非法字符，直接写入
        with open("no_w_li.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(no_w_links))
        print(f"[{now_str()}] 保存 no_w_li.txt ({file_count} 分片)", flush=True)

        # 源链接
        self.all_links.append(
            f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/no.txt"
        )
        self.all_links = list(dict.fromkeys(self.all_links))
        with open("no_li.txt", "w", encoding="utf-8", errors="replace") as f:
            f.write("\n".join(self.all_links))
