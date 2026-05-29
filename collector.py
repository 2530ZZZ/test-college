"""
GitHub 节点收集器 —— 使用 git/trees API 获取递归文件树，
通过 Commits API 串行获取文件修改时间，无 HEAD 请求。
包含 SHA 持久化缓存、raw 链接递归发现（串行）等功能。
日志增强：显示仓库链接、候选文件数、提取节点数、黑名单操作。
"""

import os
import time
import shutil
import re
import pickle
import requests
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from typing import List, Set, Optional, Tuple, Dict

import utils
from utils import safe_get, now_str, timeout_decorator, beijing_tz, check_rate_limit
from parsers import extract_raw_candidates

from config import (
    CHUNK_SIZE, MAX_PAGES, PER_PAGE, REPO_SLEEP_SECONDS, PAGE_SLEEP_SECONDS,
    REPO_TIMEOUT_SECONDS, MAX_FILE_SIZE, FILE_PROCESS_TIMEOUT,
    ALLOWED_EXTENSIONS, BLACKLIST_FILE,
    SEARCH_TIMEOUT, REPO_INFO_TIMEOUT, FILE_DOWNLOAD_TIMEOUT,
    CONTENTS_API_TIMEOUT, COMMITS_API_TIMEOUT, TREE_API_TIMEOUT,
    USE_RECURSIVE_TREE, MAX_COMMITS_PER_REPO,
    CHECK_FILE_MODIFICATION_TIME, README_SPAM_KEYWORDS,
    ENABLE_RAW_RECURSIVE, MAX_RECURSIVE_REPOS, MAX_RECURSIVE_DEPTH,
    SHA_CACHE_FILE,
)


class Collector:
    def __init__(self, token: str = "", queries: List[str] = None):
        self.token = token
        self.headers = {
            "Authorization": f"token {token}" if token else "",
            "User-Agent": "Mozilla/5.0 (compatible; FreeNodesCollector/4.0)"
        }
        self.queries = queries or []
        self.all_links: List[str] = []
        self.unique_nodes: Set[str] = set()
        self.seen_repos: Set[str] = set()
        self.blacklist_repos: Set[str] = set()
        self.checked_count: int = 0
        self.processed_dir_shas: Set[str] = set()
        self.processed_file_shas: Set[str] = set()
        self.sha_cache: Dict[str, datetime] = {}
        self.recursive_count = 0
        self.load_blacklist()
        self.load_sha_cache()

    def load_blacklist(self):
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("https://github.com/"):
                        self.blacklist_repos.add(line)
            print(f"[{now_str()}] 已加载黑名单: {len(self.blacklist_repos)} 个", flush=True)

    def load_sha_cache(self):
        if os.path.exists(SHA_CACHE_FILE):
            try:
                with open(SHA_CACHE_FILE, 'rb') as f:
                    self.sha_cache = pickle.load(f)
                print(f"[{now_str()}] 加载 SHA 缓存 {len(self.sha_cache)} 条", flush=True)
            except Exception as e:
                print(f"[{now_str()}] 加载 SHA 缓存失败: {e}", flush=True)
                self.sha_cache = {}

    def save_sha_cache(self):
        now = datetime.now(timezone.utc)
        self.sha_cache = {sha: ts for sha, ts in self.sha_cache.items() if now - ts < timedelta(hours=24)}
        try:
            with open(SHA_CACHE_FILE, 'wb') as f:
                pickle.dump(self.sha_cache, f)
        except Exception as e:
            print(f"[{now_str()}] 保存 SHA 缓存失败: {e}", flush=True)

    def _is_file_updated(self, sha: str) -> bool:
        if sha in self.sha_cache:
            last_seen = self.sha_cache[sha]
            if datetime.now(timezone.utc) - last_seen < timedelta(hours=24):
                return False
        self.sha_cache[sha] = datetime.now(timezone.utc)
        return True

    def run(self):
        print(f"[{now_str()}] 🚀 程序启动", flush=True)
        start_time = time.time()
        for idx, query in enumerate(self.queries, 1):
            if utils.total_rate_limit_wait >= utils.MAX_TOTAL_RATE_LIMIT_WAIT:
                print(f"[{now_str()}] ⚠️ 累计限流等待已达 {utils.total_rate_limit_wait:.0f}s，终止搜索", flush=True)
                break
            q_start = time.time()
            print(f"\n[{now_str()}] 🔎 搜索 {idx}/{len(self.queries)}: {query}", flush=True)
            try:
                self.search_query(query)
            except RuntimeError:
                print(f"[{now_str()}] ⚠️ 限流超限，立即终止所有搜索", flush=True)
                break
            print(f"[{now_str()}]   关键词耗时: {time.time() - q_start:.1f}s", flush=True)
        self.save_results()
        elapsed = time.time() - start_time
        print(f"\n[{now_str()}] 🎉 收集完成，总耗时 {elapsed:.0f}s", flush=True)
        print(f"[{now_str()}] 检查仓库: {self.checked_count}, 源链接: {len(self.all_links)}, 节点: {len(self.unique_nodes)}", flush=True)
        print(f"[{now_str()}] 累计限流等待: {utils.total_rate_limit_wait:.0f} 秒", flush=True)

    def search_query(self, query: str):
        for page in range(1, MAX_PAGES + 1):
            url = (f"https://api.github.com/search/repositories"
                   f"?q={query}&sort=updated&order=desc"
                   f"&per_page={PER_PAGE}&page={page}")
            resp = safe_get(url, self.headers, timeout=SEARCH_TIMEOUT, operation_name=f"搜索第{page}页")
            if not resp:
                check_rate_limit()
                break
            data = resp.json()
            total_count = data.get("total_count", 0)
            items = data.get("items", [])
            print(f"[{now_str()}] 第{page}页 total_count={total_count}, items={len(items)}", flush=True)
            if not items:
                break
            for idx, item in enumerate(items, 1):
                try:
                    repo = item["full_name"]
                except KeyError:
                    print(f"[{now_str()}] ⚠️ 搜索结果缺少 full_name: {item}", flush=True)
                    continue
                github_url = f"https://github.com/{repo}"
                # 调试日志
                print(f"[{now_str()}] 检查仓库 #{idx}: {github_url}", flush=True)
                # 已在 search_query 中统一去重，不再在 process_repo 中重复检查 seen_repos
                if repo in self.seen_repos:
                    print(f"[{now_str()}] ⏭️ 跳过已处理仓库 {github_url}", flush=True)
                    continue
                if github_url in self.blacklist_repos:
                    print(f"[{now_str()}] ⏭️ 跳过黑名单仓库 {github_url}", flush=True)
                    continue
                # 标记为已处理
                self.seen_repos.add(repo)
                self.checked_count += 1
                print(f"[{now_str()}] 开始处理仓库 {github_url}", flush=True)
                try:
                    self.process_repo(repo)
                except RuntimeError:
                    print(f"[{now_str()}] ⚠️ 限流超限，停止处理仓库", flush=True)
                    return
                except Exception as e:
                    print(f"[{now_str()}] ⚠️ 处理仓库异常 {github_url}: {e}", flush=True)
                time.sleep(REPO_SLEEP_SECONDS)
            time.sleep(PAGE_SLEEP_SECONDS)

    def process_repo(self, repo: str, depth: int = 0):
        github_url = f"https://github.com/{repo}"
        print(f"[{now_str()}] 进入 process_repo: {github_url}", flush=True)
        # 只检查黑名单，不检查 seen_repos（search_query 已处理）
        if github_url in self.blacklist_repos:
            print(f"[{now_str()}] 仓库在黑名单中: {github_url}", flush=True)
            return

        repo_info = safe_get(f"https://api.github.com/repos/{repo}", self.headers, timeout=REPO_INFO_TIMEOUT)
        if not repo_info:
            print(f"[{now_str()}] ⚠️ 获取仓库信息失败 {github_url}", flush=True)
            return
        repo_data = repo_info.json()
        if repo_data.get('size', 0) == 0 or repo_data.get('disabled', False):
            print(f"[{now_str()}] ⚠️ 仓库 {github_url} 为空或已禁用，跳过", flush=True)
            return
        default_branch = repo_data.get("default_branch", "main")
        print(f"[{now_str()}] 仓库 {github_url} (分支: {default_branch})", flush=True)
        has_nodes_flag = [False]
        if USE_RECURSIVE_TREE:
            success = self._process_with_recursive_tree(repo, default_branch, has_nodes_flag, depth)
            if not success:
                print(f"[{now_str()}] 树 API 失败，回退到 Contents API 逐层过滤", flush=True)
                try:
                    if REPO_TIMEOUT_SECONDS is not None and REPO_TIMEOUT_SECONDS > 0:
                        @timeout_decorator(REPO_TIMEOUT_SECONDS)
                        def _process():
                            self.process_file_tree(repo, "", default_branch, has_nodes_flag)
                        _process()
                    else:
                        self.process_file_tree(repo, "", default_branch, has_nodes_flag)
                except RuntimeError:
                    raise
        else:
            try:
                if REPO_TIMEOUT_SECONDS is not None and REPO_TIMEOUT_SECONDS > 0:
                    @timeout_decorator(REPO_TIMEOUT_SECONDS)
                    def _process():
                        self.process_file_tree(repo, "", default_branch, has_nodes_flag)
                    _process()
                else:
                    self.process_file_tree(repo, "", default_branch, has_nodes_flag)
            except RuntimeError:
                raise
        if not has_nodes_flag[0] and github_url not in self.blacklist_repos:
            print(f"[{now_str()}] 仓库 {github_url} 未提取到节点，加入黑名单", flush=True)
            self.blacklist_repos.add(github_url)
            with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
                f.write(github_url + "\n")

    # 以下方法保持不变：_process_with_recursive_tree, _handle_one_file, _get_file_time_via_commits, _download_and_extract, process_file_tree, save_results 等
    # 请确保包含之前实现的所有逻辑，为节省篇幅此处省略，但实际代码中需要完整保留。
