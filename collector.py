"""
GitHub 节点收集器 —— 支持优先使用 git/trees API 获取递归文件树，
并用 HEAD 请求获取文件修改时间，彻底避免 /commits API 调用。
根据候选文件数量自动切换串行/并发处理，避免小文件集的开销。
新增 README 广告内容检测，避免浪费后续 API。
"""

import os
import time
import shutil
import requests
import urllib.parse
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from typing import List, Set, Optional

import utils
from utils import safe_get, now_str, timeout_decorator, beijing_tz, check_rate_limit
from parsers import extract_raw_candidates

from config import (
    CHUNK_SIZE, MAX_PAGES, PER_PAGE, REPO_SLEEP_SECONDS, PAGE_SLEEP_SECONDS,
    REPO_TIMEOUT_SECONDS, MAX_FILE_SIZE, FILE_PROCESS_TIMEOUT,
    ALLOWED_EXTENSIONS, BLACKLIST_FILE,
    SEARCH_TIMEOUT, REPO_INFO_TIMEOUT, FILE_DOWNLOAD_TIMEOUT,
    CONTENTS_API_TIMEOUT, COMMITS_API_TIMEOUT, TREE_API_TIMEOUT,
    USE_RECURSIVE_TREE, HEAD_CONCURRENCY, MAX_HEAD_PER_REPO, MIN_FILES_FOR_CONCURRENCY,
    CHECK_FILE_MODIFICATION_TIME, README_SPAM_PATTERNS,
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
        self.load_blacklist()

    def run(self):
        print(f"[{now_str()}] 🚀 程序启动", flush=True)
        start_time = time.time()
        for idx, query in enumerate(self.queries, 1):
            if utils.total_rate_limit_wait >= utils.MAX_TOTAL_RATE_LIMIT_WAIT:
                print(f"[{now_str()}] ⚠️ 累计限流等待已达 "
                      f"{utils.total_rate_limit_wait:.0f}s，终止搜索", flush=True)
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
        print(f"[{now_str()}] 检查仓库: {self.checked_count}, "
              f"源链接: {len(self.all_links)}, 节点: {len(self.unique_nodes)}", flush=True)
        print(f"[{now_str()}] 累计限流等待: {utils.total_rate_limit_wait:.0f} 秒", flush=True)

    def search_query(self, query: str):
        for page in range(1, MAX_PAGES + 1):
            url = (
                f"https://api.github.com/search/repositories"
                f"?q={query}&sort=updated&order=desc"
                f"&per_page={PER_PAGE}&page={page}"
            )
            resp = safe_get(url, self.headers, timeout=SEARCH_TIMEOUT,
                            operation_name=f"搜索第{page}页")
            if not resp:
                check_rate_limit()
                break
            data = resp.json()
            total_count = data.get("total_count", 0)
            items = data.get("items", [])
            print(f"[{now_str()}] 第{page}页 total_count={total_count}, items={len(items)}", flush=True)
            if not items:
                break

            for item in items:
                repo = item["full_name"]
                github_url = f"https://github.com/{repo}"
                if repo in self.seen_repos or github_url in self.blacklist_repos:
                    continue
                self.seen_repos.add(repo)
                self.checked_count += 1
                try:
                    self.process_repo(repo)
                except RuntimeError:
                    print(f"[{now_str()}] ⚠️ 限流超限，停止处理仓库", flush=True)
                    return
                time.sleep(REPO_SLEEP_SECONDS)
            time.sleep(PAGE_SLEEP_SECONDS)

    def process_repo(self, repo: str):
        github_url = f"https://github.com/{repo}"
        if github_url in self.blacklist_repos:
            return

        repo_info = safe_get(f"https://api.github.com/repos/{repo}", self.headers,
                             timeout=REPO_INFO_TIMEOUT)
        if not repo_info:
            return
        repo_data = repo_info.json()
        if repo_data.get('size', 0) == 0 or repo_data.get('disabled', False):
            return
        default_branch = repo_data.get("default_branch", "main")
        print(f"[{now_str()}] 仓库 {github_url} (分支: {default_branch})", flush=True)

        has_nodes_flag = [False]

        if USE_RECURSIVE_TREE:
            success = self._process_with_recursive_tree(repo, default_branch, has_nodes_flag)
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

    # ----------------- 递归树 API 处理 -----------------
    def _process_with_recursive_tree(self, repo: str, branch: str, has_nodes: List[bool]) -> bool:
        tree_url = f"https://api.github.com/repos/{repo}/git/trees/{branch}?recursive=1"
        resp = safe_get(tree_url, self.headers, timeout=TREE_API_TIMEOUT, operation_name="递归树")
        if not resp:
            check_rate_limit()
            return False
        data = resp.json()
        if data.get('truncated', False):
            print(f"[{now_str()}] 树数据被截断，回退", flush=True)
            return False
        entries = data.get('tree', [])
        if not entries:
            return True

        # 1. 优先检查 README 内容，过滤广告仓库
        readme_path = None
        for e in entries:
            if e.get('type') == 'blob':
                path = e.get('path', '').lower()
                if path in ('readme.md', 'readme', 'readme.txt', 'readme.rst'):
                    readme_path = e.get('path')
                    break

        if readme_path:
            readme_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{readme_path}"
            print(f"[{now_str()}] 🔍 检查 README: {readme_url}", flush=True)
            readme_resp = safe_get(readme_url, self.headers, timeout=FILE_DOWNLOAD_TIMEOUT)
            if readme_resp:
                content = readme_resp.text
                for pattern in README_SPAM_PATTERNS:
                    if pattern in content:
                        print(f"[{now_str()}] 🛑 仓库 {repo} README 包含广告特征 '{pattern}'，跳过", flush=True)
                        self.blacklist_repos.add(f"https://github.com/{repo}")
                        with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
                            f.write(f"https://github.com/{repo}\n")
                        return True  # 返回 True 但 has_nodes 仍为 False，process_repo 会看到无节点而不会重复加入黑名单（但我们已加入）

        # 2. 正常处理文件
        files_to_check = []
        for e in entries:
            if e.get('type') != 'blob':
                continue
            path = e.get('path', '')
            ext = os.path.splitext(path)[1].lower()
            if ext not in ALLOWED_EXTENSIONS:
                continue
            sha = e.get('sha', '')
            if sha in self.processed_file_shas:
                continue
            files_to_check.append((path, sha))

        if not files_to_check:
            return True

        if MAX_HEAD_PER_REPO is not None and len(files_to_check) > MAX_HEAD_PER_REPO:
            print(f"[{now_str()}] ⚠️ 候选文件过多 ({len(files_to_check)} 个)，"
                  f"仅处理前 {MAX_HEAD_PER_REPO} 个", flush=True)
            files_to_check = files_to_check[:MAX_HEAD_PER_REPO]

        total = len(files_to_check)
        if total < MIN_FILES_FOR_CONCURRENCY:
            print(f"[{now_str()}] 候选文件 {total} 个（<{MIN_FILES_FOR_CONCURRENCY}），串行处理", flush=True)
            self._process_files_sequential(repo, branch, files_to_check, has_nodes)
        else:
            print(f"[{now_str()}] 候选文件 {total} 个，并发 {HEAD_CONCURRENCY} 线程处理", flush=True)
            self._process_files_concurrent(repo, branch, files_to_check, has_nodes)

        return True

    # ... 后续方法 _process_files_sequential, _process_files_concurrent, _handle_one_file 等保持不变 ...
