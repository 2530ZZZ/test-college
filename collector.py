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

    def process_repo(self, repo: str, depth: int = 0):
        github_url = f"https://github.com/{repo}"
        if github_url in self.blacklist_repos or repo in self.seen_repos:
            return
        repo_info = safe_get(f"https://api.github.com/repos/{repo}", self.headers, timeout=REPO_INFO_TIMEOUT)
        if not repo_info:
            return
        repo_data = repo_info.json()
        if repo_data.get('size', 0) == 0 or repo_data.get('disabled', False):
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

    def _process_with_recursive_tree(self, repo: str, branch: str, has_nodes: List[bool], depth: int = 0) -> bool:
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

        # ---- 检查 README 广告 ----
        readme_path = None
        for e in entries:
            if e.get('type') == 'blob':
                path = e.get('path', '').lower()
                if path in ('readme.md','readme','readme.txt','readme.rst','readme.markdown'):
                    readme_path = e.get('path')
                    break
        if readme_path:
            readme_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{readme_path}"
            print(f"[{now_str()}] 🔍 检查 README: {readme_url}", flush=True)
            readme_resp = safe_get(readme_url, self.headers, timeout=FILE_DOWNLOAD_TIMEOUT)
            if readme_resp:
                content = readme_resp.text
                for kw in README_SPAM_KEYWORDS:
                    if kw in content:
                        print(f"[{now_str()}] 🛑 仓库 {repo} README 包含广告关键词 '{kw}'，跳过", flush=True)
                        self.blacklist_repos.add(f"https://github.com/{repo}")
                        with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
                            f.write(f"https://github.com/{repo}\n")
                        has_nodes[0] = True
                        return True

        # ---- 正常处理文件 ----
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
            if CHECK_FILE_MODIFICATION_TIME and not self._is_file_updated(sha):
                continue
            files_to_check.append((path, sha))

        if not files_to_check:
            print(f"[{now_str()}] 仓库 https://github.com/{repo} 无候选文件", flush=True)
            return True

        if MAX_COMMITS_PER_REPO is not None and len(files_to_check) > MAX_COMMITS_PER_REPO:
            print(f"[{now_str()}] ⚠️ 候选文件过多 ({len(files_to_check)} 个)，仅处理前 {MAX_COMMITS_PER_REPO} 个", flush=True)
            files_to_check = files_to_check[:MAX_COMMITS_PER_REPO]

        # 串行查询 Commits API
        print(f"[{now_str()}] 仓库 https://github.com/{repo} 候选文件 {len(files_to_check)} 个，串行查询 Commits API", flush=True)
        for file_path, sha in files_to_check:
            self._handle_one_file(repo, branch, file_path, sha, has_nodes, depth)

        return True

    def _handle_one_file(self, repo, branch, file_path, sha, has_nodes, depth):
        raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{file_path}"
        if CHECK_FILE_MODIFICATION_TIME:
            file_time, success, reason = self._get_file_time_via_commits(repo, file_path)
            if not success or file_time is None:
                print(f"[{now_str()}] ⚠️ 获取时间失败 {raw_url} 原因: {reason}", flush=True)
                self.processed_file_shas.add(sha)
                return
            if datetime.now(timezone.utc) - file_time >= timedelta(hours=24):
                print(f"[{now_str()}] ⚠️ 文件过期 ({file_time}) {raw_url}", flush=True)
                self.processed_file_shas.add(sha)
                return
        self._download_and_extract(repo, branch, file_path, raw_url, sha, has_nodes, depth)

    def _get_file_time_via_commits(self, repo: str, file_path: str) -> Tuple[Optional[datetime], bool, str]:
        commit_url = f"https://api.github.com/repos/{repo}/commits?path={file_path}&per_page=1"
        resp = safe_get(commit_url, self.headers, timeout=COMMITS_API_TIMEOUT, operation_name=f"commits 查询 {file_path}")
        if not resp:
            return None, False, "Commits API 请求失败"
        try:
            commits = resp.json()
            if not commits:
                return None, False, "无提交记录"
            time_str = commits[0]["commit"]["committer"]["date"]
            file_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            return file_time, True, ""
        except Exception as e:
            return None, False, f"解析 commits 失败: {e}"

    def _download_and_extract(self, repo, branch, file_path, raw_url, sha, has_nodes, depth):
        file_resp = safe_get(raw_url, self.headers, timeout=FILE_DOWNLOAD_TIMEOUT)
        if not file_resp:
            self.processed_file_shas.add(sha)
            check_rate_limit()
            return
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
        if MAX_FILE_SIZE is not None and len(content) > MAX_FILE_SIZE:
            self.processed_file_shas.add(sha)
            return

        def extract():
            return extract_raw_candidates(content)
        try:
            if FILE_PROCESS_TIMEOUT is not None and FILE_PROCESS_TIMEOUT > 0:
                with ThreadPoolExecutor(max_workers=1) as executor2:
                    extract_future = executor2.submit(extract)
                    candidates = extract_future.result(timeout=FILE_PROCESS_TIMEOUT)
            else:
                candidates = extract()
        except FutureTimeoutError:
            print(f"[{now_str()}] ⚠️ 文件处理超时，跳过 {raw_url}", flush=True)
            self.processed_file_shas.add(sha)
            return
        except Exception:
            self.processed_file_shas.add(sha)
            return

        new_nodes = 0
        for cand in candidates:
            if cand not in self.unique_nodes:
                self.unique_nodes.add(cand)
                new_nodes += 1

        if new_nodes:
            self.all_links.append(raw_url)
            has_nodes[0] = True
            print(f"[{now_str()}] 📄 {raw_url} ✅ 提取 {len(candidates)} 个节点，新增 {new_nodes} 个", flush=True)
        else:
            if len(candidates) == 0:
                print(f"[{now_str()}] 📄 {raw_url} ❌ 无节点（文件无有效内容）", flush=True)
            else:
                print(f"[{now_str()}] 📄 {raw_url} ⚪ 解析出 {len(candidates)} 个节点，但全部已存在，无新节点", flush=True)

        self.processed_file_shas.add(sha)

        # ---- raw 链接递归发现（串行） ----
        if ENABLE_RAW_RECURSIVE and depth < MAX_RECURSIVE_DEPTH and self.recursive_count < MAX_RECURSIVE_REPOS:
            raw_pattern = re.compile(r'https://raw\.githubusercontent\.com/([^/]+/[^/]+)/([^/]+)/([^\s"\'`#]+)')
            found = set()
            for match in raw_pattern.finditer(content):
                full_name = match.group(1)
                ref = match.group(2)
                path = match.group(3)
                ext = os.path.splitext(path)[1].lower()
                if ext not in ALLOWED_EXTENSIONS:
                    continue
                if full_name in self.seen_repos or f"https://github.com/{full_name}" in self.blacklist_repos:
                    continue
                if full_name in found:
                    continue
                found.add(full_name)
                if self.recursive_count >= MAX_RECURSIVE_REPOS:
                    break
                print(f"[{now_str()}] 🔗 递归发现仓库 {full_name} (来源 {raw_url})", flush=True)
                self.recursive_count += 1
                self.process_repo(full_name, depth + 1)

    # ----------------- 回退：Contents API -----------------
    def process_file_tree(self, repo: str, path: str, branch: str, has_nodes: List[bool]):
        contents_url = (f"https://api.github.com/repos/{repo}/contents/{path}"
                        if path else f"https://api.github.com/repos/{repo}/contents")
        resp = safe_get(contents_url, self.headers, timeout=CONTENTS_API_TIMEOUT, operation_name=f"Contents API {path or '根'}")
        if not resp:
            check_rate_limit()
            return
        items = resp.json()
        for item in items:
            item_path = item["path"]
            item_type = item["type"]
            item_sha = item["sha"]
            if item_type == "dir":
                if item_sha in self.processed_dir_shas: continue
                commit_url = f"https://api.github.com/repos/{repo}/commits?path={item_path}&per_page=1"
                c_resp = safe_get(commit_url, self.headers, timeout=COMMITS_API_TIMEOUT, operation_name=f"commit 查询目录 {item_path}")
                if not c_resp:
                    self.processed_dir_shas.add(item_sha)
                    check_rate_limit()
                    continue
                try:
                    commit_list = c_resp.json()
                    if commit_list:
                        time_str = commit_list[0]["commit"]["committer"]["date"]
                        dir_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                    else:
                        dir_time = None
                except Exception:
                    dir_time = None
                self.processed_dir_shas.add(item_sha)
                if dir_time is None or datetime.now(timezone.utc) - dir_time >= timedelta(hours=24):
                    continue
                self.process_file_tree(repo, item_path, branch, has_nodes)
            elif item_type == "file":
                ext = os.path.splitext(item_path)[1].lower()
                if ext not in ALLOWED_EXTENSIONS: continue
                if item_sha in self.processed_file_shas: continue
                commit_url = f"https://api.github.com/repos/{repo}/commits?path={item_path}&per_page=1"
                c_resp = safe_get(commit_url, self.headers, timeout=COMMITS_API_TIMEOUT, operation_name=f"commit 查询文件 {item_path}")
                if not c_resp:
                    self.processed_file_shas.add(item_sha)
                    check_rate_limit()
                    continue
                try:
                    commit_list = c_resp.json()
                    if commit_list:
                        time_str = commit_list[0]["commit"]["committer"]["date"]
                        file_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                    else:
                        file_time = None
                except Exception:
                    file_time = None
                self.processed_file_shas.add(item_sha)
                if file_time is None or datetime.now(timezone.utc) - file_time >= timedelta(hours=24):
                    continue
                file_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{item_path}"
                print(f"[{now_str()}] 🔍 下载: {file_url}", flush=True)
                file_resp = safe_get(file_url, self.headers, timeout=FILE_DOWNLOAD_TIMEOUT)
                if not file_resp:
                    check_rate_limit()
                    continue
                content = file_resp.text
                if MAX_FILE_SIZE is not None and len(content) > MAX_FILE_SIZE: continue
                def extract():
                    return extract_raw_candidates(content)
                try:
                    if FILE_PROCESS_TIMEOUT is not None and FILE_PROCESS_TIMEOUT > 0:
                        with ThreadPoolExecutor(max_workers=1) as executor:
                            future = executor.submit(extract)
                            candidates = future.result(timeout=FILE_PROCESS_TIMEOUT)
                    else:
                        candidates = extract()
                except Exception:
                    continue
                new_nodes = 0
                for cand in candidates:
                    if cand not in self.unique_nodes:
                        self.unique_nodes.add(cand)
                        new_nodes += 1
                if new_nodes:
                    self.all_links.append(file_url)
                    has_nodes[0] = True
                    print(f"[{now_str()}] 📄 {file_url} ✅ 提取 {new_nodes} 个候选块", flush=True)

    def load_blacklist(self):
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("https://github.com/"):
                        self.blacklist_repos.add(line)
            print(f"[{now_str()}] 已加载黑名单: {len(self.blacklist_repos)} 个", flush=True)

    def save_results(self):
        if self.unique_nodes:
            with open("no.txt", "w", encoding="utf-8") as f:
                f.write("\n".join(self.unique_nodes))
            print(f"[{now_str()}] 保存 no.txt ({len(self.unique_nodes)} 条)", flush=True)
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
            with open(filepath, "w", encoding="utf-8") as f:
                f.write("\n".join(chunk))
            no_w_links.append(f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/no/{filename}")
        with open("no_w_li.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(no_w_links))
        print(f"[{now_str()}] 保存 no_w_li.txt ({file_count} 分片)", flush=True)
        self.all_links.append(f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/no.txt")
        self.all_links = list(dict.fromkeys(self.all_links))
        with open("no_li.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(self.all_links))
        self.save_sha_cache()
