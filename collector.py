"""
GitHub 节点收集器 —— 支持优先使用 git/trees API 获取递归文件树，
并用并发 HEAD 请求获取文件修改时间，彻底避免 /commits API 调用。
若 tree API 不可用则回退到原 Contents 逐层递归 + commits 查询。
"""

import os
import time
import shutil
import requests
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
    USE_RECURSIVE_TREE, HEAD_CONCURRENCY, MAX_HEAD_PER_REPO,
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
            url = f"https://api.github.com/search/repositories?q={query}&sort=updated&order=desc&per_page={PER_PAGE}&page={page}"
            resp = safe_get(url, self.headers, timeout=SEARCH_TIMEOUT,
                            operation_name=f"搜索第{page}页")
            if not resp:
                check_rate_limit()
                break
            items = resp.json().get("items", [])
            if not items:
                break
            print(f"[{now_str()}] 第{page}页 {len(items)} 个仓库", flush=True)
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

        # 优先尝试递归树 API
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
        """
        使用 git/trees?recursive=1 一次性获取所有文件，结合并发 HEAD 请求判断时间。
        成功处理返回 True，否则返回 False（触发回退）。
        """
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

        # 筛选候选文件：只处理 blob 类型、允许的扩展名、且 SHA 未处理过的
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

        # 限制最大处理数量（MAX_HEAD_PER_REPO 为 None 则不限制）
        if MAX_HEAD_PER_REPO is not None and len(files_to_check) > MAX_HEAD_PER_REPO:
            print(f"[{now_str()}] ⚠️ 候选文件过多 ({len(files_to_check)} 个)，"
                  f"仅处理前 {MAX_HEAD_PER_REPO} 个", flush=True)
            files_to_check = files_to_check[:MAX_HEAD_PER_REPO]

        print(f"[{now_str()}] 递归树获取到 {len(files_to_check)} 个候选文件，"
              f"并发 {HEAD_CONCURRENCY} 线程处理", flush=True)

        # 创建 Session 复用连接
        session = requests.Session()
        session.headers.update(self.headers)

        # 构建 HEAD 任务
        head_tasks = {}
        with ThreadPoolExecutor(max_workers=HEAD_CONCURRENCY) as executor:
            for file_path, sha in files_to_check:
                raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{file_path}"
                future = executor.submit(
                    self._head_one_file, session, raw_url, file_path, sha
                )
                head_tasks[future] = (file_path, sha)

            for future in as_completed(head_tasks):
                file_path, sha = head_tasks[future]
                try:
                    file_time, success = future.result()
                except Exception:
                    file_time, success = None, False

                if not success or file_time is None:
                    self.processed_file_shas.add(sha)
                    continue

                # 检查是否在 24 小时内
                if datetime.now(timezone.utc) - file_time >= timedelta(hours=24):
                    self.processed_file_shas.add(sha)
                    continue

                # 下载文件并提取节点
                raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{file_path}"
                file_resp = safe_get(raw_url, self.headers, timeout=FILE_DOWNLOAD_TIMEOUT)
                if not file_resp:
                    self.processed_file_shas.add(sha)
                    check_rate_limit()
                    continue

                content = file_resp.text
                if MAX_FILE_SIZE is not None and len(content) > MAX_FILE_SIZE:
                    self.processed_file_shas.add(sha)
                    continue

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
                    continue
                except Exception:
                    self.processed_file_shas.add(sha)
                    continue

                new_nodes = 0
                for cand in candidates:
                    if cand not in self.unique_nodes:
                        self.unique_nodes.add(cand)
                        new_nodes += 1
                if new_nodes:
                    self.all_links.append(raw_url)
                    has_nodes[0] = True
                    print(f"[{now_str()}] 📄 {raw_url} ✅ 提取 {new_nodes} 个候选块", flush=True)
                else:
                    print(f"[{now_str()}] 📄 {raw_url} ❌ 无新节点", flush=True)

                self.processed_file_shas.add(sha)

        session.close()
        return True

    def _head_one_file(self, session, raw_url, file_path, sha):
        """单个文件的 HEAD 请求，返回 (file_time, success)。"""
        try:
            head_resp = session.head(raw_url, timeout=(8, 10))
            if head_resp.status_code == 200:
                last_mod = head_resp.headers.get('Last-Modified')
                if last_mod:
                    try:
                        file_time = parsedate_to_datetime(last_mod).replace(tzinfo=timezone.utc)
                        return file_time, True
                    except Exception:
                        pass
            return None, False
        except Exception:
            return None, False

    # ----------------- 原 Contents 递归 + commits 逻辑（回退） -----------------
    def process_file_tree(self, repo: str, path: str, branch: str, has_nodes: List[bool]):
        contents_url = (f"https://api.github.com/repos/{repo}/contents/{path}"
                        if path else f"https://api.github.com/repos/{repo}/contents")
        resp = safe_get(contents_url, self.headers, timeout=CONTENTS_API_TIMEOUT,
                        operation_name=f"Contents API {path or '根'}")
        if not resp:
            check_rate_limit()
            return
        items = resp.json()
        for item in items:
            item_path = item["path"]
            item_type = item["type"]
            item_sha = item["sha"]

            if item_type == "dir":
                if item_sha in self.processed_dir_shas:
                    continue
                commit_url = f"https://api.github.com/repos/{repo}/commits?path={item_path}&per_page=1"
                c_resp = safe_get(commit_url, self.headers, timeout=COMMITS_API_TIMEOUT,
                                  operation_name=f"commit 查询目录 {item_path}")
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
                if ext not in ALLOWED_EXTENSIONS:
                    continue
                if item_sha in self.processed_file_shas:
                    continue
                commit_url = f"https://api.github.com/repos/{repo}/commits?path={item_path}&per_page=1"
                c_resp = safe_get(commit_url, self.headers, timeout=COMMITS_API_TIMEOUT,
                                  operation_name=f"commit 查询文件 {item_path}")
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
                if MAX_FILE_SIZE is not None and len(content) > MAX_FILE_SIZE:
                    continue

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
            no_w_links.append(
                f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/no/{filename}"
            )
        with open("no_w_li.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(no_w_links))
        print(f"[{now_str()}] 保存 no_w_li.txt ({file_count} 分片)", flush=True)
        self.all_links.append(f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/no.txt")
        self.all_links = list(dict.fromkeys(self.all_links))
        with open("no_li.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(self.all_links))
