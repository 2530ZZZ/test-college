"""
GitHub 节点收集器 —— 负责搜索仓库、遍历文件树、提取节点。
移除仓库级别的 commit 检查（因为搜索已限定 pushed:>date），
保留文件缓存、黑名单、去重、分片等全部功能。
"""

import os
import time
import shutil
from datetime import datetime, timezone, timedelta
from typing import List, Set, Dict, Tuple, Optional
from email.utils import parsedate_to_datetime

import utils
from utils import safe_get, now_str, timeout_decorator, beijing_tz
from parsers import extract_and_parse
from proxy_model import StandardProxy


BLACKLIST_FILE = "ljck.txt"
FILE_CACHE_FILE = "dr_commit_cache.txt"          # 仅文件缓存保留


class Collector:
    def __init__(self, token: str = "", queries: List[str] = None):
        self.token = token
        self.headers = {
            "Authorization": f"token {token}" if token else "",
            "User-Agent": "Mozilla/5.0 (compatible; FreeNodesCollector/3.0)"
        }
        self.queries = queries or []

        # 状态变量
        self.all_links: List[str] = []
        self.unique_nodes: Set[str] = set()
        self.seen_repos: Set[str] = set()                  # 本次运行已处理仓库
        self.blacklist_repos: Set[str] = set()              # 永久黑名单
        self.checked_count: int = 0

        # 仅文件缓存 (key: raw_url, value: (commit_sha, timestamp))
        self.file_cache: Dict[str, Tuple[str, datetime]] = {}

        # 加载持久化数据
        self.load_blacklist()
        self.load_file_cache()

    # ==================== 主流程 ====================
    def run(self):
        print(f"[{now_str()}] 🚀 程序启动", flush=True)
        start_time = time.time()

        for idx, query in enumerate(self.queries, 1):
            q_start = time.time()
            print(f"\n[{now_str()}] 🔎 搜索 {idx}/{len(self.queries)}: {query}", flush=True)
            self.search_query(query)
            print(f"[{now_str()}]   关键词耗时: {time.time() - q_start:.1f}s", flush=True)

        self.save_results()

        elapsed = time.time() - start_time
        print(f"\n[{now_str()}] 🎉 收集完成，总耗时 {elapsed:.0f}s", flush=True)
        print(f"[{now_str()}] 检查仓库: {self.checked_count}, 源链接: {len(self.all_links)}, 节点: {len(self.unique_nodes)}", flush=True)

    def search_query(self, query: str, max_pages: int = 3):
        """搜索关键词，每页30条，最多3页"""
        for page in range(1, max_pages + 1):
            url = f"https://api.github.com/search/repositories?q={query}&sort=updated&order=desc&per_page=30&page={page}"
            resp = safe_get(url, self.headers, timeout=(15, 30), operation_name=f"搜索第{page}页")
            if not resp:
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
                self.process_repo(repo)
                time.sleep(0.5)
            page += 1
            time.sleep(2)

    # ==================== 仓库处理（简化） ====================
    @timeout_decorator(60)
    def process_repo(self, repo: str):
        """只获取默认分支，然后进入文件树遍历，不再检查仓库整体 commit"""
        github_url = f"https://github.com/{repo}"
        if github_url in self.blacklist_repos:
            return

        # 获取仓库信息（为了得到默认分支）
        repo_info = safe_get(f"https://api.github.com/repos/{repo}", self.headers)
        if not repo_info:
            return
        repo_data = repo_info.json()
        if repo_data.get('size', 0) == 0 or repo_data.get('disabled', False):
            return

        default_branch = repo_data.get("default_branch", "main")
        print(f"[{now_str()}] 仓库 {github_url} (分支: {default_branch})", flush=True)

        has_nodes_flag = [False]
        self.process_file_tree(repo, "", default_branch, has_nodes_flag)

        if not has_nodes_flag[0] and github_url not in self.blacklist_repos:
            print(f"[{now_str()}] 仓库 {github_url} 未提取到节点，加入黑名单", flush=True)
            self.blacklist_repos.add(github_url)
            with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
                f.write(github_url + "\n")

    # ==================== 文件树遍历（不变，仍检查文件级 commit） ====================
    def process_file_tree(self, repo: str, path: str, branch: str, has_nodes: List[bool]):
        contents_url = f"https://api.github.com/repos/{repo}/contents/{path}" if path else f"https://api.github.com/repos/{repo}/contents"
        resp = safe_get(contents_url, self.headers, timeout=(10, 20), operation_name=f"Contents API {path or '根'}")
        if not resp:
            return

        items = resp.json()
        for item in items:
            item_path = item["path"]
            item_type = item["type"]

            file_time = None
            time_source = None
            file_commit_sha = None

            # Commits API
            commit_url = f"https://api.github.com/repos/{repo}/commits?path={item_path}&per_page=1"
            c_resp = safe_get(commit_url, self.headers, timeout=(8, 12), operation_name=f"commit 查询 {item_path}")
            if c_resp and c_resp.status_code == 200:
                try:
                    commit_list = c_resp.json()
                    if commit_list:
                        commit_obj = commit_list[0]
                        file_commit_sha = commit_obj["sha"]
                        time_str = commit_obj["commit"]["committer"]["date"]
                        file_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                        time_source = "commits API"
                except Exception:
                    pass

            # HEAD Last-Modified
            if file_time is None and item_type == "file":
                head_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{item_path}"
                head_resp = safe_get(head_url, self.headers, timeout=(8, 10), operation_name=f"HEAD 请求 {item_path}", max_retries=1)
                if head_resp and head_resp.status_code == 200:
                    last_mod = head_resp.headers.get('Last-Modified')
                    if last_mod:
                        try:
                            file_time = parsedate_to_datetime(last_mod).replace(tzinfo=timezone.utc)
                            time_source = "Last-Modified"
                        except Exception:
                            pass

            if file_time is None:
                if item_type == "dir":
                    print(f"[{now_str()}] ➡️ 进入目录 https://github.com/{repo}/tree/{branch}/{item_path} (无时间)", flush=True)
                    self.process_file_tree(repo, item_path, branch, has_nodes)
                else:
                    print(f"[{now_str()}] ⏭️ 跳过文件 https://github.com/{repo}/blob/{branch}/{item_path} (无时间)", flush=True)
                continue

            if datetime.now(timezone.utc) - file_time >= timedelta(hours=24):
                print(f"[{now_str()}] ⏭️ 跳过 https://github.com/{repo}/blob/{branch}/{item_path} "
                      f"({file_time.strftime('%Y-%m-%d %H:%M:%S')}, {time_source})", flush=True)
                if item_type == "dir":
                    print(f"[{now_str()}] 🚫 目录过期，跳过递归", flush=True)
                continue

            print(f"[{now_str()}] ✅ https://github.com/{repo}/blob/{branch}/{item_path} "
                  f"({file_time.strftime('%Y-%m-%d %H:%M:%S')}, {time_source})", flush=True)

            if item_type == "dir":
                self.process_file_tree(repo, item_path, branch, has_nodes)
            elif item_type == "file":
                file_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{item_path}"
                # 文件缓存检查
                if file_commit_sha and file_url in self.file_cache:
                    cached_sha, cached_time = self.file_cache[file_url]
                    if cached_sha == file_commit_sha and datetime.now(timezone.utc) - cached_time < timedelta(hours=24):
                        print(f"[{now_str()}] ⏭️ 跳过文件 {file_url} (缓存命中 {file_commit_sha[:7]})", flush=True)
                        continue

                print(f"[{now_str()}] 🔍 检查文件: {file_url}", flush=True)
                file_resp = safe_get(file_url, self.headers, timeout=(10, 30))
                if not file_resp:
                    continue

                parse_start = time.time()
                proxies = extract_and_parse(file_resp.text, source_url=file_url)
                new_nodes = []
                for proxy in proxies:
                    node_str = proxy.to_node_line()
                    if node_str not in self.unique_nodes:
                        self.unique_nodes.add(node_str)
                        new_nodes.append(node_str)

                if new_nodes:
                    self.all_links.append(file_url)
                    has_nodes[0] = True
                    print(f"[{now_str()}] 📄 {file_url} ✅ 新增 {len(new_nodes)} 条 "
                          f"(解析耗时 {time.time() - parse_start:.2f}s)", flush=True)
                else:
                    print(f"[{now_str()}] 📄 {file_url} ❌ 无新节点", flush=True)

                if file_commit_sha:
                    self.file_cache[file_url] = (file_commit_sha, datetime.now(timezone.utc))

    # ==================== 持久化 ====================
    def load_blacklist(self):
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("https://github.com/"):
                        self.blacklist_repos.add(line)
            print(f"[{now_str()}] 加载黑名单: {len(self.blacklist_repos)} 个", flush=True)

    def load_file_cache(self):
        if os.path.exists(FILE_CACHE_FILE):
            with open(FILE_CACHE_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split(maxsplit=2)
                    if len(parts) >= 2 and "raw.githubusercontent.com" in parts[0]:
                        url = parts[0]
                        sha = parts[1]
                        ts = datetime.min.replace(tzinfo=timezone.utc)
                        if len(parts) >= 3:
                            try:
                                ts = datetime.fromisoformat(parts[2])
                            except ValueError:
                                pass
                        self.file_cache[url] = (sha, ts)
            print(f"[{now_str()}] 加载文件缓存: {len(self.file_cache)} 条", flush=True)

    def clean_file_cache(self):
        now = datetime.now(timezone.utc)
        self.file_cache = {url: (sha, ts) for url, (sha, ts) in self.file_cache.items()
                           if now - ts < timedelta(hours=24)}

    def save_file_cache(self):
        self.clean_file_cache()
        with open(FILE_CACHE_FILE, "w", encoding="utf-8") as f:
            for url, (sha, ts) in self.file_cache.items():
                f.write(f"{url} {sha} {ts.isoformat()}\n")

    def save_results(self):
        # no.txt
        if self.unique_nodes:
            with open("no.txt", "w", encoding="utf-8") as f:
                f.write("\n".join(self.unique_nodes))
            print(f"[{now_str()}] 保存 no.txt ({len(self.unique_nodes)} 条)", flush=True)

        # 分片 no/ 和 no_w_li.txt
        no_dir = "no"
        if os.path.exists(no_dir):
            shutil.rmtree(no_dir)
        os.makedirs(no_dir, exist_ok=True)

        nodes_list = list(self.unique_nodes)
        chunk_size = 10000
        file_count = 0
        no_w_links = []
        repo_name = os.getenv("GITHUB_REPOSITORY", "2530ZZZ/cooo")
        branch_name = os.getenv("GITHUB_REF_NAME", "main")

        for i in range(0, len(nodes_list), chunk_size):
            chunk = nodes_list[i:i + chunk_size]
            file_count += 1
            filename = f"{file_count}.txt"
            filepath = os.path.join(no_dir, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write("\n".join(chunk))
            no_w_links.append(f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/no/{filename}")

        with open("no_w_li.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(no_w_links))
        print(f"[{now_str()}] 保存 no_w_li.txt ({file_count} 分片)", flush=True)

        # no_li.txt
        self.all_links.append(f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/no.txt")
        self.all_links = list(dict.fromkeys(self.all_links))
        with open("no_li.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(self.all_links))

        self.save_file_cache()
        print(f"[{now_str()}] 文件缓存已更新", flush=True)
