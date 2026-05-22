"""
GitHub 节点收集器 —— Contents API 逐层递归 + 目录/文件 SHA 内存去重。
收集到的候选块通过 subconverter 解析为标准化 URI 节点。
"""

import os
import time
import shutil
from datetime import datetime, timezone, timedelta
from typing import List, Set, Optional

import utils
from utils import safe_get, now_str, timeout_decorator, beijing_tz
from parsers import extract_raw_candidates
from config import CHUNK_SIZE

BLACKLIST_FILE = "ljck.txt"
ALLOWED_EXTENSIONS = {'.yaml', '.yml', '.json', '.txt', '.md', '.conf', '.list', '.base64'}


class Collector:
    def __init__(self, token: str = "", queries: List[str] = None):
        self.token = token
        self.headers = {
            "Authorization": f"token {token}" if token else "",
            "User-Agent": "Mozilla/5.0 (compatible; FreeNodesCollector/4.0)"
        }
        self.queries = queries or []
        self.all_links: List[str] = []
        self.unique_nodes: Set[str] = set()          # 标准化 URI 节点集合
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
            q_start = time.time()
            print(f"\n[{now_str()}] 🔎 搜索 {idx}/{len(self.queries)}: {query}", flush=True)
            self.search_query(query)
            print(f"[{now_str()}]   关键词耗时: {time.time() - q_start:.1f}s", flush=True)
        self.save_results()
        print(f"\n[{now_str()}] 🎉 收集完成，总耗时 {time.time() - start_time:.0f}s", flush=True)
        print(f"[{now_str()}] 检查仓库: {self.checked_count}, 源链接: {len(self.all_links)}, 节点: {len(self.unique_nodes)}", flush=True)

    def search_query(self, query: str, max_pages: int = 3):
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

    @timeout_decorator(60)
    def process_repo(self, repo: str):
        github_url = f"https://github.com/{repo}"
        if github_url in self.blacklist_repos:
            return
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

    def process_file_tree(self, repo: str, path: str, branch: str, has_nodes: List[bool]):
        contents_url = f"https://api.github.com/repos/{repo}/contents/{path}" if path else f"https://api.github.com/repos/{repo}/contents"
        resp = safe_get(contents_url, self.headers, timeout=(10, 20), operation_name=f"Contents API {path or '根'}")
        if not resp:
            return
        items = resp.json()
        for item in items:
            item_path = item["path"]
            item_type = item["type"]
            item_sha = item["sha"]

            if item_type == "dir":
                if item_sha in self.processed_dir_shas:
                    print(f"[{now_str()}] ⏭️ 跳过目录 https://github.com/{repo}/tree/{branch}/{item_path} (SHA 已处理)", flush=True)
                    continue
                commit_url = f"https://api.github.com/repos/{repo}/commits?path={item_path}&per_page=1"
                c_resp = safe_get(commit_url, self.headers, timeout=(8, 12), operation_name=f"commit 查询目录 {item_path}")
                if not c_resp:
                    self.processed_dir_shas.add(item_sha)
                    print(f"[{now_str()}] ⚠️ 目录 {item_path} 查询 commits 失败，跳过", flush=True)
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
                    print(f"[{now_str()}] ⏭️ 跳过目录 https://github.com/{repo}/tree/{branch}/{item_path} (超过24h)", flush=True)
                    continue
                print(f"[{now_str()}] ✅ 进入目录 https://github.com/{repo}/tree/{branch}/{item_path}", flush=True)
                self.process_file_tree(repo, item_path, branch, has_nodes)

            elif item_type == "file":
                ext = os.path.splitext(item_path)[1].lower()
                if ext not in ALLOWED_EXTENSIONS:
                    continue
                if item_sha in self.processed_file_shas:
                    continue
                commit_url = f"https://api.github.com/repos/{repo}/commits?path={item_path}&per_page=1"
                c_resp = safe_get(commit_url, self.headers, timeout=(8, 12), operation_name=f"commit 查询文件 {item_path}")
                if not c_resp:
                    self.processed_file_shas.add(item_sha)
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
                file_resp = safe_get(file_url, self.headers, timeout=(10, 30))
                if not file_resp:
                    continue

                candidates = extract_raw_candidates(file_resp.text)
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
