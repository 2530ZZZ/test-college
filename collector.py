"""
GitHub 收集器，负责搜索仓库、遍历文件树、提取节点。
采用类封装，消除全局变量，保留黑名单、SHA 缓存、分片等全部功能。
"""

import os
import time
import shutil
from datetime import datetime, timezone, timedelta
from typing import List, Set, Dict

import utils
from utils import safe_get, now_str, timeout_decorator, beijing_tz
from parsers import extract_raw_links, parse_line
from proxy_model import StandardProxy


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
        self.unique_nodes: Set[str] = set()          # 去重后的 raw_link 集合
        self.seen_repos: Set[str] = set()
        self.blacklist_repos: Set[str] = set()
        self.commit_cache: Dict[str, str] = {}
        self.checked_count: int = 0

        # 加载持久化数据
        self.load_blacklist()
        self.load_commit_cache()

    def run(self):
        """主流程入口"""
        print(f"[{now_str()}] 🚀 程序启动，开始动态搜索...", flush=True)

        for idx, query in enumerate(self.queries, 1):
            print(f"\n[{now_str()}] 🔎 搜索 {idx}/{len(self.queries)}: {query}", flush=True)
            self.search_query(query)

        # 保存结果
        self.save_results()

    def search_query(self, query: str, max_pages: int = 10):
        """对单个关键词进行多页搜索"""
        query_links_count = 0
        for page in range(1, max_pages + 1):
            url = f"https://api.github.com/search/repositories?q={query}&sort=updated&order=desc&per_page=100&page={page}"
            resp = safe_get(url, self.headers, timeout=(15, 30), operation_name=f"搜索第{page}页")
            if not resp:
                break

            items = resp.json().get("items", [])
            if not items:
                break

            print(f"[{now_str()}] 第{page}页找到 {len(items)} 个仓库", flush=True)
            for item in items:
                repo = item["full_name"]
                if repo in self.seen_repos or f"https://github.com/{repo}" in self.blacklist_repos:
                    continue
                self.seen_repos.add(repo)
                self.checked_count += 1
                self.process_repo(repo)
                time.sleep(1.2)

            page += 1
            time.sleep(6)

        print(f"[{now_str()}] └─ 本关键词贡献 {query_links_count} 条有效链接", flush=True)

    @timeout_decorator(60)
    def process_repo(self, repo: str):
        """处理单个仓库"""
        github_url = f"https://github.com/{repo}"
        if github_url in self.blacklist_repos:
            return

        # 获取默认分支
        repo_info = safe_get(f"https://api.github.com/repos/{repo}", self.headers)
        if not repo_info:
            return
        repo_data = repo_info.json()
        if repo_data.get('size', 0) == 0 or repo_data.get('disabled', False):
            return

        default_branch = repo_data.get("default_branch", "main")

        # 检查最近提交
        commit_resp = safe_get(f"https://api.github.com/repos/{repo}/commits?per_page=1", self.headers)
        if not commit_resp:
            return
        try:
            commit_data = commit_resp.json()[0]
            commit_sha = commit_data["sha"]
            commit_time_str = commit_data["commit"]["committer"]["date"]
            commit_time = datetime.fromisoformat(commit_time_str.replace("Z", "+00:00"))
            if datetime.now(timezone.utc) - commit_time >= timedelta(hours=24):
                return
            if github_url in self.commit_cache and self.commit_cache[github_url] == commit_sha:
                print(f"  [{now_str()}] 仓库 {repo} 无新提交，跳过", flush=True)
                return
        except Exception as e:
            print(f"  [{now_str()}] 处理仓库 {repo} 异常: {e}", flush=True)
            return

        # 处理文件树
        has_nodes_flag = [False]
        self.process_file_tree(repo, "", default_branch, has_nodes_flag)

        if not has_nodes_flag[0] and github_url not in self.blacklist_repos:
            print(f"  [{now_str()}] 仓库 {repo} 未提取到节点，加入黑名单", flush=True)
            self.blacklist_repos.add(github_url)
            with open("ljck.txt", "a", encoding="utf-8") as f:
                f.write(github_url + "\n")

    def process_file_tree(self, repo: str, path: str, branch: str, has_nodes: List[bool]):
        """递归遍历文件树，提取节点"""
        contents_url = f"https://api.github.com/repos/{repo}/contents/{path}" if path else f"https://api.github.com/repos/{repo}/contents"
        resp = safe_get(contents_url, self.headers, timeout=(10, 20))
        if not resp:
            return

        items = resp.json()
        for item in items:
            item_path = item["path"]
            item_type = item["type"]

            # 简化逻辑：不逐文件查 commit（假设整个仓库已足够新）
            if item_type == "dir":
                self.process_file_tree(repo, item_path, branch, has_nodes)
            elif item_type == "file":
                file_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{item_path}"
                print(f"   🔍 检查文件: {file_url}", flush=True)
                file_resp = safe_get(file_url, self.headers, timeout=(10, 30))
                if not file_resp:
                    continue

                # 提取原始节点行
                raw_lines = extract_raw_links(file_resp.text)
                new_nodes = []
                for line in raw_lines:
                    proxy = parse_line(line)
                    if proxy:
                        node_str = proxy.to_node_line()
                        if node_str not in self.unique_nodes:
                            self.unique_nodes.add(node_str)
                            new_nodes.append(node_str)

                if new_nodes:
                    self.all_links.append(file_url)
                    has_nodes[0] = True
                    print(f"   📄 {file_url} ✅ 新增 {len(new_nodes)} 条新节点", flush=True)
                else:
                    print(f"   📄 {file_url} ❌ 无新节点", flush=True)

    # ========== 持久化 ==========
    def load_blacklist(self):
        if os.path.exists("ljck.txt"):
            with open("ljck.txt", "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("https://github.com/"):
                        self.blacklist_repos.add(line)

    def load_commit_cache(self):
        if os.path.exists("repo_commit_cache.txt"):
            with open("repo_commit_cache.txt", "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) >= 2 and parts[0].startswith("https://github.com/"):
                        self.commit_cache[parts[0]] = parts[1]

    def save_commit_cache(self):
        with open("repo_commit_cache.txt", "w", encoding="utf-8") as f:
            for url, sha in self.commit_cache.items():
                f.write(f"{url} {sha}\n")

    def save_results(self):
        """保存 no.txt、no_li.txt、分片文件夹、no_w_li.txt"""
        # 1. no.txt
        if self.unique_nodes:
            with open("no.txt", "w", encoding="utf-8") as f:
                f.write("\n".join(self.unique_nodes))
            print(f"\n[{now_str()}] 已保存 {len(self.unique_nodes)} 条节点到 no.txt", flush=True)

        # 2. 分片 no/ 文件夹
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
            chunk = nodes_list[i:i+chunk_size]
            file_count += 1
            filename = f"{file_count}.txt"
            filepath = os.path.join(no_dir, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write("\n".join(chunk))
            raw_url = f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/no/{filename}"
            no_w_links.append(raw_url)

        with open("no_w_li.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(no_w_links))
        print(f"[{now_str()}] 已生成 no_w_li.txt ({file_count} 个分片)", flush=True)

        # 3. no_li.txt
        no_txt_raw = f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/no.txt"
        self.all_links.append(no_txt_raw)
        self.all_links = list(dict.fromkeys(self.all_links))  # 去重
        with open("no_li.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(self.all_links))

        # 4. 保存缓存
        self.save_commit_cache()
