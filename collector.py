"""
GitHub 节点收集器 —— 使用 tree API 一次性获取文件树，按内容 SHA 去重。
仅保留黑名单 (ljck.txt) 持久化，其余全部为运行时内存状态。
"""

import os
import time
import shutil
from datetime import datetime, timezone, timedelta
from typing import List, Set, Optional

import utils
from utils import safe_get, now_str, timeout_decorator, beijing_tz
from parsers import extract_and_parse
from proxy_model import StandardProxy


BLACKLIST_FILE = "ljck.txt"

# 文件扩展名过滤：只处理这些类型的文件
ALLOWED_EXTENSIONS = {'.yaml', '.yml', '.json', '.txt', '.md', '.conf', '.list', '.base64'}


class Collector:
    def __init__(self, token: str = "", queries: List[str] = None):
        self.token = token
        self.headers = {
            "Authorization": f"token {token}" if token else "",
            "User-Agent": "Mozilla/5.0 (compatible; FreeNodesCollector/3.0)"
        }
        self.queries = queries or []

        # 运行时状态
        self.all_links: List[str] = []               # 本次收集到的源文件 raw URL
        self.unique_nodes: Set[str] = set()          # 全局去重后的节点字符串
        self.seen_repos: Set[str] = set()            # 本次已处理的仓库全名
        self.blacklist_repos: Set[str] = set()       # 永久黑名单仓库
        self.processed_shas: Set[str] = set()        # 已处理文件的 Git blob SHA（跨仓库去重）
        self.checked_count: int = 0

        # 加载黑名单
        self.load_blacklist()

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
        """搜索关键词，每页 30 条，最多 3 页"""
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

    # ==================== 仓库处理（核心修改） ====================
    @timeout_decorator(60)
    def process_repo(self, repo: str):
        """
        处理单个仓库：获取默认分支 → 尝试 tree API → 按 sha 去重下载文件
        """
        github_url = f"https://github.com/{repo}"
        if github_url in self.blacklist_repos:
            return

        # 1. 获取默认分支
        repo_info = safe_get(f"https://api.github.com/repos/{repo}", self.headers)
        if not repo_info:
            return
        repo_data = repo_info.json()
        if repo_data.get('size', 0) == 0 or repo_data.get('disabled', False):
            return

        default_branch = repo_data.get("default_branch", "main")
        print(f"[{now_str()}] 仓库 {github_url} (分支: {default_branch})", flush=True)

        # 2. 获取递归文件树
        tree_url = f"https://api.github.com/repos/{repo}/git/trees/{default_branch}?recursive=1"
        tree_resp = safe_get(tree_url, self.headers, timeout=(12, 20), operation_name="tree API")
        if not tree_resp:
            print(f"[{now_str()}] ❌ tree API 请求失败，跳过仓库 {github_url}", flush=True)
            return

        tree_data = tree_resp.json()
        # 检查截断标志
        if tree_data.get('truncated', False):
            print(f"[{now_str()}] ❌ tree 数据被截断（仓库过大），跳过仓库 {github_url}", flush=True)
            return

        entries = tree_data.get('tree', [])
        if not entries:
            print(f"[{now_str()}] 📭 仓库文件树为空，跳过", flush=True)
            return

        # 3. 过滤需要处理的文件
        files_to_process = []
        for entry in entries:
            if entry.get('type') != 'blob':
                continue
            size = entry.get('size', 0)
            if size == 0:
                continue
            path = entry.get('path', '')
            ext = os.path.splitext(path)[1].lower()
            # 简单文件扩展名过滤（可自行调整）
            if ext not in ALLOWED_EXTENSIONS:
                continue
            files_to_process.append({
                'path': path,
                'sha': entry['sha'],
                'size': size
            })

        print(f"[{now_str()}] 📂 仓库文件 {len(entries)} 个, 需处理 {len(files_to_process)} 个", flush=True)

        has_nodes_flag = False
        # 4. 按 SHA 去重下载
        for file_info in files_to_process:
            sha = file_info['sha']
            if sha in self.processed_shas:
                continue
            self.processed_shas.add(sha)          # 先标记为已处理，避免并发问题

            file_path = file_info['path']
            raw_url = f"https://raw.githubusercontent.com/{repo}/{default_branch}/{file_path}"
            print(f"[{now_str()}] 🔍 下载: {raw_url}", flush=True)

            file_resp = safe_get(raw_url, self.headers, timeout=(10, 30))
            if not file_resp:
                continue

            parse_start = time.time()
            try:
                proxies = extract_and_parse(file_resp.text, source_url=raw_url)
            except Exception as e:
                print(f"[{now_str()}] ⚠️ 解析失败 {raw_url}: {e}", flush=True)
                continue

            new_nodes = []
            for proxy in proxies:
                node_str = proxy.to_node_line()
                if node_str not in self.unique_nodes:
                    self.unique_nodes.add(node_str)
                    new_nodes.append(node_str)

            if new_nodes:
                self.all_links.append(raw_url)
                has_nodes_flag = True
                print(f"[{now_str()}] 📄 {raw_url} ✅ 新增 {len(new_nodes)} 条 "
                      f"(解析耗时 {time.time() - parse_start:.2f}s)", flush=True)
            else:
                print(f"[{now_str()}] 📄 {raw_url} ❌ 无新节点", flush=True)

        # 5. 如果仓库没有提取到任何节点，加入黑名单
        if not has_nodes_flag and github_url not in self.blacklist_repos:
            print(f"[{now_str()}] 仓库 {github_url} 未提取到节点，加入黑名单", flush=True)
            self.blacklist_repos.add(github_url)
            with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
                f.write(github_url + "\n")

    # ==================== 持久化（仅黑名单） ====================
    def load_blacklist(self):
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("https://github.com/"):
                        self.blacklist_repos.add(line)
            print(f"[{now_str()}] 已加载黑名单: {len(self.blacklist_repos)} 个", flush=True)

    def save_results(self):
        """保存 no.txt、分片、no_li.txt，不涉及任何缓存持久化"""
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

        print(f"[{now_str()}] 保存 no_li.txt ({len(self.all_links)} 条链接)", flush=True)
