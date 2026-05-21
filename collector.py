"""
GitHub 节点收集器 —— Contents API 逐层递归 + 目录/文件 SHA 内存去重。
只保留 ljck.txt 黑名单持久化，其他状态全部为运行时内存。
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

# 允许处理的文件扩展名
ALLOWED_EXTENSIONS = {'.yaml', '.yml', '.json', '.txt', '.md', '.conf', '.list', '.base64'}


class Collector:
    def __init__(self, token: str = "", queries: List[str] = None):
        self.token = token
        self.headers = {
            "Authorization": f"token {token}" if token else "",
            "User-Agent": "Mozilla/5.0 (compatible; FreeNodesCollector/4.0)"
        }
        self.queries = queries or []

        # 运行时状态
        self.all_links: List[str] = []
        self.unique_nodes: Set[str] = set()
        self.seen_repos: Set[str] = set()
        self.blacklist_repos: Set[str] = set()
        self.checked_count: int = 0

        # 内存 SHA 去重集合
        self.processed_dir_shas: Set[str] = set()    # 目录 tree sha
        self.processed_file_shas: Set[str] = set()   # 文件 blob sha

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

    # ==================== 仓库处理 ====================
    @timeout_decorator(60)
    def process_repo(self, repo: str):
        """获取默认分支，然后递归遍历文件树"""
        github_url = f"https://github.com/{repo}"
        if github_url in self.blacklist_repos:
            return

        # 获取仓库信息
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

    # ==================== 文件树递归（核心） ====================
    def process_file_tree(self, repo: str, path: str, branch: str, has_nodes: List[bool]):
        """
        递归处理目录：
        1. 使用 Contents API 获取当前目录条目
        2. 对每个条目，先检查 SHA（目录 tree sha / 文件 blob sha）
        3. SHA 已存在 → 跳过（不再查询 commits，不再下载）
        4. SHA 不存在 → 调用 commits API 获取最后修改时间
           - 目录：若时间 ≤ 24h → 递归进入；无论是否进入，都记录 SHA
           - 文件：若时间 ≤ 24h → 下载并提取节点；无论是否下载，都记录 SHA
        """
        contents_url = f"https://api.github.com/repos/{repo}/contents/{path}" if path else f"https://api.github.com/repos/{repo}/contents"
        resp = safe_get(contents_url, self.headers, timeout=(10, 20), operation_name=f"Contents API {path or '根'}")
        if not resp:
            return

        items = resp.json()
        for item in items:
            item_path = item["path"]            # 完整路径
            item_type = item["type"]            # "file" 或 "dir"
            item_sha = item["sha"]              # blob sha or tree sha

            # ---------- 目录处理 ----------
            if item_type == "dir":
                # SHA 去重
                if item_sha in self.processed_dir_shas:
                    print(f"[{now_str()}] ⏭️ 跳过目录 https://github.com/{repo}/tree/{branch}/{item_path} (目录 SHA 已处理)", flush=True)
                    continue

                # 首次遇见，查询 commits 获取最后修改时间
                commit_url = f"https://api.github.com/repos/{repo}/commits?path={item_path}&per_page=1"
                c_resp = safe_get(commit_url, self.headers, timeout=(8, 12), operation_name=f"commit 查询目录 {item_path}")
                if not c_resp:
                    # API 失败，保守跳过，但记录 SHA 避免重复请求
                    self.processed_dir_shas.add(item_sha)
                    print(f"[{now_str()}] ⚠️ 目录 {item_path} 查询 commits 失败，跳过", flush=True)
                    continue

                try:
                    commit_list = c_resp.json()
                    if commit_list:
                        time_str = commit_list[0]["commit"]["committer"]["date"]
                        dir_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                        time_source = "commits API"
                    else:
                        # 无 commit 记录，视为过期
                        dir_time = None
                        time_source = "无记录"
                except Exception:
                    dir_time = None
                    time_source = "解析失败"

                # 无论如何都要记录 SHA
                self.processed_dir_shas.add(item_sha)

                if dir_time is None or datetime.now(timezone.utc) - dir_time >= timedelta(hours=24):
                    print(f"[{now_str()}] ⏭️ 跳过目录 https://github.com/{repo}/tree/{branch}/{item_path} "
                          f"({dir_time.strftime('%Y-%m-%d %H:%M:%S') if dir_time else '无时间'}, {time_source})", flush=True)
                    continue

                # 目录在 24 小时内更新，递归进入
                print(f"[{now_str()}] ✅ 进入目录 https://github.com/{repo}/tree/{branch}/{item_path} "
                      f"({dir_time.strftime('%Y-%m-%d %H:%M:%S')}, {time_source})", flush=True)
                self.process_file_tree(repo, item_path, branch, has_nodes)

            # ---------- 文件处理 ----------
            elif item_type == "file":
                # 扩展名过滤
                ext = os.path.splitext(item_path)[1].lower()
                if ext not in ALLOWED_EXTENSIONS:
                    continue

                # SHA 去重
                if item_sha in self.processed_file_shas:
                    print(f"[{now_str()}] ⏭️ 跳过文件 https://github.com/{repo}/blob/{branch}/{item_path} (文件 SHA 已处理)", flush=True)
                    continue

                # 首次遇见，查询 commits 获取最后修改时间
                commit_url = f"https://api.github.com/repos/{repo}/commits?path={item_path}&per_page=1"
                c_resp = safe_get(commit_url, self.headers, timeout=(8, 12), operation_name=f"commit 查询文件 {item_path}")
                if not c_resp:
                    # API 失败，跳过下载，但记录 SHA
                    self.processed_file_shas.add(item_sha)
                    print(f"[{now_str()}] ⚠️ 文件 {item_path} 查询 commits 失败，跳过", flush=True)
                    continue

                try:
                    commit_list = c_resp.json()
                    if commit_list:
                        time_str = commit_list[0]["commit"]["committer"]["date"]
                        file_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                        time_source = "commits API"
                    else:
                        file_time = None
                        time_source = "无记录"
                except Exception:
                    file_time = None
                    time_source = "解析失败"

                # 无论如何都要记录 SHA
                self.processed_file_shas.add(item_sha)

                if file_time is None or datetime.now(timezone.utc) - file_time >= timedelta(hours=24):
                    print(f"[{now_str()}] ⏭️ 跳过文件 https://github.com/{repo}/blob/{branch}/{item_path} "
                          f"({file_time.strftime('%Y-%m-%d %H:%M:%S') if file_time else '无时间'}, {time_source})", flush=True)
                    continue

                # 文件在 24 小时内更新，下载并提取
                file_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{item_path}"
                print(f"[{now_str()}] 🔍 下载: {file_url} "
                      f"({file_time.strftime('%Y-%m-%d %H:%M:%S')}, {time_source})", flush=True)

                file_resp = safe_get(file_url, self.headers, timeout=(10, 30))
                if not file_resp:
                    continue

                parse_start = time.time()
                try:
                    proxies = extract_and_parse(file_resp.text, source_url=file_url)
                except Exception as e:
                    print(f"[{now_str()}] ⚠️ 解析失败 {file_url}: {e}", flush=True)
                    continue

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
        """保存 no.txt、分片、no_li.txt"""
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
