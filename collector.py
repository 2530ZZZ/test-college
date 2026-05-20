"""
GitHub 节点收集器 —— 负责搜索仓库、遍历文件树、提取节点。
采用类封装，保留黑名单、仓库 SHA 缓存、文件 SHA 缓存、分片等全部功能。
现已适配新版 parsers.py，直接产出 StandardProxy 并转为去重字符串集合。
"""

import os
import time
import shutil
from datetime import datetime, timezone, timedelta
from typing import List, Set, Dict, Tuple, Optional
from email.utils import parsedate_to_datetime

import utils
from utils import safe_get, now_str, timeout_decorator, beijing_tz
from parsers import extract_and_parse          # 新解析器：一次调用完成提取+解析
from proxy_model import StandardProxy


# 持久化文件路径
BLACKLIST_FILE = "ljck.txt"
REPO_CACHE_FILE = "repo_commit_cache.txt"
FILE_CACHE_FILE = "dr_commit_cache.txt"


class Collector:
    def __init__(self, token: str = "", queries: List[str] = None):
        self.token = token
        self.headers = {
            "Authorization": f"token {token}" if token else "",
            "User-Agent": "Mozilla/5.0 (compatible; FreeNodesCollector/3.0)"
        }
        self.queries = queries or []

        # 状态变量
        self.all_links: List[str] = []                     # 本次收集到的所有源文件 raw URL
        self.unique_nodes: Set[str] = set()                # 去重后的节点字符串（raw_link 或 json）
        self.seen_repos: Set[str] = set()                  # 已检查过的仓库（避免重复处理）
        self.blacklist_repos: Set[str] = set()             # 永久黑名单仓库
        self.checked_count: int = 0                        # 已检查仓库总数

        # 缓存：仓库 + 文件
        # repo_cache: key = GitHub 仓库完整URL, value = (commit_sha, last_update_utc)
        self.repo_cache: Dict[str, Tuple[str, datetime]] = {}
        # file_cache: key = 文件 raw URL, value = (commit_sha, last_processed_utc)
        self.file_cache: Dict[str, Tuple[str, datetime]] = {}

        # 加载持久化数据
        self.load_blacklist()
        self.load_repo_cache()
        self.load_file_cache()

    # ==================== 主流程 ====================
    def run(self):
        """启动收集流程"""
        print(f"[{now_str()}] 🚀 程序启动，开始动态搜索...", flush=True)
        start_time = time.time()

        for idx, query in enumerate(self.queries, 1):
            print(f"\n[{now_str()}] 🔎 搜索 {idx}/{len(self.queries)}: {query}", flush=True)
            self.search_query(query)

        # 保存结果（同时清理过期缓存并写入文件）
        self.save_results()

        elapsed = time.time() - start_time
        print(f"\n[{now_str()}] 🎉 收集完成，总耗时 {elapsed:.0f} 秒", flush=True)
        print(f"  检查仓库数: {self.checked_count}")
        print(f"  收集到源链接数: {len(self.all_links)}")
        print(f"  去重节点总数: {len(self.unique_nodes)}")

    def search_query(self, query: str, max_pages: int = 10):
        """对单个关键词进行多页搜索"""
        query_links_count = 0
        for page in range(1, max_pages + 1):
            url = (f"https://api.github.com/search/repositories"
                   f"?q={query}&sort=updated&order=desc&per_page=100&page={page}")
            resp = safe_get(url, self.headers, timeout=(15, 30),
                            operation_name=f"搜索第{page}页")
            if not resp:
                break

            items = resp.json().get("items", [])
            if not items:
                break

            print(f"[{now_str()}] 第{page}页找到 {len(items)} 个仓库", flush=True)
            for item in items:
                repo = item["full_name"]
                github_url = f"https://github.com/{repo}"
                if repo in self.seen_repos or github_url in self.blacklist_repos:
                    continue
                self.seen_repos.add(repo)
                self.checked_count += 1
                self.process_repo(repo)
                time.sleep(1.2)  # 控制请求频率

            page += 1
            time.sleep(6)  # 翻页冷却

        print(f"[{now_str()}] └─ 本关键词贡献 {query_links_count} 条有效链接", flush=True)

    # ==================== 仓库处理 ====================
    @timeout_decorator(60)
    def process_repo(self, repo: str):
        """
        处理单个仓库：
        1. 检查黑名单
        2. 获取默认分支
        3. 检查仓库整体 commit 是否在 24 小时内
        4. 利用仓库缓存跳过无新提交的仓库
        5. 递归遍历文件树
        """
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

        # 获取仓库最近一次 commit
        commit_resp = safe_get(f"https://api.github.com/repos/{repo}/commits?per_page=1", self.headers)
        if not commit_resp:
            return
        try:
            commits = commit_resp.json()
            if not commits:
                return
            commit_data = commits[0]
            commit_sha = commit_data["sha"]
            commit_time_str = commit_data["commit"]["committer"]["date"]
            commit_time = datetime.fromisoformat(commit_time_str.replace("Z", "+00:00"))

            # 超过 24 小时直接丢弃
            if datetime.now(timezone.utc) - commit_time >= timedelta(hours=24):
                return

            # 利用仓库缓存：如果 sha 未变且缓存时间还在 24 小时内，则跳过
            if github_url in self.repo_cache:
                cached_sha, cached_time = self.repo_cache[github_url]
                if (cached_sha == commit_sha and
                        datetime.now(timezone.utc) - cached_time < timedelta(hours=24)):
                    print(f"  [{now_str()}] 仓库 {repo} 无新提交（缓存命中），跳过", flush=True)
                    return

        except Exception as e:
            print(f"  [{now_str()}] 处理仓库 {repo} 异常: {e}", flush=True)
            return

        # 更新仓库缓存（记录当前 sha 与当前时间）
        self.repo_cache[github_url] = (commit_sha, datetime.now(timezone.utc))

        # 处理文件树，收集节点
        has_nodes_flag = [False]
        self.process_file_tree(repo, "", default_branch, has_nodes_flag)

        # 如果未提取到任何节点，加入黑名单
        if not has_nodes_flag[0] and github_url not in self.blacklist_repos:
            print(f"  [{now_str()}] 仓库 {repo} 未提取到节点，加入黑名单", flush=True)
            self.blacklist_repos.add(github_url)
            with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
                f.write(github_url + "\n")

    # ==================== 文件树遍历（核心） ====================
    def process_file_tree(self, repo: str, path: str, branch: str, has_nodes: List[bool]):
        """
        递归处理文件树，只进入 24 小时内更新的目录，只下载 24 小时内更新的文件。
        同时使用文件缓存（dr_commit_cache）避免重复处理相同 commit 的文件。
        """
        contents_url = (f"https://api.github.com/repos/{repo}/contents/{path}"
                        if path else f"https://api.github.com/repos/{repo}/contents")
        resp = safe_get(contents_url, self.headers, timeout=(10, 20),
                        operation_name=f"Contents API {path or '根'}")
        if not resp:
            return

        items = resp.json()
        for item in items:
            item_path = item["path"]
            item_type = item["type"]  # "file" 或 "dir"

            # ---------- 获取该条目的最近 commit 时间 ----------
            file_time = None
            time_source = None
            file_commit_sha = None

            # 方法1：通过 Commits API 获取
            commit_url = f"https://api.github.com/repos/{repo}/commits?path={item_path}&per_page=1"
            c_resp = safe_get(commit_url, self.headers, timeout=(8, 12),
                              operation_name=f"commit 查询 {item_path}")
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

            # 方法2：备选 HEAD 请求 Last-Modified（仅文件）
            if file_time is None and item_type == "file":
                head_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{item_path}"
                head_resp = safe_get(head_url, self.headers, timeout=(8, 10),
                                     operation_name=f"HEAD 请求 {item_path}", max_retries=1)
                if head_resp and head_resp.status_code == 200:
                    last_mod = head_resp.headers.get('Last-Modified')
                    if last_mod:
                        try:
                            file_time = parsedate_to_datetime(last_mod).replace(tzinfo=timezone.utc)
                            time_source = "Last-Modified"
                        except Exception:
                            pass

            # 无法获取修改时间
            if file_time is None:
                if item_type == "dir":
                    print(f"   ➡️ 进入目录 {item_path}（无法获取修改时间）", flush=True)
                    self.process_file_tree(repo, item_path, branch, has_nodes)
                else:
                    print(f"   ⏭️ 跳过文件 {item_path}：无法获取修改时间", flush=True)
                continue

            # 检查是否在 24 小时内
            if datetime.now(timezone.utc) - file_time >= timedelta(hours=24):
                print(f"   ⏭️ 跳过 {item_path}：最后更新超过 24 小时 "
                      f"({file_time.strftime('%Y-%m-%d %H:%M')}, 来源：{time_source})", flush=True)
                if item_type == "dir":
                    print(f"   🚫 目录 {item_path} 过期，跳过递归", flush=True)
                continue

            # ---------- 24 小时内更新 ----------
            print(f"   ✅ {item_path} 在 24 小时内更新 "
                  f"({file_time.strftime('%Y-%m-%d %H:%M')}, 来源：{time_source})", flush=True)

            if item_type == "dir":
                self.process_file_tree(repo, item_path, branch, has_nodes)
            elif item_type == "file":
                file_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{item_path}"
                # ---------- 文件缓存检查 ----------
                if file_commit_sha is not None:
                    if file_url in self.file_cache:
                        cached_sha, cached_time = self.file_cache[file_url]
                        if (cached_sha == file_commit_sha and
                                datetime.now(timezone.utc) - cached_time < timedelta(hours=24)):
                            print(f"   ⏭️ 跳过文件 {file_url}：文件缓存命中（{file_commit_sha[:7]}）", flush=True)
                            continue
                # 下载文件并提取节点
                print(f"   🔍 检查文件: {file_url}", flush=True)
                file_resp = safe_get(file_url, self.headers, timeout=(10, 30))
                if not file_resp:
                    continue

                # 使用新解析器一次性提取并解析
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
                    print(f"   📄 {file_url} ✅ 新增 {len(new_nodes)} 条节点", flush=True)
                else:
                    print(f"   📄 {file_url} ❌ 无新节点", flush=True)

                # 更新文件缓存（记录 commit sha 和当前时间）
                if file_commit_sha is not None:
                    self.file_cache[file_url] = (file_commit_sha, datetime.now(timezone.utc))

    # ==================== 持久化加载/保存 ====================
    def load_blacklist(self):
        """加载永久黑名单 ljck.txt"""
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("https://github.com/"):
                        self.blacklist_repos.add(line)
            print(f"[{now_str()}] 已加载 ljck.txt，黑名单仓库 {len(self.blacklist_repos)} 个", flush=True)

    def load_repo_cache(self):
        """加载仓库缓存 repo_commit_cache.txt，格式：url sha timestamp"""
        if os.path.exists(REPO_CACHE_FILE):
            with open(REPO_CACHE_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(maxsplit=2)
                    if len(parts) >= 2:
                        url = parts[0]
                        sha = parts[1]
                        timestamp = None
                        if len(parts) >= 3:
                            try:
                                timestamp = datetime.fromisoformat(parts[2])
                            except ValueError:
                                pass
                        if timestamp is None:
                            timestamp = datetime.min.replace(tzinfo=timezone.utc)
                        if url.startswith("https://github.com/"):
                            self.repo_cache[url] = (sha, timestamp)
            print(f"[{now_str()}] 已加载 repo 缓存 {len(self.repo_cache)} 条", flush=True)

    def load_file_cache(self):
        """加载文件缓存 dr_commit_cache.txt，格式：raw_url sha timestamp"""
        if os.path.exists(FILE_CACHE_FILE):
            with open(FILE_CACHE_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(maxsplit=2)
                    if len(parts) >= 2:
                        url = parts[0]
                        sha = parts[1]
                        timestamp = None
                        if len(parts) >= 3:
                            try:
                                timestamp = datetime.fromisoformat(parts[2])
                            except ValueError:
                                pass
                        if timestamp is None:
                            timestamp = datetime.min.replace(tzinfo=timezone.utc)
                        # 仅接受 raw.githubusercontent.com 的 URL
                        if "raw.githubusercontent.com" in url:
                            self.file_cache[url] = (sha, timestamp)
            print(f"[{now_str()}] 已加载 file 缓存 {len(self.file_cache)} 条", flush=True)

    def clean_cache(self, cache: Dict[str, Tuple[str, datetime]]) -> Dict[str, Tuple[str, datetime]]:
        """清理超过 24 小时的缓存条目，返回新的字典"""
        now = datetime.now(timezone.utc)
        cleaned = {}
        for key, (sha, ts) in cache.items():
            if now - ts < timedelta(hours=24):
                cleaned[key] = (sha, ts)
        return cleaned

    def save_repo_cache(self):
        """写入仓库缓存（先清理过期）"""
        cleaned = self.clean_cache(self.repo_cache)
        with open(REPO_CACHE_FILE, "w", encoding="utf-8") as f:
            for url, (sha, ts) in cleaned.items():
                f.write(f"{url} {sha} {ts.isoformat()}\n")

    def save_file_cache(self):
        """写入文件缓存（先清理过期）"""
        cleaned = self.clean_cache(self.file_cache)
        with open(FILE_CACHE_FILE, "w", encoding="utf-8") as f:
            for url, (sha, ts) in cleaned.items():
                f.write(f"{url} {sha} {ts.isoformat()}\n")

    def save_results(self):
        """保存所有结果并持久化缓存（包含过期清理）"""
        # 1. 保存 no.txt
        if self.unique_nodes:
            with open("no.txt", "w", encoding="utf-8") as f:
                f.write("\n".join(self.unique_nodes))
            print(f"\n[{now_str()}] 已保存 {len(self.unique_nodes)} 条节点到 no.txt", flush=True)

        # 2. 分片到 no/ 文件夹，生成 no_w_li.txt
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
            raw_url = f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/no/{filename}"
            no_w_links.append(raw_url)

        with open("no_w_li.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(no_w_links))
        print(f"[{now_str()}] 已生成 no_w_li.txt ({file_count} 个分片)", flush=True)

        # 3. 保存 no_li.txt（包含 no.txt 自身的 raw 链接）
        no_txt_raw = f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/no.txt"
        self.all_links.append(no_txt_raw)
        self.all_links = list(dict.fromkeys(self.all_links))  # 去重
        with open("no_li.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(self.all_links))

        # 4. 清理并保存缓存
        self.save_repo_cache()
        self.save_file_cache()
        print(f"[{now_str()}] 缓存已更新（已清理超过 24 小时的记录）", flush=True)
