"""
GitHub 节点收集器 —— Contents API 逐层递归 + 目录/文件 SHA 内存去重。

所有可调参数（搜索页数、每页数量、请求间隔、限流阈值、文件大小限制等）
均统一由 config.py 管理，本模块通过 import 引用。

收集到的候选块后续由 mihomo 进行解析和测试。
新增累计限流监控，若限流等待超过 config.MAX_TOTAL_RATE_LIMIT_WAIT 则主动停止搜索。
"""

import os
import time
import shutil
from datetime import datetime, timezone, timedelta
from typing import List, Set, Optional

import utils
from utils import safe_get, now_str, timeout_decorator, beijing_tz, check_rate_limit
from parsers import extract_raw_candidates

# 从统一配置源导入所有可调参数
from config import (
    CHUNK_SIZE, MAX_PAGES, PER_PAGE, REPO_SLEEP_SECONDS, PAGE_SLEEP_SECONDS,
    REPO_TIMEOUT_SECONDS, MAX_FILE_SIZE, ALLOWED_EXTENSIONS, BLACKLIST_FILE,
    SEARCH_TIMEOUT, REPO_INFO_TIMEOUT, FILE_DOWNLOAD_TIMEOUT,
    CONTENTS_API_TIMEOUT, COMMITS_API_TIMEOUT,
)


class Collector:
    """
    GitHub 节点收集器。

    所有固定参数从 config.py 导入：
      - MAX_PAGES: 每个关键词最多搜索页数
      - PER_PAGE: 每页返回仓库数
      - REPO_SLEEP_SECONDS / PAGE_SLEEP_SECONDS: 请求间隔
      - REPO_TIMEOUT_SECONDS: 单仓库处理超时
      - MAX_FILE_SIZE: 下载文件大小上限
      - ALLOWED_EXTENSIONS: 允许处理的文件扩展名
      - BLACKLIST_FILE: 黑名单持久化文件路径
      - *_TIMEOUT: 各类 API 请求超时
    """

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
        """启动收集流程。若累计限流等待超过 config.MAX_TOTAL_RATE_LIMIT_WAIT，提前终止。"""
        print(f"[{now_str()}] 🚀 程序启动", flush=True)
        start_time = time.time()
        for idx, query in enumerate(self.queries, 1):
            # 检查限流状态：若全局累计等待已超过阈值，退出搜索循环
            # 阈值由 config.MAX_TOTAL_RATE_LIMIT_WAIT 定义（默认 600 秒）
            if utils.total_rate_limit_wait >= utils.MAX_TOTAL_RATE_LIMIT_WAIT:
                print(f"[{now_str()}] ⚠️ 累计限流等待已达 "
                      f"{utils.total_rate_limit_wait:.0f}s，终止搜索", flush=True)
                break
            q_start = time.time()
            print(f"\n[{now_str()}] 🔎 搜索 {idx}/{len(self.queries)}: {query}", flush=True)
            self.search_query(query)
            print(f"[{now_str()}]   关键词耗时: {time.time() - q_start:.1f}s", flush=True)

        self.save_results()
        elapsed = time.time() - start_time
        print(f"\n[{now_str()}] 🎉 收集完成，总耗时 {elapsed:.0f}s", flush=True)
        print(f"[{now_str()}] 检查仓库: {self.checked_count}, "
              f"源链接: {len(self.all_links)}, 节点: {len(self.unique_nodes)}", flush=True)
        print(f"[{now_str()}] 累计限流等待: {utils.total_rate_limit_wait:.0f} 秒", flush=True)

    def search_query(self, query: str):
        """
        对单个关键词进行多页搜索。

        参数说明（均来自 config.py）：
          - max_pages = MAX_PAGES（默认 3）：每关键词最多搜索页数
          - per_page = PER_PAGE（默认 30）：每页返回仓库数
          - page_sleep = PAGE_SLEEP_SECONDS（默认 2s）：翻页冷却间隔
          - repo_sleep = REPO_SLEEP_SECONDS（默认 0.5s）：仓库处理间隔
          - search_timeout = SEARCH_TIMEOUT（默认 (15, 30)s）：搜索请求超时

        GitHub Search API 限制：已认证用户 30 次/分钟。
        配置 3 页 × 40 关键词 = 120 次搜索，配合 PAGE_SLEEP_SECONDS 间隔，
        可在 4 分钟内平稳完成，不触发限流。
        """
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
                # 仓库处理间隔：避免触发 GitHub 二次限流
                # config.REPO_SLEEP_SECONDS 默认 0.5 秒
                time.sleep(REPO_SLEEP_SECONDS)
            # 翻页冷却：确保不超过 Search API 的 30 次/分钟限制
            # config.PAGE_SLEEP_SECONDS 默认 2 秒
            time.sleep(PAGE_SLEEP_SECONDS)

    @timeout_decorator(REPO_TIMEOUT_SECONDS)
    def process_repo(self, repo: str):
        """
        处理单个仓库。

        超时保护：
          - 由 config.REPO_TIMEOUT_SECONDS 控制（默认 120 秒）
          - 使用 @timeout_decorator 装饰器实现
          - 防止因大文件或网络问题导致单个仓库永久卡住

        处理流程：
          1. 获取仓库默认分支
          2. 递归遍历文件树（Contents API 逐层过滤）
          3. 未提取到节点的仓库加入黑名单
        """
        github_url = f"https://github.com/{repo}"
        if github_url in self.blacklist_repos:
            return

        # 获取仓库信息（获取默认分支）
        # 超时由 config.REPO_INFO_TIMEOUT 控制（默认 (8, 15)s）
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
        try:
            self.process_file_tree(repo, "", default_branch, has_nodes_flag)
        except RuntimeError:
            raise

        if not has_nodes_flag[0] and github_url not in self.blacklist_repos:
            print(f"[{now_str()}] 仓库 {github_url} 未提取到节点，加入黑名单", flush=True)
            self.blacklist_repos.add(github_url)
            with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
                f.write(github_url + "\n")

    def process_file_tree(self, repo: str, path: str, branch: str, has_nodes: List[bool]):
        """
        递归遍历文件树。

        过滤策略：
          1. 目录/文件 SHA 内存去重：相同 SHA 只处理一次
          2. 逐层 commits API 时间过滤：只深入 24 小时内更新的目录
          3. 文件扩展名过滤：config.ALLOWED_EXTENSIONS
          4. 文件大小过滤：config.MAX_FILE_SIZE（默认 1MB）

        超时参数（均来自 config.py）：
          - CONTENTS_API_TIMEOUT：目录内容请求超时
          - COMMITS_API_TIMEOUT：文件/目录修改时间查询超时
          - FILE_DOWNLOAD_TIMEOUT：文件下载超时
        """
        contents_url = (
            f"https://api.github.com/repos/{repo}/contents/{path}"
            if path else f"https://api.github.com/repos/{repo}/contents"
        )
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
                    print(f"[{now_str()}] ⏭️ 跳过目录 "
                          f"https://github.com/{repo}/tree/{branch}/{item_path} "
                          f"(SHA 已处理)", flush=True)
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
                # 文件大小保护：超过 config.MAX_FILE_SIZE 跳过
                if len(content) > MAX_FILE_SIZE:
                    print(f"[{now_str()}] ⚠️ 文件过大 ({len(content)} 字节)，跳过", flush=True)
                    continue

                candidates = extract_raw_candidates(content)
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
        """加载黑名单文件（持久化排除无效仓库）。"""
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("https://github.com/"):
                        self.blacklist_repos.add(line)
            print(f"[{now_str()}] 已加载黑名单: {len(self.blacklist_repos)} 个", flush=True)

    def save_results(self):
        """
        保存收集结果：
          - no.txt：去重后的节点列表
          - no/：分片文件（每 CHUNK_SIZE 个节点一个文件）
          - no_w_li.txt：分片文件的 raw 链接索引
          - no_li.txt：所有源文件链接
        """
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
