"""
主入口 — 多源搜集节点，输出 alive.txt。

架构：
  搜集线程 (Collector) — 并行多通道（GitHub / Web）
  → 边搜集边持久化 → 输出 alive.txt

所有日志同时输出到控制台和 log/ 文件夹。
"""

import os
import sys
import time
import glob
from datetime import datetime, timezone, timedelta

from collector import Collector
from output import save_alive_nodes
from utils import now_str
from config import (
    GITHUB_TOKEN, BASE_QUERIES, SEARCH_SUFFIX, SEARCH_IN,
    LOG_DIR, MAX_LOG_FILES,
)


# ==================== 日志持久化 ====================

os.makedirs(LOG_DIR, exist_ok=True)
log_filename = f"collect_{datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')}.log"
log_path = os.path.join(LOG_DIR, log_filename)


class Tee:
    """同时输出到原始 stdout/stderr 和日志文件。"""
    def __init__(self, file, stream):
        self.file = file
        self.stream = stream

    def write(self, data):
        self.file.write(data)
        self.file.flush()
        self.stream.write(data)
        self.stream.flush()

    def flush(self):
        self.file.flush()
        self.stream.flush()

    def close(self):
        self.flush()
        if self.file:
            self.file.close()


log_file = open(log_path, "a", encoding="utf-8")
sys.stdout = Tee(log_file, sys.__stdout__)
sys.stderr = Tee(log_file, sys.__stderr__)

existing_logs = sorted(
    glob.glob(os.path.join(LOG_DIR, "collect_*.log")),
    key=os.path.getctime
)
while len(existing_logs) > MAX_LOG_FILES:
    os.remove(existing_logs[0])
    existing_logs.pop(0)


# ==================== 搜索关键词构建 ====================

def build_queries():
    """动态构建搜索关键词列表。"""
    utc_now = datetime.now(timezone.utc)
    time_limit = (utc_now - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')
    time_suffix = f"pushed:>{time_limit}"
    queries = []
    for q in BASE_QUERIES:
        if SEARCH_IN:
            query_body = f"{q} in:{SEARCH_IN} {time_suffix} {SEARCH_SUFFIX}"
        else:
            query_body = f"{q} {time_suffix} {SEARCH_SUFFIX}"
        queries.append(query_body)
    return queries


# ==================== 主流程 ====================

def main():
    start_time = time.time()
    queries = build_queries()
    print(f"[{now_str()}] 🚀 程序启动")
    print(f"[{now_str()}] 关键词: {len(queries)} 个", flush=True)

    collector = Collector(token=GITHUB_TOKEN, queries=queries)
    collector.run()

    # 输出 alive.txt（全部搜集到的节点）
    alive_uris = list(collector.unique_nodes)
    save_alive_nodes(alive_uris)

    total_elapsed = time.time() - start_time
    print(f"[{now_str()}] 🎉 全部完成，总耗时 {total_elapsed:.1f} 秒", flush=True)

    # 恢复原始 stdout/stderr，避免 Python 退出时 flush 已关闭的 Tee 导致 exit code 120
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    log_file.close()


if __name__ == "__main__":
    main()
