"""
主入口：动态生成搜索关键词，启动收集与测速。
所有日志同时输出到控制台和 log/ 文件夹，保留最近 10 个日志文件。
"""

import os, sys, time, glob
from datetime import datetime, timezone, timedelta
from collector import Collector
from tester import run_full_test
from config import BASE_QUERIES, SEARCH_SUFFIX, SEARCH_IN

# ---------- 日志持久化 ----------
LOG_DIR = "log"
os.makedirs(LOG_DIR, exist_ok=True)

# 生成带时间戳的日志文件名
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

log_file = open(log_path, "a", encoding="utf-8")
sys.stdout = Tee(log_file, sys.__stdout__)
sys.stderr = Tee(log_file, sys.__stderr__)

# 清理旧日志，只保留最近 10 个
existing_logs = sorted(glob.glob(os.path.join(LOG_DIR, "collect_*.log")), key=os.path.getctime)
while len(existing_logs) > 10:
    os.remove(existing_logs[0])
    existing_logs.pop(0)

# ---------- 搜索构建 ----------
UTC_NOW = datetime.now(timezone.utc)
TIME_LIMIT = (UTC_NOW - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')
TIME_SUFFIX = f"pushed:>{TIME_LIMIT}"

QUERIES = []
for q in BASE_QUERIES:
    if SEARCH_IN:
        query_body = f"{q} in:{SEARCH_IN} {TIME_SUFFIX} {SEARCH_SUFFIX}"
    else:
        query_body = f"{q} {TIME_SUFFIX} {SEARCH_SUFFIX}"
    QUERIES.append(query_body)

if __name__ == "__main__":
    start_time = time.time()
    token = os.getenv("GITHUB_TOKEN", "")
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] 🚀 程序启动，时间基准：{TIME_LIMIT}", flush=True)

    collector = Collector(token=token, queries=QUERIES)
    collector.run()

    node_strings = list(collector.unique_nodes)
    if node_strings:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] 🔍 开始测速...", flush=True)
        run_full_test(node_strings)
    else:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ 无节点可供测速", flush=True)

    elapsed = time.time() - start_time
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] 🎉 全部完成，总耗时 {elapsed:.1f} 秒", flush=True)
    log_file.close()
