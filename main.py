"""主入口：动态生成搜索关键词，启动收集与测速。"""

import os
import time
from datetime import datetime, timezone, timedelta
from collector import Collector
from tester import run_full_test
from config import BASE_QUERIES, SEARCH_SUFFIX, SEARCH_IN

# 生成精确到小时的 pushed 限定词
UTC_NOW = datetime.now(timezone.utc)
TIME_LIMIT = (UTC_NOW - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')
TIME_SUFFIX = f"pushed:>{TIME_LIMIT}"

# 构建最终搜索查询列表
QUERIES = []
for q in BASE_QUERIES:
    # 如果配置了搜索范围，拼接 "in:name,description" 等
    if SEARCH_IN:
        final_query = f"{q} in:{SEARCH_IN} {TIME_SUFFIX}{SEARCH_SUFFIX}"
    else:
        final_query = f"{q} {TIME_SUFFIX}{SEARCH_SUFFIX}"
    QUERIES.append(final_query)

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
