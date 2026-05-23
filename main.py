"""主入口：动态生成搜索关键词，启动收集与测速。"""

import os
import time
from datetime import datetime, timezone, timedelta
from collector import Collector
from tester import run_full_test

# 使用最近 24 小时的精确时间戳
UTC_NOW = datetime.now(timezone.utc)
TIME_LIMIT = (UTC_NOW - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')
TIME_SUFFIX = f"pushed:>{TIME_LIMIT}"

BASE_QUERIES = [
    "免费节点", "免费clash订阅", "免费v2ray订阅", "免费机场节点",
    "节点订阅", "免费机场", "科学上网", "代理",
    "免费ssr节点", "免费vless节点", "免费reality节点", "免费tuic节点", "免费singbox节点",
    "公益节点", "节点分享", "节点仓库", "每日节点", "免费节点合集",
    "clash订阅", "v2ray订阅", "trojan订阅", "hysteria2订阅",
    "free nodes", "free v2ray nodes", "free clash nodes", "free trojan nodes",
    "free proxy list", "free proxy subscription",
    "subconverter", "ACL4SSR", "v2rayN", "mihomo", "Clash.Meta",
    "Shadowrocket", "Hiddify", "Nekoray",
    "ProxyCollector", "TelegramV2rayCollector",
    "free proxy scraper", "free proxy bot",
]

QUERIES = [f"{q} {TIME_SUFFIX}" for q in BASE_QUERIES]

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
