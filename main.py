"""主入口，配置关键词列表并启动收集器，最后调用测速"""

import os
from collector import Collector
from tester import run_full_test

QUERIES = [

    # ==================== 4. 中文高频 ====================
    "免费节点",
    "免费clash订阅",
    "免费v2ray订阅",
    "免费机场节点",
    "节点订阅",
    "免费机场",
    "科学上网",
    "梯子",
    "代理",
    "免费ssr节点",
    "免费vless节点",
    "免费reality节点",
    "免费tuic节点",
    "免费singbox节点",
    "免费翻墙",
    "公益节点",
    "节点分享",
    "节点仓库",
    "每日节点",
    "免费节点合集",

]

if __name__ == "__main__":
    token = os.getenv("GITHUB_TOKEN", "")
    collector = Collector(token=token, queries=QUERIES)
    collector.run()

    # 提取节点字符串列表
    node_strings = list(collector.unique_nodes)
    if node_strings:
        run_full_test(node_strings)
    else:
        print("无节点可供测速")
