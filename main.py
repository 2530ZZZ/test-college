"""主入口，配置关键词列表并启动收集器"""

from collector import Collector
from tester import run_full_test

QUERIES = [

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
    import os
    token = os.getenv("GITHUB_TOKEN", "")
    collector = Collector(token=token, queries=QUERIES)
    collector.run()
    # ... 收集过程 ...
    collector.run()

    # 收集到的节点原始字符串列表
    all_proxy_lines = list(collector.unique_nodes)

    # 将字符串列表转换为 StandardProxy 对象列表（需要解析器）
    from parsers import parse_line
    proxies = []
    for line in all_proxy_lines:
        p = parse_line(line)
        if p:
            proxies.append(p)

    # 启动测速模块
    if proxies:
        run_full_test(proxies)
    else:
        print("无节点可供测速")
