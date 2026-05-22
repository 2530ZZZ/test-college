project/

├── main.py                  # 主入口

├── config.py                # 全局配置（Sub-Store/mihomo版本/测速参数）

├── collector.py             # GitHub 搜索 + 文件树遍历 + 节点提取

├── parsers.py               # 纯文本提取（不再负责协议解析）

├── tester.py                # TCP预筛选 + mihomo延迟测试 + 下载速度测试

├── utils.py                 # 通用工具函数

└── .github/workflows/collect.yml  # GitHub Actions 工作流
