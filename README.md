project/
├── main.py                  # 主入口，调度整个流程
├── collector.py             # GitHub 搜索、仓库处理、文件树遍历
├── parsers.py               # 节点提取器集合（策略模式）
├── proxy_model.py           # 统一数据模型 StandardProxy
├── utils.py                 # 工具函数（safe_get、base64解码、超时装饰器等）
└── .github/workflows/collect.yml  # GitHub Actions 工作流
