"""
全局配置 —— 集中管理所有可调参数和外部工具版本。
mihomo 使用 GitHub API 自动获取最新稳定版。

设计原则：
  - 所有可调参数集中在此文件，外部模块通过 import 引用。
  - 每个参数均有详细注释，说明含义、默认值、调整建议。
  - 版本号自动获取，失败时回退到硬编码的稳定版本。
  - 本模块不再依赖 utils 模块，避免循环导入。
"""

import os
import json
import requests
from datetime import datetime, timezone, timedelta


# ==================== 内部工具 ====================

_BEIJING_TZ = timezone(timedelta(hours=8))

def _now_str() -> str:
    """返回北京时间字符串，用于模块内日志。"""
    return datetime.now(_BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')


# ==================== 自动获取最新版本 ====================

def _fetch_latest_mihomo_version() -> str:
    url = "https://github.com/MetaCubeX/mihomo/releases/latest/download/version.txt"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        version = resp.text.strip()
        if version:
            print(f"[{_now_str()}] 获取到 mihomo 最新版本: {version}", flush=True)
            return version
    except Exception as e:
        print(f"[{_now_str()}] 获取 mihomo 最新版本失败: {e}，回退到 v1.18.7", flush=True)
    return "v1.18.7"

MIHOMO_VERSION = os.getenv("MIHOMO_VERSION") or _fetch_latest_mihomo_version()

# ==================== mihomo 配置 ====================
MIHOMO_URL = (
    f"https://github.com/MetaCubeX/mihomo/releases/download/"
    f"{MIHOMO_VERSION}/mihomo-linux-amd64-{MIHOMO_VERSION}.gz"
)
MIHOMO_BIN = "mihomo"
MIXED_PORT = 7890
API_PORT = 9090

# ==================== 测速 URL ====================
LATENCY_TEST_URL = "https://www.gstatic.com/generate_204"
SPEED_TEST_URL = "https://speed.cloudflare.com/__down?bytes=10485760"

# ==================== 测速超时与阈值 ====================
LATENCY_TIMEOUT = 5000
SPEED_TIMEOUT = 15
MIN_DOWNLOAD_BYTES = 5 * 1024 * 1024
MAX_LATENCY = 3000
MIN_SPEED_MB = 0.5

# ==================== TCP 端口预筛选 ====================
TCP_SCAN_ENABLED = True
TCP_SCAN_TIMEOUT = 1.5
TCP_SCAN_WORKERS = 200

# ==================== 测速分批 ====================
TEST_BATCH_SIZE = 3000

# ==================== GitHub 搜索配置 ====================
MAX_PAGES = 3
PER_PAGE = 30
REPO_SLEEP_SECONDS = 0.5
PAGE_SLEEP_SECONDS = 2
REPO_TIMEOUT_SECONDS = 120

# ==================== 限流控制 ====================
MAX_TOTAL_RATE_LIMIT_WAIT = 600

# ==================== 文件收集配置 ====================
MAX_FILE_SIZE = None               # None 则不限制
FILE_PROCESS_TIMEOUT = 30          # 单文件处理超时（秒），用于控制正则执行
ALLOWED_EXTENSIONS = {
    '.yaml', '.yml', '.json', '.txt', '.md', '.conf', '.list', '.base64'
}
BLACKLIST_FILE = "ljck.txt"

# ==================== API 请求超时 ====================
SEARCH_TIMEOUT = (15, 30)
REPO_INFO_TIMEOUT = (8, 15)
FILE_DOWNLOAD_TIMEOUT = (10, 30)
CONTENTS_API_TIMEOUT = (10, 20)
COMMITS_API_TIMEOUT = (8, 12)
TREE_API_TIMEOUT = (12, 20)

# ==================== 树 API 策略 ====================
# 是否优先使用 git/trees?recursive=1 一次性获取文件树，而不是逐层 Contents API
USE_RECURSIVE_TREE = True

# ==================== 输出文件 ====================
CHUNK_SIZE = 10000
ALIVE_NODE_FILE = "alive.txt"
MIHOMO_OUTPUT_FILE = "mihomo.yaml"
MIHOMO_TEMPLATE_FILE = "new.yaml"
