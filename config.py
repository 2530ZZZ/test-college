"""
全局配置 —— 集中管理所有可调参数和外部工具版本。
mihomo 使用 GitHub API 自动获取最新稳定版。

设计原则：
  - 所有可调参数集中在此文件，外部模块通过 import 引用。
  - 每个参数均有详细注释，说明含义、类型、默认值、是否可为空、如何设置为空以及调整建议。
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

# ==================== mihomo 连接与路径 ====================
MIHOMO_URL = (
    f"https://github.com/MetaCubeX/mihomo/releases/download/"
    f"{MIHOMO_VERSION}/mihomo-linux-amd64-{MIHOMO_VERSION}.gz"
)
MIHOMO_BIN = "mihomo"
MIXED_PORT = 7890
API_PORT = 9090

# ==================== 测速目标 URL ====================
LATENCY_TEST_URL = "https://www.gstatic.com/generate_204"
SPEED_TEST_URL = "https://speed.cloudflare.com/__down?bytes=10485760"

# ==================== 测速超时与过滤阈值 ====================
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
MAX_FILE_SIZE = None
FILE_PROCESS_TIMEOUT = 30
ALLOWED_EXTENSIONS = {
    '.yaml', '.yml', '.json', '.txt', '.md', '.conf', '.list', '.base64', ''
}
BLACKLIST_FILE = "ljck.txt"

# 是否检查文件的 Last-Modified 时间（24小时内）
# 类型：bool；默认值：True。
# 设为 True 时：只下载 24 小时内修改过的文件，节省 API 和带宽。
# 设为 False 时：对仓库内所有符合扩展名的文件进行下载（只要 SHA 未被处理过）。
CHECK_FILE_MODIFICATION_TIME = True

# ==================== HEAD 请求优化参数 ====================
HEAD_CONCURRENCY = 20
MAX_HEAD_PER_REPO = None
MIN_FILES_FOR_CONCURRENCY = 50

# ==================== API 请求超时（秒） ====================
SEARCH_TIMEOUT = (15, 30)
REPO_INFO_TIMEOUT = (8, 15)
FILE_DOWNLOAD_TIMEOUT = (10, 30)
CONTENTS_API_TIMEOUT = (10, 20)
COMMITS_API_TIMEOUT = (8, 12)
TREE_API_TIMEOUT = (12, 20)

# ==================== 树 API 策略 ====================
USE_RECURSIVE_TREE = True

# ==================== 搜索关键词与限定符 ====================

# --- 基础关键词列表（中英文全覆盖） ---
BASE_QUERIES = [

    # 英文
    "free nodes in:name,description",
    "free v2ray nodes in:name,description",
    "free clash nodes in:name,description",
    "free trojan nodes in:name,description",
    "free proxy list in:name,description",
    "free proxy subscription in:name,description",
    "subconverter in:name,description",
    "ACL4SSR in:name,description",
    "v2rayN in:name,description",
    "mihomo in:name,description",
    "Clash.Meta in:name,description",
    "Shadowrocket in:name,description",
    "Hiddify in:name,description",
    "Nekoray in:name,description",
    "ProxyCollector in:name,description",
    "TelegramV2rayCollector in:name,description",
    "free proxy scraper in:name,description",
    "free proxy bot in:name,description",
]

# --- 否定关键词列表 ---
SEARCH_NEGATIVE_KEYWORDS = [
    "adblock", "adguard", "filter", "blocklist", "domain",
    "asn", "iptv", "dns", "geosite", "geoip", "firewall",
    "malware", "phishing", "tracker", "spam", "telemetry",
    "crypto", "mining", "scraper",
    "飞鸟加速", "星辰VPN",
]

# 是否包含 fork 仓库
SEARCH_FORK = True
# 仓库大小范围（空字符串表示不限制）
SEARCH_SIZE_RANGE = ""
# 是否排除已归档仓库
SEARCH_ARCHIVED = False

# 搜索范围限定（已被直接写进BASE_QUERIES，此处保留为空）
SEARCH_IN = ""

# 组装固定后缀（不含 pushed 时间）
SEARCH_SUFFIX = ""
if SEARCH_FORK:
    SEARCH_SUFFIX += " fork:true"
if SEARCH_SIZE_RANGE:
    SEARCH_SUFFIX += f" size:{SEARCH_SIZE_RANGE}"
if SEARCH_ARCHIVED:
    SEARCH_SUFFIX += " archived:false"
for kw in SEARCH_NEGATIVE_KEYWORDS:
    SEARCH_SUFFIX += f" -{kw}"

# ==================== 输出文件 ====================
CHUNK_SIZE = 10000
ALIVE_NODE_FILE = "alive.txt"
MIHOMO_OUTPUT_FILE = "mihomo.yaml"
MIHOMO_TEMPLATE_FILE = "new.yaml"
