"""
全局配置 —— 集中管理所有可调参数和外部工具版本。
mihomo 使用 GitHub API 自动获取最新稳定版。

设计原则：
  - 所有可调参数集中在此文件，外部模块通过 import 引用。
  - 每个参数均有详细注释，说明含义、默认值、调整建议。
  - 版本号自动获取，失败时回退到硬编码的稳定版本。

config.py（唯一配置源）
  ├── MIHOMO_URL, MIHOMO_BIN, MIXED_PORT, API_PORT → tester.py
  ├── LATENCY_TEST_URL, SPEED_TEST_URL, *_TIMEOUT → tester.py
  ├── MAX_LATENCY, MIN_SPEED_MB, TCP_SCAN_* → tester.py
  ├── TEST_BATCH_SIZE → tester.py
  ├── MAX_TOTAL_RATE_LIMIT_WAIT → utils.py, collector.py
  ├── MAX_PAGES, PER_PAGE → collector.py
  ├── REPO_SLEEP_SECONDS, PAGE_SLEEP_SECONDS → collector.py
  ├── REPO_TIMEOUT_SECONDS → collector.py
  ├── MAX_FILE_SIZE, ALLOWED_EXTENSIONS, BLACKLIST_FILE → collector.py
  ├── SEARCH_TIMEOUT, REPO_INFO_TIMEOUT, FILE_DOWNLOAD_TIMEOUT,
  │   CONTENTS_API_TIMEOUT, COMMITS_API_TIMEOUT → collector.py
  ├── CHUNK_SIZE → collector.py
  ├── ALIVE_NODE_FILE, MIHOMO_OUTPUT_FILE, MIHOMO_TEMPLATE_FILE → tester.py
  └── MIHOMO_VERSION → 自动获取
"""

import os
import re
import json
import requests
from utils import now_str


# ==================== 自动获取最新版本 ====================

def _fetch_latest_mihomo_version() -> str:
    """
    从 mihomo 的 version.txt 端点获取最新稳定版本号。
    GitHub API 端点：
      GET https://github.com/MetaCubeX/mihomo/releases/latest/download/version.txt
    返回格式如 "v1.19.24"。
    若网络异常或 API 限流，回退到硬编码版本。
    """
    url = "https://github.com/MetaCubeX/mihomo/releases/latest/download/version.txt"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        version = resp.text.strip()
        if version:
            print(f"[{now_str()}] 获取到 mihomo 最新版本: {version}", flush=True)
            return version
    except Exception as e:
        print(f"[{now_str()}] 获取 mihomo 最新版本失败: {e}，回退到 v1.18.7", flush=True)
    return "v1.18.7"  # 回退版本：经过验证的稳定版


MIHOMO_VERSION = os.getenv("MIHOMO_VERSION") or _fetch_latest_mihomo_version()


# ==================== mihomo 连接与路径 ====================

# mihomo 二进制下载地址（根据自动获取的版本号拼接）
# 格式：https://github.com/MetaCubeX/mihomo/releases/download/{version}/mihomo-linux-amd64-{version}.gz
MIHOMO_URL = (
    f"https://github.com/MetaCubeX/mihomo/releases/download/"
    f"{MIHOMO_VERSION}/mihomo-linux-amd64-{MIHOMO_VERSION}.gz"
)

# 本地二进制文件名（解压后）
MIHOMO_BIN = "mihomo"

# mihomo HTTP 代理监听端口（用于速度测试时的本地代理）
MIXED_PORT = 7890

# mihomo External Controller API 端口（用于延迟测试和节点切换）
API_PORT = 9090


# ==================== 测速目标 URL ====================

# 延迟测试 URL：mihomo 通过代理访问此地址，测量 HTTP 往返时间
# 要求：全球可达、响应速度快、稳定不封 IP
LATENCY_TEST_URL = "https://www.gstatic.com/generate_204"

# 速度测试 URL：mihomo 通过代理下载此文件，测量下载速度
# 当前使用 Cloudflare 的 10MB 测试文件
SPEED_TEST_URL = "https://speed.cloudflare.com/__down?bytes=10485760"  # 10MB


# ==================== 测速超时与阈值 ====================

# 延迟测试超时（毫秒）
# mihomo API 的 timeout 参数，超过此值视为节点不可达
LATENCY_TIMEOUT = 5000

# 速度测试超时（秒）
# 下载测试文件的最大允许时间，超时视为测速失败
SPEED_TIMEOUT = 15

# 最小下载字节数（5MB）
# 下载量低于此值的节点视为测速失败（防止瞬时连接被误判为有效）
MIN_DOWNLOAD_BYTES = 5 * 1024 * 1024

# 最大允许延迟（毫秒）
# 延迟超过此值的节点将被过滤掉
MAX_LATENCY = 3000

# 最低允许速度（MB/s）
# 下载速度低于此值的节点将被过滤掉
MIN_SPEED_MB = 0.5


# ==================== TCP 端口预筛选 ====================

# 是否启用 TCP 端口预筛选
# 强烈建议开启，可以在 1-2 分钟内筛掉 70%-90% 的死节点
TCP_SCAN_ENABLED = True

# TCP 连接超时（秒）
# 单个端口的连接超时时间，建议 1-2 秒
TCP_SCAN_TIMEOUT = 1.5

# TCP 扫描并发线程数
# 提高此值可以加快预筛选速度，但过高可能导致系统资源耗尽
TCP_SCAN_WORKERS = 200


# ==================== 测速分批 ====================

# 每批送入 mihomo 的节点数
# mihomo 一次性加载过多节点会导致启动失败或配置文件过大
# 3000 是一个经过验证的安全值
TEST_BATCH_SIZE = 3000


# ==================== GitHub 搜索配置 ====================

# 每个关键词最多搜索的页数
# GitHub Search API 每页最多返回 100 条结果（已认证用户）
# 搜索结果按「最近更新」降序排列，前 3 页的相关性最高
# 调整为 3 页可在覆盖率和 API 消耗之间取得平衡
MAX_PAGES = 3

# 每页返回的仓库数
# GitHub Search API 上限为 100，但 30 可减少单次响应体积、加快响应速度
# 同时降低触发二次限流的风险
# 注意：GitHub Search API 对已认证用户限制为 30 次/分钟
PER_PAGE = 30

# 仓库处理间隔（秒）
# 每个仓库处理完后暂停的时间，避免触发 GitHub 的二次限流
REPO_SLEEP_SECONDS = 0.5

# 翻页冷却间隔（秒）
# 搜索页之间的强制冷却时间，确保不超过每分钟 30 次的 Search API 限制
PAGE_SLEEP_SECONDS = 2

# 单个仓库处理的超时时间（秒）
# 防止因大文件、死循环等问题导致单个仓库处理永久卡住
REPO_TIMEOUT_SECONDS = 120


# ==================== 限流控制 ====================

# 累计限流等待阈值（秒）
# 当整个运行过程中因触发 GitHub 限流而等待的总时间超过此值，
# 程序将主动终止搜索，直接对已收集的节点进行测速
# 设置为 600 秒（10 分钟），在实际运行中足够应对偶尔的限流
# 同时避免无限制等待
MAX_TOTAL_RATE_LIMIT_WAIT = 600


# ==================== 文件收集配置 ====================

# 最大允许下载的文件大小（字节）
# 超过此大小的文件将被跳过，避免下载大型日志、广告规则等无效文件
# 代理订阅文件通常只有几十 KB 到几百 KB
MAX_FILE_SIZE = 1_000_000  # 1MB

# 允许处理的文件扩展名
# 只处理这些类型的文件，减少对图片、二进制等无关文件的下载
ALLOWED_EXTENSIONS = {
    '.yaml', '.yml', '.json', '.txt', '.md', '.conf', '.list', '.base64'
}

# 黑名单文件路径
# 记录「未提取到任何节点」的仓库，持久化排除，避免重复处理
BLACKLIST_FILE = "ljck.txt"


# ==================== API 请求超时（秒） ====================

# 搜索请求超时：(连接超时, 读取超时)
# 搜索 API 响应较慢，需要更长的超时时间
SEARCH_TIMEOUT = (15, 30)

# 仓库信息请求超时
REPO_INFO_TIMEOUT = (8, 15)

# 文件内容请求超时
# 文件下载可能较慢，给予更长的超时
FILE_DOWNLOAD_TIMEOUT = (10, 30)

# Contents API 请求超时
CONTENTS_API_TIMEOUT = (10, 20)

# Commits API 请求超时
COMMITS_API_TIMEOUT = (8, 12)


# ==================== 输出文件 ====================

# 分片大小：no.txt 中每多少个节点分一个文件
CHUNK_SIZE = 10000

# 存活节点文件：测速后存活的节点 URI 列表
ALIVE_NODE_FILE = "alive.txt"

# mihomo 订阅输出文件：基于用户模板生成的订阅配置
MIHOMO_OUTPUT_FILE = "mihomo.yaml"

# mihomo 模板文件：用户编写的模板，测速后自动填充节点
MIHOMO_TEMPLATE_FILE = "new.yaml"
