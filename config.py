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
    """
    从 mihomo 官方获取最新稳定版本号。
    接口：https://github.com/MetaCubeX/mihomo/releases/latest/download/version.txt
    返回格式如 "v1.19.24"。失败时回退到 "v1.18.7"。
    类型：函数；不可为空。
    """
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
# MIHOMO_VERSION 类型：str；不可为空。可由环境变量 MIHOMO_VERSION 覆盖。

# ==================== mihomo 连接与路径 ====================

# mihomo 二进制下载地址（根据版本号拼接）
# 类型：str；不可为空。
MIHOMO_URL = (
    f"https://github.com/MetaCubeX/mihomo/releases/download/"
    f"{MIHOMO_VERSION}/mihomo-linux-amd64-{MIHOMO_VERSION}.gz"
)

# 本地解压后的可执行文件名
# 类型：str；默认值："mihomo"；不可为空。
MIHOMO_BIN = "mihomo"

# mihomo HTTP 代理监听端口（用于速度测试）
# 类型：int；默认值：7890；不可为空，建议不要更改，避免冲突。
MIXED_PORT = 7890

# mihomo External Controller API 端口（用于延迟测试和代理切换）
# 类型：int；默认值：9090；不可为空，建议不要更改。
API_PORT = 9090


# ==================== 测速目标 URL ====================

# 延迟测试 URL：通过代理访问此地址，测量 HTTP 往返时间（毫秒级）
# 类型：str；默认值："https://www.gstatic.com/generate_204"；不可为空。
# 建议：使用稳定且快速的全球可达 URL，如果被屏蔽可更换为其他 generate_204 端点。
LATENCY_TEST_URL = "https://www.gstatic.com/generate_204"

# 速度测试 URL：通过代理下载此文件来测量带宽
# 类型：str；默认值："https://speed.cloudflare.com/__down?bytes=10485760"（10MB）；
# 不可为空。如需更换，确保目标文件大小已知且支持断点续传无关紧要。
SPEED_TEST_URL = "https://speed.cloudflare.com/__down?bytes=10485760"


# ==================== 测速超时与过滤阈值 ====================

# 单节点延迟测试超时（毫秒）
# 类型：int；默认值：5000；不可为空。
# 超过此值视为节点不可达，mihomo API 内部控制。
LATENCY_TIMEOUT = 5000

# 单节点速度测试超时（秒）
# 类型：int；默认值：15；不可为空。
# 超过此时间未完成下载视为测速失败。
SPEED_TIMEOUT = 15

# 最小下载字节数（5MB）
# 类型：int；默认值：5 * 1024 * 1024；不可为空。
# 下载量低于此值视为测速失败，防止瞬时连接误判为有效。
MIN_DOWNLOAD_BYTES = 5 * 1024 * 1024

# 最大允许延迟（毫秒）
# 类型：int；默认值：3000；不可为空。
# 延迟超过此值的节点将被丢弃。
MAX_LATENCY = 3000

# 最低允许速度（MB/s）
# 类型：float；默认值：0.5；不可为空。
# 下载速度低于此值的节点将被丢弃。
MIN_SPEED_MB = 0.5


# ==================== TCP 端口预筛选 ====================

# 是否启用 TCP 端口连通性预筛选
# 类型：bool；默认值：True；不可为空。
# 强烈建议开启，可快速滤除 70%-90% 的死节点。
TCP_SCAN_ENABLED = True

# 单个端口连接超时（秒）
# 类型：float；默认值：1.5；不可为空。建议 1-2 秒。
TCP_SCAN_TIMEOUT = 1.5

# TCP 扫描并发线程数
# 类型：int；默认值：200；不可为空。可根据机器性能调整。
TCP_SCAN_WORKERS = 200


# ==================== 测速分批 ====================

# 每批送入 mihomo 的节点数
# 类型：int；默认值：3000；不可为空。
# 一次性加载过多节点会导致配置文件过大、mihomo 启动失败或内存溢出。
TEST_BATCH_SIZE = 3000


# ==================== GitHub 搜索配置 ====================

# 每个关键词最多搜索的页数（每页 PER_PAGE 条结果）
# 类型：int；默认值：3；不可为空。
# 搜索结果按最近更新排序，前 3 页通常已覆盖绝大多数有效仓库。
MAX_PAGES = 3

# 每页返回的仓库数
# 类型：int；默认值：30；不可为空。GitHub 上限 100，设为 30 可减少单次响应体积，降低限流风险。
PER_PAGE = 30

# 处理完一个仓库后的休眠时间（秒）
# 类型：float；默认值：0.5；不可为空。避免触发 GitHub 二次限流。
REPO_SLEEP_SECONDS = 0.5

# 搜索翻页冷却时间（秒）
# 类型：float；默认值：2；不可为空。确保不超过 Search API 的 30 次/分钟限制。
PAGE_SLEEP_SECONDS = 2

# 单个仓库的处理超时时间（秒）
# 类型：int；默认值：120；不可为空。防止大仓库或网络问题导致卡死。
REPO_TIMEOUT_SECONDS = 120


# ==================== 限流控制 ====================

# 累计限流等待阈值（秒）
# 类型：int；默认值：600（10分钟）；不可为空。
# 当整个运行过程中因触发 GitHub 限流而等待的总时间超过此值，
# 程序将主动终止搜索，直接进入测速阶段。
MAX_TOTAL_RATE_LIMIT_WAIT = 600


# ==================== 文件收集配置 ====================

# 最大允许下载的文件大小（字节）
# 类型：int 或 None；默认值：None（不限制）。
# 可设置为具体数值来限制。代理订阅文件通常几十 KB 到几 MB。
# 设置为 None 表示不限制。
MAX_FILE_SIZE = None

# 单个文件处理超时（秒）
# 类型：int 或 None；默认值：30。
# 用于控制正则提取的执行时间，防止复杂文件卡死。
# 设置为 None 表示不限制。
FILE_PROCESS_TIMEOUT = 30

# 允许处理的文件扩展名集合
# 类型：set；默认值：{'.yaml', '.yml', '.json', '.txt', '.md', '.conf', '.list', '.base64', ''}
# 空字符串代表无扩展名的文件（如 'all'）。如果不需要包含某类扩展名，直接删除对应元素即可。
# 不可为空集合（但可以是一个空 set，表示只允许无扩展名文件？建议至少保留一个有效扩展名）。
ALLOWED_EXTENSIONS = {
    '.yaml', '.yml', '.json', '.txt', '.md', '.conf', '.list', '.base64', ''
}

# 黑名单文件路径
# 类型：str；默认值："ljck.txt"；不可为空。
# 记录“未提取到任何节点”的仓库，跨运行持久化，避免重复处理无效仓库。
BLACKLIST_FILE = "ljck.txt"


# ==================== HEAD 请求优化参数 ====================

# HEAD 请求并发线程数
# 类型：int；默认值：20；不可为空。
# 增大可提高处理速度，但过高可能触发 raw 端点限流。
HEAD_CONCURRENCY = 20

# 单仓库最大 HEAD 请求数
# 类型：int 或 None；默认值：None（不限制）。
# 超过此数量的文件直接跳过，防止超大仓库拖慢整体进度。
# 设置为 None 表示不限制。
MAX_HEAD_PER_REPO = None

# 候选文件数阈值：低于此值时不启动并发 HEAD，直接串行处理，避免线程池开销
# 类型：int；默认值：50；不可为空。
MIN_FILES_FOR_CONCURRENCY = 50


# ==================== API 请求超时（秒） ====================

# 搜索请求超时：(连接超时, 读取超时)
# 类型：tuple；默认值：(15, 30)；不可为空。
SEARCH_TIMEOUT = (15, 30)

# 仓库信息请求超时：(连接超时, 读取超时)
# 类型：tuple；默认值：(8, 15)；不可为空。
REPO_INFO_TIMEOUT = (8, 15)

# 文件下载请求超时：(连接超时, 读取超时)
# 类型：tuple；默认值：(10, 30)；不可为空。
FILE_DOWNLOAD_TIMEOUT = (10, 30)

# Contents API 超时 (连接超时, 读取超时)
# 类型：tuple；默认值：(10, 20)；不可为空。
CONTENTS_API_TIMEOUT = (10, 20)

# Commits API 超时（回退方案时使用）(连接超时, 读取超时)
# 类型：tuple；默认值：(8, 12)；不可为空。
COMMITS_API_TIMEOUT = (8, 12)

# git/trees API 超时 (连接超时, 读取超时)
# 类型：tuple；默认值：(12, 20)；不可为空。
TREE_API_TIMEOUT = (12, 20)


# ==================== 树 API 策略 ====================

# 是否优先使用 git/trees?recursive=1 一次性获取文件树
# 类型：bool；默认值：True；不可为空。
# 开启后可大幅减少 API 调用，但超大仓库可能被截断（会触发回退）。
USE_RECURSIVE_TREE = True


# ==================== 搜索关键词与限定符 ====================

# --- 基础关键词列表 ---
# 类型：list of str；不可为空列表。
# 每个关键词将自动拼接搜索后缀，形成最终的 GitHub 搜索查询。
BASE_QUERIES = [
    # ==================== 1. 基础高频 ====================
    "free nodes",
    "free proxy nodes",
    "free v2ray nodes",
    "free clash nodes",
    "free trojan nodes",
    "free hysteria nodes",
    "free vless nodes",
    "free hysteria2 nodes",
    "free tuic nodes",
    "free reality nodes",
    "free singbox nodes",
    "free shadowsocks nodes",
    "free wireguard nodes",
    "free proxy list",
    "free proxy subscription",
    "free proxy config",
    "free proxy yaml",
    "free proxy json",
    "free proxy base64",

]

# --- 否定关键词列表 ---
# 类型：list of str；可以为空列表 []。
# 这些词代表与代理节点无关的常见仓库类型，搜索时会以“-keyword”形式排除。
# 例如 "-adblock -filter -blocklist -domain -asn ..."
# 如果不需要否定关键词，设置为空列表 [] 即可。
# 否定关键词本身不消耗额外配额。搜索 API 的限流是按“请求次数”计算的，不是按查询的复杂程度。你在一次搜索请求里加上 100 个否定关键词，仍然只算 1 次搜索请求。
# 数量没有硬性限制。GitHub 搜索查询字符串的最大长度是 256 个字符，但不适用于 API 查询？实际上 GitHub API 的查询字符串也有长度限制（通常约 8KB），但对于否定关键词来说，几百个词完全没问题。
SEARCH_NEGATIVE_KEYWORDS = [

]

# --- 搜索后缀常量 ---
# 以下配置控制搜索查询的附加限定符，所有值均可在对应位置设置为空/False以禁用。

# 是否包含 fork 仓库。大量节点分享仓库以 fork 形式存在。
# 类型：bool；默认值：True；不可为空。
# 设为 False 则不在搜索后缀中包含 fork:true。
SEARCH_FORK = True

# 仓库大小范围（KB）。格式如 "1..50000"。
# 类型：str；默认值：""（空字符串，不限制大小）。
# 设置为空字符串 "" 表示不限制仓库大小。
# 设置示例："1..50000" 表示仓库大小在 1KB 到 50MB 之间。
SEARCH_SIZE_RANGE = ""

# 是否排除已归档仓库。
# 类型：bool；默认值：False（不排除，因为 pushed 限定已过滤掉不更新的仓库）。
# 不可为空。若设为 True，会在搜索后缀中加入 archived:false。
SEARCH_ARCHIVED = False

# 是否将搜索范围限制在仓库名和描述中。
# 类型：bool；默认值：True；不可为空。
# 开启后会将 "in:name,description" 添加到关键词与时间之间，大幅提高精准度。
SEARCH_IN_NAME_DESCRIPTION = False

# 组装固定后缀（不含 pushed 时间，由 main.py 动态添加）
# 此变量由下方代码自动生成，无需手动修改。
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

# 分片大小：no.txt 中每多少个节点分一个文件
# 类型：int；默认值：10000；不可为空。
CHUNK_SIZE = 10000

# 存活节点文件（测速后）
# 类型：str；默认值："alive.txt"；不可为空。
ALIVE_NODE_FILE = "alive.txt"

# mihomo 订阅输出文件
# 类型：str；默认值："mihomo.yaml"；不可为空。
MIHOMO_OUTPUT_FILE = "mihomo.yaml"

# mihomo 模板文件（用户自定义）
# 类型：str；默认值："new.yaml"；不可为空。
# 如果文件不存在，程序会使用默认最小配置。
MIHOMO_TEMPLATE_FILE = "new.yaml"
