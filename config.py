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
    """
    从 mihomo 官方获取最新稳定版本号。
    接口：https://github.com/MetaCubeX/mihomo/releases/latest/download/version.txt
    返回格式如 "v1.19.24"。失败时回退到 "v1.18.7"。
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

# ==================== mihomo 连接与路径 ====================

# mihomo 二进制下载地址（根据版本号拼接）
MIHOMO_URL = (
    f"https://github.com/MetaCubeX/mihomo/releases/download/"
    f"{MIHOMO_VERSION}/mihomo-linux-amd64-{MIHOMO_VERSION}.gz"
)
# 本地解压后的可执行文件名
MIHOMO_BIN = "mihomo"
# mihomo 本地代理端口（用于速度测试）
MIXED_PORT = 7890
# mihomo External Controller API 端口（用于延迟测试和代理切换）
API_PORT = 9090

# ==================== 测速目标 URL ====================

# 延迟测试 URL：通过代理访问此地址，测量 HTTP 往返时间（毫秒级）
# 推荐使用 Google 的 generate_204 端点，全球可达且响应极快
LATENCY_TEST_URL = "https://www.gstatic.com/generate_204"
# 速度测试 URL：通过代理下载此文件来测量带宽
# 使用 Cloudflare 的 10MB 测试文件，可根据需要替换为自建测速点
SPEED_TEST_URL = "https://speed.cloudflare.com/__down?bytes=10485760"

# ==================== 测速超时与过滤阈值 ====================

# 单节点延迟测试超时（毫秒）。超过此值视为节点不可达，mihomo API 控制
LATENCY_TIMEOUT = 5000
# 单节点速度测试超时（秒）。超过此时间未完成下载视为测速失败
SPEED_TIMEOUT = 15
# 最小下载字节数（5MB）。低于此下载量视为测速失败，防止连接抖动误判
MIN_DOWNLOAD_BYTES = 5 * 1024 * 1024
# 最大允许延迟（毫秒）。超过此值的节点将被丢弃
MAX_LATENCY = 3000
# 最低允许速度（MB/s）。低于此值的节点将被丢弃
MIN_SPEED_MB = 0.5

# ==================== TCP 端口预筛选 ====================

# 是否启用 TCP 端口连通性预筛选。强烈建议开启，可快速滤除 70%-90% 的死节点
TCP_SCAN_ENABLED = True
# 单个端口连接超时（秒），建议 1-2 秒
TCP_SCAN_TIMEOUT = 1.5
# TCP 扫描并发线程数，可根据机器性能调整（200 较为平衡）
TCP_SCAN_WORKERS = 200

# ==================== 测速分批 ====================

# 每批送入 mihomo 的节点数
# 一次性加载过多节点会导致配置文件过大、mihomo 启动失败或内存溢出
# 建议 3000 个节点一批
TEST_BATCH_SIZE = 3000

# ==================== GitHub 搜索配置 ====================

# 每个关键词最多搜索的页数（每页 PER_PAGE 条结果）
# 搜索结果按最近更新排序，前 3 页通常已覆盖绝大多数有效仓库
MAX_PAGES = 3
# 每页返回的仓库数。GitHub 上限 100，设为 30 可减少单次响应体积，降低限流风险
PER_PAGE = 30
# 处理完一个仓库后的休眠时间（秒），避免触发 GitHub 二次限流
REPO_SLEEP_SECONDS = 0.5
# 搜索翻页冷却时间（秒），确保不超过 Search API 的 30 次/分钟限制
PAGE_SLEEP_SECONDS = 2
# 单个仓库的处理超时时间（秒）。防止大仓库或网络问题导致卡死
REPO_TIMEOUT_SECONDS = 120

# ==================== 限流控制 ====================

# 累计限流等待阈值（秒）。当整个运行过程中因触发 GitHub 限流而等待的总时间超过此值，
# 程序将主动终止搜索，直接进入测速阶段。设为 600 秒（10 分钟），兼顾任务完整性与效率
MAX_TOTAL_RATE_LIMIT_WAIT = 600

# ==================== 文件收集配置 ====================

# 最大允许下载的文件大小（字节）。None 表示不限制。
# 代理订阅文件通常几十 KB 到几 MB，设定上限可避免下载大型日志、广告规则等无用文件。
MAX_FILE_SIZE = None
# 单个文件处理超时（秒）。用于控制正则提取的执行时间，防止复杂文件卡死
FILE_PROCESS_TIMEOUT = 30
# 允许处理的文件扩展名集合。只有这些扩展名的文件才会被下载和解析。
# 添加了空字符串，以支持无扩展名的文件（如部分 Base64 订阅）
ALLOWED_EXTENSIONS = {
    '.yaml', '.yml', '.json', '.txt', '.md', '.conf', '.list', '.base64', ''
}
# 黑名单文件路径。记录“未提取到任何节点”的仓库，跨运行持久化，避免重复处理无效仓库
BLACKLIST_FILE = "ljck.txt"

# ==================== HEAD 请求优化参数 ====================

# HEAD 请求并发线程数。增大可提高处理速度，但过高可能触发 raw 端点限流
HEAD_CONCURRENCY = 20
# 单仓库最大 HEAD 请求数。超过此数量的文件直接跳过，防止超大仓库拖慢整体进度。
# None 表示不限制。
MAX_HEAD_PER_REPO = None
# 候选文件数阈值：低于此值时不启动并发 HEAD，直接串行处理，避免线程池开销
MIN_FILES_FOR_CONCURRENCY = 50

# ==================== API 请求超时（秒） ====================

# 搜索请求超时 (连接超时, 读取超时)
SEARCH_TIMEOUT = (15, 30)
# 仓库信息请求超时 (连接超时, 读取超时)
REPO_INFO_TIMEOUT = (8, 15)
# 文件下载请求超时 (连接超时, 读取超时)
FILE_DOWNLOAD_TIMEOUT = (10, 30)
# Contents API 超时
CONTENTS_API_TIMEOUT = (10, 20)
# Commits API 超时（回退方案时使用）
COMMITS_API_TIMEOUT = (8, 12)
# git/trees API 超时
TREE_API_TIMEOUT = (12, 20)

# ==================== 树 API 策略 ====================

# 是否优先使用 git/trees?recursive=1 一次性获取文件树
# 开启后可大幅减少 API 调用，但超大仓库可能被截断（会触发回退）
USE_RECURSIVE_TREE = True

# ==================== 搜索关键词与限定符 ====================

# --- 基础关键词列表 ---
# 每个关键词将自动拼接搜索后缀，形成最终的 GitHub 搜索查询
# 可按“精准词”和“泛化词”分组，但目前统一处理，后续可通过分层方式优化翻页数
BASE_QUERIES = [
    # 免费节点相关（中文）
    "免费节点",
    "免费clash订阅",
    "免费v2ray订阅",
    "免费机场节点",
    "节点订阅",
    "免费机场",
    "科学上网",
    "代理",
    "免费ssr节点",
    "免费vless节点",
    "免费reality节点",
    "免费tuic节点",
    "免费singbox节点",
    "公益节点",
    "节点分享",
    "节点仓库",
    "每日节点",
    "免费节点合集",
    # 订阅相关
    "clash订阅",
    "v2ray订阅",
    "trojan订阅",
    "hysteria2订阅",
    # 英文关键词
    "free nodes",
    "free v2ray nodes",
    "free clash nodes",
    "free trojan nodes",
    "free proxy list",
    "free proxy subscription",
    # 知名项目名
    "subconverter",
    "ACL4SSR",
    "v2rayN",
    "mihomo",
    "Clash.Meta",
    "Shadowrocket",
    "Hiddify",
    "Nekoray",
    # 收集器项目名
    "ProxyCollector",
    "TelegramV2rayCollector",
    "free proxy scraper",
    "free proxy bot",
]

# --- 否定关键词列表 ---
# 这些词代表与代理节点无关的常见仓库类型，搜索时会以“-keyword”形式排除
# 例如 "-adblock -filter -blocklist -domain -asn ..."
# 可根据实际噪声类型增删
SEARCH_NEGATIVE_KEYWORDS = [
    "adblock",      # 广告拦截
    "adguard",      # AdGuard 过滤规则
    "filter",       # 通用过滤规则
    "blocklist",    # 黑名单列表
    "blacklist",    # 黑名单
    "domain",       # 域名列表（非节点）
    "asn",          # ASN 列表
    "iptv",         # IPTV 频道列表
    "dns",          # DNS 配置
    "geosite",      # GeoSite 数据
    "geoip",        # GeoIP 数据库
    "rule",         # 通用规则
    "regex",        # 正则表达式规则
    "firewall",     # 防火墙规则
    "parental",     # 家长控制
    "phishing",     # 钓鱼检测
    "malware",      # 恶意软件
    "tracker",      # 跟踪器列表
    "spam",         # 垃圾邮件
    "telemetry",    # 遥测
    "crypto",       # 加密货币钱包/挖矿
    "mining",       # 挖矿
    "bot",          # 机器人
    "scraper",      # 爬虫
]

# --- 搜索后缀常量 ---
# 此部分由 main.py 动态拼接待搜索的 pushed 时间，然后与下列固定后缀组合
# 最终每个查询的格式为："{keyword} pushed:>{time} {固定后缀}"
SEARCH_FORK = True                 # 是否包含 fork 仓库。大量节点分享仓库以 fork 形式存在，强烈建议开启
SEARCH_SIZE_RANGE = "1..50000"     # 仓库大小范围（KB），过滤掉过大或过小的仓库。节点仓库通常在几 MB 到几十 MB
SEARCH_ARCHIVED = False            # 是否排除已归档仓库。由于已用 pushed 限定最近更新，归档仓库通常不会更新，可设为 False 以简化查询
SEARCH_IN_NAME_DESCRIPTION = True  # 是否将搜索范围限制在仓库名和描述中。开启可大幅提高精准度，减少 README 中的无关匹配

# 组装固定后缀（不含 pushed 时间，由 main.py 动态添加）
# 形如 "fork:true size:1..50000 -adblock -filter ..."
# 如果启用 in:name,description，会在每个关键词内部拼接，此处不添加
# 否定关键词直接拼接为 "-keyword"
SEARCH_SUFFIX = ""
if SEARCH_FORK:
    SEARCH_SUFFIX += " fork:true"
if SEARCH_SIZE_RANGE:
    SEARCH_SUFFIX += f" size:{SEARCH_SIZE_RANGE}"
if SEARCH_ARCHIVED:
    SEARCH_SUFFIX += " archived:false"
# 追加否定关键词
for kw in SEARCH_NEGATIVE_KEYWORDS:
    SEARCH_SUFFIX += f" -{kw}"

# ==================== 输出文件 ====================

# 分片大小：no.txt 中每多少个节点分一个文件
CHUNK_SIZE = 10000
# 存活节点文件（测速后）
ALIVE_NODE_FILE = "alive.txt"
# mihomo 订阅输出文件
MIHOMO_OUTPUT_FILE = "mihomo.yaml"
# mihomo 模板文件（用户自定义）
MIHOMO_TEMPLATE_FILE = "new.yaml"
