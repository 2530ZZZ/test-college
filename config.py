"""
全局配置 — 集中管理所有可调参数。

设计原则：
  1. 纯常量 — import 本模块不会触发任何网络请求或副作用。
  2. 详细注释 — 每个参数注明：作用、类型、默认值、取值范围、配置建议。
  3. 分类清晰 — 按功能模块分组，便于查找。

版本号等需要网络获取的值统一通过环境变量传入，或在使用处动态获取。
"""

import os

# ==================== GitHub API 认证 ====================

# GitHub Personal Access Token，从环境变量获取
# 未认证 API 限额 60 次/小时，认证后 5,000 次/小时。强烈建议设置。
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

# ==================== 限流控制 ====================

# 累计限流等待上限（秒）。
# 当 safe_get 遇到 403 限流后的累计等待时间超过此值，立即终止所有后续网络请求。
# 默认 600（10 分钟），取值范围 60 - 3600。
# 调整建议：GitHub Actions 超时 6h，建议不超过 1800（30 分钟）以保留足够时间。
MAX_TOTAL_RATE_LIMIT_WAIT = 600

# ==================== GitHub 搜索配置 ====================

# 每个关键词搜索的最大页数
# 默认 3，取值范围 1-10。每页 30 条，3 页 = 最多 90 个仓库。
MAX_PAGES = 3

# 每页搜索结果数
# 默认 30，取值范围 1-100（GitHub API 上限）。
PER_PAGE = 30

# 仓库间休眠（秒），避免连续请求触发限流
# 默认 0.5，取值范围 0.1-2.0。
REPO_SLEEP_SECONDS = 0.5

# 翻页间休眠（秒）
# 默认 2.0，取值范围 1.0-5.0。
PAGE_SLEEP_SECONDS = 2.0

# 单仓库处理总超时（秒），None 表示不限制
# 默认 120，取值范围 30-300。超时后跳过该仓库继续处理下一个。
REPO_TIMEOUT_SECONDS = 120

# 单仓库最大直接下载候选文件数（不含 commits API 过滤）
# 候选文件数 ≤ 此值时，直接通过 raw URL 下载（无 API 消耗）。
# 候选文件数 > 此值时，用 GitHub Compare API 获取 24h 内变更的文件集合（3 次 API 调用）。
# 默认 20，取值范围 5-100。
MAX_RAW_DOWNLOADS_PER_REPO = 20

# 单仓库最大候选文件处理数，None 表示不限制
# 默认 None。限制值可设为 50，避免超大仓库消耗过多资源。

# ==================== 文件收集配置 ====================

# 允许处理的文件扩展名（集合，包含空字符串支持无扩展名文件）
ALLOWED_EXTENSIONS = {
    '.yaml', '.yml', '.json', '.txt', '.md', '.conf', '.list', '.base64', ''
}

# 最大下载文件大小（字节），None 表示不限制
# 默认 None。限制值可设为 5MB = 5242880。
MAX_FILE_SIZE = None

# 单个文件内容正则提取超时（秒），None 表示不限制
# 默认 None。对大文件可设为 10-30 避免正则回溯爆炸。
FILE_PROCESS_TIMEOUT = None

# 黑名单文件路径
# 存储已验证无节点或广告仓库的 GitHub URL，每行一个。跨运行持久化。
BLACKLIST_FILE = "ljck.txt"

# SHA 缓存文件路径
# 存储已处理文件 SHA → 时间戳的 pickle 字典。跨运行持久化。
SHA_CACHE_FILE = "sha_cache.pkl"

# SHA 缓存保留天数
# 默认 7。超过此天数的缓存条目将被清理，释放磁盘空间。
SHA_CACHE_TTL_DAYS = 7

# 单仓库最大候选文件处理数，None 表示不限制
# 默认 None。限制值可设为 50，避免超大仓库消耗过多资源。
MAX_COMMITS_PER_REPO = None

# ==================== API 请求超时设置 ====================

# 超时格式: (connect_timeout, read_timeout)，单位秒

# 搜索 API 超时
SEARCH_TIMEOUT = (15, 30)

# 仓库信息 API 超时（仅 Contents API 回退路径使用）
REPO_INFO_TIMEOUT = (8, 15)

# raw 文件下载超时
FILE_DOWNLOAD_TIMEOUT = (10, 30)

# Contents API 超时（回退路径，仅在树 API 失败时使用）
CONTENTS_API_TIMEOUT = (10, 20)

# Commits API 超时
COMMITS_API_TIMEOUT = (8, 12)

# Tree API 超时（递归树）
TREE_API_TIMEOUT = (12, 20)

# ==================== 树 API 策略 ====================

# 是否使用递归树 API（`git/trees/{branch}?recursive=1`）
# 默认 True。一次调用获取全仓库文件树，失败时自动回退到 Contents API 逐层遍历。
USE_RECURSIVE_TREE = True

# ==================== 搜索关键词 ====================

# 基础搜索关键词列表（纯文本，不含时间/语言限定符）
# 每个关键词会附加 pushed:>24h 时间限定，确保只搜索最近活跃的仓库。
BASE_QUERIES = [
    "free v2ray nodes",
]

# 是否包含 fork 仓库
# 默认 True。fork 仓库可能包含不同的节点集合。
SEARCH_FORK = True

# 仓库大小范围（空字符串表示不限制）
# 格式: ">=1000" 或 "100..5000"，见 GitHub 搜索语法。
SEARCH_SIZE_RANGE = ""

# 是否排除已归档仓库
# 默认 False。归档仓库虽然不更新但节点可能仍然有效。
SEARCH_ARCHIVED = False

# 搜索范围限定（空字符串表示不限制）
# 例如: "readme", "name", "description"
SEARCH_IN = ""

# 排除的编程语言列表
# 默认 ["HTML"]。HTML 仓库以网页为主，节点少且多含广告。
SEARCH_EXCLUDE_LANGUAGES = ["HTML"]

# 排除关键词列表（搜索阶段排除噪音）
# 每个关键词前缀 "-" 加入搜索查询。例如 "-免费VPN"。
SEARCH_NEGATIVE_KEYWORDS = []

# ==================== README 广告检测 ====================

# README 广告关键词（子串匹配，不区分大小写）
# 在下载文件之前先检查 README，包含任一关键词的仓库加入黑名单并跳过。
# 注意：raw.githubusercontent.com 下载不计入 API 配额。
README_SPAM_KEYWORDS = [
    "飞鸟加速", "星辰VPN", "西游云", "老村长机场", "农夫山泉", "狗狗加速",
    "高速机场推荐", "机场推荐", "免费试用", "注册地址", "购买地址", "购买链接", "官网地址",
    "倍率", "折合", "续订", "大流量", "限速", "跑路", "不清零", "邀请码",
    "超值", "不限时", "优惠", "性价比", "客服", "支付宝", "微信", "付款", "套餐",
    "无视高峰，全天4K秒开", "IPLC、IEPL中转", "小电影丝般顺滑", "高速冲浪，科学上网不二选择",
]

# ==================== Raw 链接递归发现 ====================

# 是否启用从 raw 链接反向发现仓库
# 默认 True。下载文件中引用的 raw 链接可能指向其他节点仓库。
ENABLE_RAW_RECURSIVE = True

# 最多递归发现的仓库数量
# 默认 5，取值范围 0-20。
MAX_RECURSIVE_REPOS = 5

# 最大递归深度
# 默认 2，取值范围 1-3。过深可能导致链式爬取失控。
MAX_RECURSIVE_DEPTH = 2

# ==================== 输出配置 ====================

# 批次文件存放目录（用于 subs-check 测速）
# 默认 "batches"，在项目根目录下创建。
BATCH_DIR = "batches"

# 每个分片文件包含的节点数
# 默认 10000，取值范围 1000-50000。
CHUNK_SIZE = 10000

# 存活节点输出文件
ALIVE_NODE_FILE = "alive.txt"

# mihomo 兼容配置文件输出
MIHOMO_OUTPUT_FILE = "mihomo.yaml"

# mihomo 配置模板文件
MIHOMO_TEMPLATE_FILE = "new.yaml"

# ==================== mihomo 信息（仅用于最终输出 YAML，不用于测速） ====================

# mihomo 版本号。通过环境变量 MIHOMO_VERSION 指定，默认 v1.18.7。
# 注意：版本获取逻辑已移出 config，避免 import 时触发网络请求。
# 如需自动获取最新版本，在使用处调用 _fetch_latest_mihomo_version()。
MIHOMO_VERSION = os.getenv("MIHOMO_VERSION", "v1.18.7")

# mihomo 下载 URL 模板
MIHOMO_URL_TEMPLATE = (
    "https://github.com/MetaCubeX/mihomo/releases/download/"
    "{version}/mihomo-linux-amd64-{version}.gz"
)
MIHOMO_BIN = "mihomo"
MIHOMO_MIXED_PORT = 7890
MIHOMO_API_PORT = 9090

# ==================== 测速开关 ====================

# 是否启用测速。若关闭，则只搜集节点并保存，不执行 subs-check 测速。
# 默认 True。如果测速环境不可用（如 GitHub Actions 网络受限），设为 False。
SPEED_TEST_ENABLED = True

# ==================== subs-check 测速配置 ====================

# subs-check 二进制文件路径
# 默认 "subs-check"。如果在 PATH 中可直接用命令名，否则用绝对路径。
SUBS_CHECK_BIN = "subs-check"

# 每批节点数（边搜集边测速模式下，每凑够此数量就持久化并投喂给 subs-check）
# 默认 10000，取值范围 1000-50000。值越小单批越快但启动开销占比高。
SUBS_CHECK_BATCH_SIZE = 2000

# 最大并发 subs-check 实例数
# 默认 3，取值范围 1-5。GitHub Actions (2核/7GB) 不建议超过 3。
SUBS_CHECK_MAX_CONCURRENT = 3

# 单批次测速超时（秒）
# 默认 3600（1 小时），取值范围 300-18000。超时后 kill 进程并标记该批次失败。
SUBS_CHECK_BATCH_TIMEOUT = 3600

# subs-check 实例起始端口
# 默认 7890，每个并发实例递增 10 避免端口冲突。
SUBS_CHECK_BASE_PORT = 7890

# subs-check 并发测速线程数（goroutines）
# 默认 10，取值范围 1-50。GitHub Actions (2核/7GB) 建议 5-10。
# 本地高配机器可设 20+。这个参数对测速耗时影响最大。
SUBS_CHECK_CONCURRENT = 10

# 测速目标 URL — 延迟测试
# 使用 Google 的 generate_204 端点，全球 CDN，响应快且稳定。
SUBS_CHECK_LATENCY_URL = "https://www.gstatic.com/generate_204"

# 测速目标 URL — 下载速度测试
# 使用 Cloudflare 的测速端点。bytes 参数为文件大小（字节），默认 10MB。
SUBS_CHECK_SPEED_TEST_URL = "https://speed.cloudflare.com/__down?bytes=10485760"

# ==================== 测速过滤阈值 ====================

# 延迟测试超时（毫秒）
# 默认 5000，取值范围 1000-30000。
LATENCY_TIMEOUT = 5000

# 最大允许延迟（毫秒），超过此值的节点将被过滤
# 默认 3000，取值范围 500-10000。
MAX_LATENCY = 3000

# 速度测试超时（秒）
# 默认 15，取值范围 5-60。
SPEED_TIMEOUT = 15

# 最小下载字节数，低于此值视为下载失败
# 默认 5MB = 5242880，取值范围 1048576-104857600。
MIN_DOWNLOAD_BYTES = 5 * 1024 * 1024

# 最小下载速度（MB/s），低于此值的节点将被过滤
# 默认 0.5，取值范围 0.0-100.0。设为 0 表示不过滤。
MIN_SPEED_MB = 0.5

# ==================== 去重策略 ====================

# 是否启用去重
# 默认 True。
DEDUP_ENABLED = True

# 去重策略：
#   "server_port"          — 仅按 (server, port) 去重。同 IP:端口只保留第一个。
#   "server_port_protocol" — 按 (server, port, protocol) 去重（推荐）。
#                             同 IP:端口但不同协议的节点都保留（如 vmess + trojan 共享 443）。
# 默认 "server_port_protocol"。
DEDUP_STRATEGY = "server_port_protocol"

# ==================== 日志与持久化 ====================

# 日志目录
LOG_DIR = "log"

# 保留日志文件最大数量
# 默认 10。
MAX_LOG_FILES = 10

# ==================== 组装搜索后缀 ====================

# 搜索后缀（由 config 初始化时生成，纯字符串拼接，无副作用）
_SEARCH_SUFFIX_PARTS = []
if SEARCH_FORK:
    _SEARCH_SUFFIX_PARTS.append("fork:true")
if SEARCH_SIZE_RANGE:
    _SEARCH_SUFFIX_PARTS.append(f"size:{SEARCH_SIZE_RANGE}")
if SEARCH_ARCHIVED:
    _SEARCH_SUFFIX_PARTS.append("archived:false")
for kw in SEARCH_NEGATIVE_KEYWORDS:
    _SEARCH_SUFFIX_PARTS.append(f"-{kw}")
for lang in SEARCH_EXCLUDE_LANGUAGES:
    _SEARCH_SUFFIX_PARTS.append(f"-language:{lang}")

SEARCH_SUFFIX = " ".join(_SEARCH_SUFFIX_PARTS)

# 清理内部变量，避免被外部 import 污染命名空间
del _SEARCH_SUFFIX_PARTS
