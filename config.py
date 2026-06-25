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

# GitHub 访问令牌，从环境变量获取。
# ⚠️ GA 内置的 GITHUB_TOKEN 限额仅 1000/小时，本项目一次运行消耗 3000+ 次核心 API。
# 强烈建议使用 Personal Access Token (PAT) 以获得 5000/小时的额度。
# 在仓库 Settings → Secrets and variables → Actions → 添加 GH_PAT
# 然后修改 workflow：GITHUB_TOKEN: ${{ secrets.GH_PAT }}
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

# ==================== 限流控制 ====================

# 累计限流等待上限（秒）。
# GitHub API 限流每小时重置一次，实际等待时间可能在 1-60 分钟。
# GA 超时 6h，设 3600（1 小时）足够等到下个小时的配额刷新。
# 取值范围 600 - 7200（1 小时为推荐值）。
MAX_TOTAL_RATE_LIMIT_WAIT = 3600

# 程序最大运行时间（秒），超出后停止搜集、开始保存。
# GA 默认超时 6 小时（21600s），提前 30 分钟收尾留足保存和提交时间。
# 默认 19800（5.5 小时）。设为 0 或 None 表示不限制。
MAX_RUNTIME_SECONDS = 19800

# ==================== 搜集渠道开关 ====================

# 是否启用 GitHub 搜索（搜索仓库 → 下载文件 → 提取节点）
# 默认 True。关闭时可单独调试网页/TG 渠道。
GITHUB_SEARCH_ENABLED = True

# ==================== GitHub 搜索配置 ====================

# 每个关键词搜索的最大页数
# 默认 3，取值范围 1-10。每页 30 条，3 页 = 最多 90 个仓库。
MAX_PAGES = 5

# 每页搜索结果数
# 默认 30，取值范围 1-100（GitHub API 上限）。
PER_PAGE = 50

# 仓库间休眠（秒），避免连续请求触发限流
# 默认 0.5，取值范围 0.1-2.0。
REPO_SLEEP_SECONDS = 0.5

# 翻页间休眠（秒）
# 默认 2.0，取值范围 1.0-5.0。
PAGE_SLEEP_SECONDS = 2.0

# 单仓库处理总超时（秒），None 表示不限制
# 默认 120，取值范围 30-300。超时后跳过该仓库继续处理下一个。
REPO_TIMEOUT_SECONDS = 120

# 仓库最 后更新时间阈值（小时）。非搜索来源的仓库超过此时间未推送则直接跳过。
# ≤ 24h → 正常处理（可能有新文件）
# 24h-168h（7天）→ 跳过，但不进黑名单（可能有历史节点）
# > 168h → 跳过 + 加黑名单（废弃仓库）
# 默认 168（7天），设为 0 表示不限制。
REPO_MAX_AGE_HOURS = 168

# 单仓库最大直接下载候选文件数（不含 commits API 过滤）
# 候选文件数 ≤ 此值时，直接通过 raw URL 下载（无 API 消耗）。
# 候选文件数 > 此值时，用 GitHub Compare API 获取 24h 内变更的文件集合（3 次 API 调用）。
# 默认 20，取值范围 5-100。
MAX_RAW_DOWNLOADS_PER_REPO = 100

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

# SHA 缓存最大条目数
# SHA 是内容哈希，内容不变则 SHA 不变，缓存应长期保留。
# 有上限时文件大小固定，不会随时间无限增长。
# 默认 1000000（一百万条），约 4MB 文件、200MB 内存。GA 7GB 内存下完全可接受。
# 加载耗时约 0.2 秒，查重为微秒级。
# 取值范围 50000-5000000。超过 500 万条时注意内存占用 (~1GB)。
SHA_CACHE_MAX_ENTRIES = 5000000

# 单仓库最大候选文件处理数，None 表示不限制
# 默认 None。限制值可设为 50，避免超大仓库消耗过多资源。
MAX_COMMITS_PER_REPO = None

# ==================== Fork 链追踪 ====================

# 是否追踪 fork 链（发现 fork 仓库后追溯父仓库，再查父仓库的所有 fork）
# 默认 False。可发现同一模板在不同 fork 中的不同节点。
FORK_CHAIN_ENABLED = True

# 每个仓库最多查几个 fork（分页，每页 30），用于子仓库和兄弟仓库遍历
# 默认 30，取值范围 10-100。
FORK_CHAIN_MAX_FORKS = 30

# 往上追溯父仓库的层数
# 默认 1。1 层通常够，更深的链路中节点高度重复。
MAX_PARENT_TRACE_DEPTH = 1

# Fork 链中本仓库的子仓库遍历层数。
# 0 表示不查子仓库。1 表示查本仓库的直接 fork，2 表示还查 fork 的 fork。
FORK_CHAIN_CHILD_DEPTH = 1


# ==================== 同用户仓库遍历 ====================

# 是否在发现节点后遍历该用户名下的所有公开仓库
# 默认 False。节点搜集者通常有多个相关仓库，一次发现可扫光。
USER_REPOS_ENABLED = True

# 每个用户最多额外查询几个仓库（通过 repos API 分页获取）
# 默认 30，取值范围 5-100。设为 0 表示不限制。
USER_REPOS_MAX_PER_USER = 5

# ==================== 种子仓库自动维护 ====================

# 是否自动收录高产出仓库到种子文件
# 默认 False。满足所有阈值条件的仓库会被自动写入 seed_repos.json。
AUTO_SEED_ENABLED = False

# 连续 N 次运行都产出新节点才收录。0 表示不限制。
# 默认 3，取值范围 0-10。
AUTO_SEED_MIN_CONSECUTIVE = 3

# 每次至少产出 N 个新节点才计数。0 表示不限制。
# 默认 10，取值范围 0-100。
AUTO_SEED_MIN_NODES = 10

# ==================== GitHub Topic 搜索 ====================

# 是否启用 GitHub 话题（topic）搜索
# 默认 False。Topic 搜索比关键词搜仓库名更精准。
TOPIC_SEARCH_ENABLED = True

# Topic 搜索词列表（每个会被拓展为 topic:xxx pushed:>24h）
TOPIC_QUERIES = [
    "proxypool",
    "v2ray",
    "free-proxy",
    "clash-subscribe",
    "v2ray-subscribe",
]

# 并行下载阈值：候选文件数超过此值启用线程池并发下载
# 默认 10。大部分仓库只有 2-5 个候选文件，串行更省开销。
PARALLEL_DOWNLOAD_THRESHOLD = 10

# 并行下载最大线程数
# 默认 8。GitHub Actions 2核下 8 线程足够。raw 下载不占 API 配额，无限制风险。
PARALLEL_DOWNLOAD_WORKERS = 8

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

# ==================== 种子仓库 ====================

# 种子仓库文件路径（JSON 数组，每行一个 "owner/repo"）
# 作为数据文件独立存储，与代码分离，便于动态更新。
# 系统每次运行会将此文件中的仓库加入处理队列，并通过 sources.json 追踪产出。
SEED_REPOS_FILE = "seed_repos.json"

# ==================== 来源种子管理 ====================

# 来源淘汰天数
# 默认 7。种子仓库的价值不仅在于自身产出，更在于其 fork 链和聚合资源。
# 只有当种子仓库自身 + 其 fork 链 + 其发现的其他仓库 全部超过此天数
# 无新节点时，才视为失效并从种子文件移除。
# 设为 0 表示不自动淘汰。
SOURCE_STALE_DAYS = 1

# ==================== 网页搜索配置 ====================

# 是否启用网页搜索（搜索引擎抓取）
# 默认 False。需要配合 WEB_SEARCH_ENGINES 使用。
WEB_SEARCH_ENABLED = False

# 搜索引擎列表（可多选）
# 支持: "google", "bing", "duckduckgo", "yandex"
# 每个引擎的搜索结果都会被下载和提取。
# DuckDuckGo 对 DC IP 限流严格（202），bing/yandex 更宽松，bing 放首位
WEB_SEARCH_ENGINES = ["bing", "duckduckgo", "yandex"]

# 每个关键词搜索的最大页数
# 默认 2，取值范围 1-5。
WEB_MAX_PAGES = 5

# 每页搜索结果数
# 默认 30，取值范围 10-100。
WEB_PER_PAGE = 50

# 搜索结果页间休眠（秒）
# 默认 3.0，取值范围 1.0-10.0。
WEB_PAGE_SLEEP = 3.0

# 搜索结果 URL 下载超时（秒）
# 默认 (8, 15)。
WEB_DOWNLOAD_TIMEOUT = (8, 15)

# 网页 URL 黑名单文件路径
# 存储已知无节点或广告网站的域名，每行一个。跨运行持久化。
WEB_BLACKLIST_FILE = "web_blacklist.txt"

# User-Agent 轮换池（搜索引擎爬取时随机选取，避免被反爬）
WEB_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
]

# ==================== 搜索关键词 ====================

# 基础搜索关键词列表（纯文本，不含时间/语言限定符）
# 每个关键词会附加 pushed:>24h 时间限定，确保只搜索最近活跃的仓库。
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
        # ==================== 2. 主流项目 ====================
        "ACL4SSR",
        "subconverter",
        "v2rayN",
        "v2rayNG",
        "Clash.Meta",
        "mihomo",
        "Hiddify",
        "Shadowrocket",
        "Quantumult X",
        "Stash",
        "clash verge",
        "clash verge rev",
        "v2rayA",
        "Nekoray",
        "Nekobox",
        "FlClash",
        # ==================== 3. 订阅相关 ====================
        "free subscription github",
        "daily subscription",
        "base64 subscription",
        "free sub",
        "v2ray sub",
        "clash sub",
        "trojan sub",
        "ss sub",
        "ssr sub",
        "sing-box sub",
        "hysteria sub",
        "tuic sub",
        "sub list",
        "sub store",
        "sub collection",
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
        # ==================== 5. 混合 OR 组合 ====================
        "免费 (clash OR v2ray OR trojan) (订阅 OR 节点 OR 机场)",
        "clash (订阅 OR 配置 OR 节点) github",
        "v2ray (订阅 OR 配置 OR 节点) github",
        "trojan (订阅 OR 配置 OR 节点) github",
        "hysteria (订阅 OR 配置 OR 节点) github",
        "hysteria2 (订阅 OR 配置 OR 节点) github",
        "tuic (订阅 OR 配置 OR 节点) github",
        "singbox (订阅 OR 配置 OR 节点) github",
        # ==================== 6. 收集器/项目名 ====================
        "TelegramV2rayCollector",
        "ProxyCollector",
        "V2RAY-CLASH-BASE64-Subscription",
        "free airport nodes",
        "free shadowrocket nodes",
        "free hiddify nodes",
        "free v2rayng nodes",
        "free clash meta nodes",
        "free mihomo nodes",
        "free proxy scraper",
        "free proxy spider",
        "free proxy bot",
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
MAX_RECURSIVE_REPOS = 20

# 最大递归深度
# 默认 2，取值范围 1-3。过深可能导致链式爬取失控。
MAX_RECURSIVE_DEPTH = 2

# ==================== 输出配置 ====================

# 批次刷盘阈值（buffer 中累积到此数量自动写入文件）
# 默认 10000，取值范围 1000-50000。
BATCH_FLUSH_SIZE = 10000

# 批次文件存放目录（节点边搜集边分批次持久化）
# 默认 "batches"，在项目根目录下创建。
BATCH_DIR = "batches"

# 每个分片文件包含的节点数
# 默认 10000，取值范围 1000-50000。
CHUNK_SIZE = 5000

# 存活节点输出文件
ALIVE_NODE_FILE = "alive.txt"

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
