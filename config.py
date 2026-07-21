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
MAX_TOTAL_RATE_LIMIT_WAIT = 7200

# 程序最大运行时间（秒），超出后停止搜集、开始保存。
# GA 默认超时 6 小时（21600s），提前 1 小时收尾留足保存和提交时间。
# 默认 18000（5 小时）。设为 0 或 None 表示不限制。
MAX_RUNTIME_SECONDS = 18000

# ==================== 搜集渠道开关 ====================

# 是否启用 GitHub 搜索（搜索仓库 → 下载文件 → 提取节点）
# 默认 True。关闭时可单独调试网页/TG 渠道。
GITHUB_SEARCH_ENABLED = True

# ==================== GitHub 搜索配置 ====================

# 每个关键词搜索的最大页数
# 默认 3，取值范围 1-10。每页 30 条，3 页 = 最多 90 个仓库。
MAX_PAGES = 5

# 每页搜索结果数
# 默认 100（GitHub API 上限）。越大翻页越少、搜索越快。
PER_PAGE = 100

# 仓库间休眠（秒），避免连续请求触发限流
# 默认 0.5，取值范围 0.1-2.0。
REPO_SLEEP_SECONDS = 0.5

# 翻页间休眠（秒）
# 默认 2.0，取值范围 1.0-5.0。
PAGE_SLEEP_SECONDS = 2.0

# 单仓库处理总超时（秒），None 表示不限制
# 默认 120，取值范围 30-300。超时后跳过该仓库继续处理下一个。
REPO_TIMEOUT_SECONDS = 300

# 仓库废弃年龄阈值（小时）。超过此值永久加入黑名单。
# 默认 168（7天），取值范围 0-720。设为 0 表示不限制。
REPO_MAX_AGE_HOURS = 168

# 仓库入口跳过年龄阈值（小时）。
# 超过此值的仓库跳过文件解析（省 Tree API），但仍追踪 fork 链。
# fork 仓库本身可能不活跃，但其链上的其他 fork 可能活跃并产出节点。
# 默认 24，取值范围 1-168。设为 0 关闭此过滤（所有仓库都处理）。
SKIP_PROCESSING_AGE_HOURS = 24

# 单仓库最大直接下载候选文件数（不含 commits API 过滤）
# 候选文件数 ≤ 此值时，直接通过 raw URL 下载（无 API 消耗）。
# 候选文件数 > 此值时，用 GitHub Compare API 获取 24h 内变更的文件集合（3 次 API 调用）。
# 默认 20，取值范围 5-100。
MAX_RAW_DOWNLOADS_PER_REPO = 100

# ==================== 文件收集配置 ====================

# 允许处理的文件扩展名（集合，包含空字符串支持无扩展名文件）
ALLOWED_EXTENSIONS = {
    '.yaml', '.yml', '.json', '.txt', '.md', '.conf', '.list', '.base64', ''
}

# 最大下载文件大小（字节），None 表示不限制
# 默认 None。限制值可设为 5MB = 5242880。
MAX_FILE_SIZE = None

# 单个文件内容正则提取超时（秒），None 表示不限制
# 默认 120。对大文件可设为 60-300 避免单文件卡死 Worker。
FILE_PROCESS_TIMEOUT = 120

# 黑名单文件路径
# 存储已验证无节点或广告仓库的 GitHub URL，每行一个。跨运行持久化。
BLACKLIST_FILE = "ljck.txt"

# SHA 缓存目录（分片存储，每片 ≤ 45MB，远离 GitHub 100MB 硬限制）
# 加载时遍历目录合并到内存一个 dict。写入时分片保证每文件不超限。
SHA_CACHE_DIR = "sha_cache"

# SHA 缓存每片最大字节数（默认 45MB）。
# 设为 90MB 的一半：pickle 序列化有 ~10% 开销（60字节估算 vs 实际 65字节），
# 90MB 阈值可能导致实际文件 95-100MB，太接近 GitHub 硬限制。
SHA_CACHE_MAX_BYTES = 45_000_000

# SHA 缓存最大条目数（总上限，0 表示不限制）
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
USER_REPOS_MAX_PER_USER = 30

# ==================== 日志配置 ====================

# 详细日志模式。True=输出每个文件的提取结果，False=仅摘要。
# 日常保持 False，排查问题时临时改为 True。
VERBOSE_LOG = False

# ==================== 种子仓库自动维护 ====================

# 是否自动收录高产出仓库到种子文件
# 默认 True。满足阈值条件的仓库自动收录到种子文件，无需手动维护。
AUTO_SEED_ENABLED = True

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
    # 协议/工具生态
    "proxypool",
    "v2ray",
    "sing-box",
    "hysteria2",
    "mihomo",
    "clash",
    "subconverter",
    "shadowsocks",
    "wireguard",
    "hysteria",
    "tuic-protocol",
    "xray-core",
    # 代理/节点
    "free-proxy",
    "proxy-list",
    "v2ray-nodes",
    "vpn",
    "proxy-provider",
    # 配置/订阅
    "clash-subscribe",
    "v2ray-subscribe",
    "v2ray-config",
    "clash-config",
    "vpn-configuration",
    "clash-meta",
    "sing-box-config",
    "subscriptions",
    "proxy-configuration",
    "v2ray-proxy",
]

# ==================== README 内容搜索 ====================

# 是否启用 README 内容搜索（in:readme 限定，只搜 README 文件内容）
# 默认 False。命中精度极高——README 里提到配置名的仓库必然相关。
README_SEARCH_ENABLED = True

# README 搜索关键词（独立于 BASE_QUERIES，侧重软件生态名和配置字段名）
README_QUERIES = [
    "Clash.Meta 订阅",
    "v2rayN 订阅",
    "v2rayNG 配置",
    "sing-box config",
    "Shadowrocket 订阅",
    "Nekobox 配置",
    "FlClash 节点",
    "Hiddify config",
    "mihomo 订阅",
    "proxy-groups clash",
    "vmess:// vless://",
    "trojan:// ss://",
    "机场订阅",
    "免费节点 订阅链接",
    "clash 订阅 节点",
]

# README 搜索最大页数（命中率高，浅页即可）
# 默认 2，取值范围 1-5。
README_MAX_PAGES = 5

# ==================== Code 文件内容搜索 ====================

# 是否启用 GitHub Code Search（搜文件内容中的 URI 字符串和配置字段）
# 默认 False。命中精度几乎 100%，直接定位到包含节点 URI 的文件。
CODE_SEARCH_ENABLED = True

# Code 搜索词列表
CODE_QUERIES = [
    # ============================================================
    # 分组 1: URI 前缀 × 全后缀（搜文件内容中的节点链接）
    # ============================================================
    # --- yaml ---
    "vmess:// extension:yaml",
    "vless:// extension:yaml",
    "trojan:// extension:yaml",
    "ss:// extension:yaml",
    "ssr:// extension:yaml",
    "hysteria2:// extension:yaml",
    "hy2:// extension:yaml",
    "tuic:// extension:yaml",
    # --- yml ---
    "vmess:// extension:yml",
    "vless:// extension:yml",
    "trojan:// extension:yml",
    "ss:// extension:yml",
    "hysteria2:// extension:yml",
    "tuic:// extension:yml",
    # --- json ---
    "vmess:// extension:json",
    "vless:// extension:json",
    "trojan:// extension:json",
    "ss:// extension:json",
    "hysteria2:// extension:json",
    "hy2:// extension:json",
    "tuic:// extension:json",
    # --- txt ---
    "vmess:// extension:txt",
    "vless:// extension:txt",
    "trojan:// extension:txt",
    "ss:// extension:txt",
    "ssr:// extension:txt",
    "hysteria2:// extension:txt",
    "hy2:// extension:txt",
    "tuic:// extension:txt",
    # --- conf ---
    "vmess:// extension:conf",
    "vless:// extension:conf",
    "trojan:// extension:conf",
    "ss:// extension:conf",
    "hysteria2:// extension:conf",
    "tuic:// extension:conf",
    # --- list ---
    "vmess:// extension:list",
    "vless:// extension:list",
    "trojan:// extension:list",
    "ss:// extension:list",
    "hysteria2:// extension:list",
    "tuic:// extension:list",
    # --- 无后缀 (base64 等) ---
    "vmess://",
    "vless://",
    "trojan://",
    "ss://",
    "hysteria2://",
    "tuic://",

    # ============================================================
    # 分组 2: 配置字段匹配 × 后缀（搜 Clash/Sing-box 配置块）
    # ============================================================
    # --- 协议 type 声明 ---
    '"type: vmess" extension:yaml',
    '"type: vless" extension:yaml',
    '"type: trojan" extension:yaml',
    '"type: ss" extension:yaml',
    '"type: ssr" extension:yaml',
    '"type: hysteria2" extension:yaml',
    '"type: tuic" extension:yaml',
    '"type: vmess" extension:yml',
    '"type: vless" extension:yml',
    '"type: trojan" extension:yml',
    '"type: hysteria2" extension:yml',
    '"type: hysteria2" extension:json',
    '"type: tuic" extension:json',
    # --- 通用配置字段 ---
    '"server:" extension:yaml',
    '"port:" extension:yaml',
    '"uuid:" extension:yaml',
    '"password:" extension:yaml',
    '"cipher:" extension:yaml',
    '"network:" extension:yaml',
    '"sni:" extension:yaml',
    '"alterId:" extension:yaml',
    '"server:" extension:yml',
    '"uuid:" extension:yml',
    '"server:" extension:json',
    '"server_port" extension:json',
    '"method:" extension:json',
    # --- 数组/块标记 ---
    "proxy-groups extension:yaml",
    "proxy-groups extension:yml",
    "outbounds extension:json",
    '"outbounds" extension:json',

    # ============================================================
    # 分组 3: 拆分关键词（协议名+配置特征共现，捕捉散落字段）
    # ============================================================
    "hysteria2 config extension:yaml",
    "hy2 outbound extension:json",
    "tuic server extension:yaml",
    "vless reality extension:json",
    "trojan password extension:yaml",
    "vmess uuid extension:json",
    "shadowsocks method extension:yaml",
    "ssr protocol extension:yaml",
    "hysteria2 server extension:json",
    "tuic congestion extension:json",
    "vless flow extension:yaml",

    # ============================================================
    # 分组 4: 聚合链接 + 搜集器代码
    # ============================================================
    "subscription-userinfo extension:txt",
    "subscription-userinfo extension:yml",
    "raw.githubusercontent.com extension:yaml",
    "raw.githubusercontent.com extension:json",
    "raw.githubusercontent.com extension:txt",
    "raw.githubusercontent.com extension:yml",
    "v2ray aggregator extension:py",
    "proxy collector extension:py",
    "vmess:// raw.githubusercontent extension:py",
]

# Code 搜索最大页数（每页 100 个文件）
# 默认 3，取值范围 1-5。
CODE_MAX_PAGES = 5

# ==================== 中文关键词翻页 ====================

# 中文关键词的翻页倍数（相较于 base MAX_PAGES）。中文仓库前几页广告多，
# 需要翻更多页才能找到真正的节点仓库。
# 默认 2。实际页数 = MAX_PAGES * MAX_PAGES_ZH_MULTIPLIER。
MAX_PAGES_ZH_MULTIPLIER = 2

# 并行下载阈值：候选文件数超过此值启用线程池并发下载
# 默认 10。大部分仓库只有 2-5 个候选文件，串行更省开销。
PARALLEL_DOWNLOAD_THRESHOLD = 10

# 并行下载最大线程数（raw CDN 不限流，设大加速）
PARALLEL_DOWNLOAD_WORKERS = 16

# ==================== 共用线程池 ====================

# 共用线程池 Worker 数（处理仓库/fork/用户仓库等所有类型任务）
# 默认 4。降低并发峰值可避免触发 GitHub 次级限流。
SHARED_POOL_WORKERS = 4

# 共用任务队列最大长度（背压控制，防止内存爆炸）
# 默认 200。队列满时生产者阻塞，等待消费者腾出空间。
SHARED_POOL_QUEUE_SIZE = 200

# ==================== API 配额管理 ====================

# 每小时最大 API 调用次数（留 200 余量给非关键调用）
# GitHub 认证用户限额 5000/小时，设为 4800 保证不触顶。
QUOTA_MAX_PER_HOUR = 4800

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
        # ==================== 7. 聚合/订阅补充 ====================
        "subconverter config",
        "proxy provider clash",
        "clash proxy provider",
        "surge proxy list",
        "v2ray node list",
        "FreeNodes",
        "singbox outbound config",
        "mihomo proxy provider",
        "proxy pool clash",
        "node pool free",
        "节点池",
        "subscribe pool",
        "免费订阅链接",
        "sub store",
        "sub collection github",
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
# 默认 50。raw 下载不消耗 API 配额，设大扩大覆盖面。
MAX_RECURSIVE_REPOS = 50

# 最大递归深度
# 默认 3。多一层可发现"链接的链接"引用的仓库。
MAX_RECURSIVE_DEPTH = 3

# ==================== 输出配置 ====================

# 批次刷盘阈值（buffer 中累积到此数量自动写入文件）
# 默认 10000，取值范围 1000-50000。
BATCH_FLUSH_SIZE = 5000

# 批次文件存放目录（节点边搜集边分批次持久化）
# 默认 "batches"，在项目根目录下创建。
BATCH_DIR = "batches"

# 每个分片文件包含的节点数
# 默认 10000，取值范围 1000-50000。
CHUNK_SIZE = 5000

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

# ==================== 队列控制 ====================

# 队列清空超时（秒）。
# 搜索阶段完成后等待 Worker 处理完队列中剩余任务的最大时间。
# 超时 → 放弃剩余任务 → 保存结果 → 正常退出（不再挂死在 task_queue.join()）。
# 默认 900（15分钟），取值范围 60-3600。
QUEUE_DRAIN_TIMEOUT_SECONDS = 900

# 队列满时等待超时（秒）。
# fork 链/用户仓库往队列放任务时，队列满最多等这么久。
# 超时 → 丢弃该任务（下次运行可能再发现）。
# 默认 10，取值范围 1-60。
QUEUE_PUT_TIMEOUT_SECONDS = 60

# ==================== 黑名单管理 ====================

# 是否启用黑名单自动淘汰（LRU 冷热分离）。
# True: 每次运行淘汰末尾冷门条目，热条目和本次新加入的条目不受影响。
# False: 黑名单只增不减。
# 默认 True。
BLACKLIST_EVICTION_ENABLED = True

# 黑名单淘汰比例（1/N）。
# 每次运行淘汰末尾 1/N 的旧冷条目。新加入的条目和本次命中过的"热"条目不受淘汰。
# 默认 30（即 1/30），取值范围 5-100。越小淘汰越激进。
BLACKLIST_EVICTION_RATIO = 30

# ==================== 解析失败记录 ====================

# 是否记录解析失败的候选文件。
# True: 有候选但全验证失败的文件写入 failed_candidates.txt（含样本和策略信息）。
# False: 不记录。
# 默认 True。
LOG_FAILED_CANDIDATES = True

# ==================== 并行下载内存保护 ====================

# 按仓库文件总体积动态降 Worker 数的阈值（MB）。
# 仓库候选文件总大小 > HIGH → 4 Worker，> MED → 8 Worker，≤ MED → 全速。
# 设为 0 关闭动态降级（始终全速并行下载）。
# 默认值：HIGH=500, MED=200。
PARALLEL_DOWNLOAD_MB_HIGH = 500
PARALLEL_DOWNLOAD_MB_MED = 200
