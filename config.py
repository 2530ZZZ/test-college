"""
全局配置 — 集中管理所有可调参数。

设计原则：
  1. 纯常量 — import 本模块不会触发任何网络请求或副作用。
  2. 详细注释 — 每个参数注明：作用、原理、默认值、取值范围、配置建议。
  3. 分类清晰 — 按功能模块分组，便于查找。

版本号等需要网络获取的值统一通过环境变量传入，或在使用处动态获取。

═══════════════════════════════════════════════════════════════
持久化文件一览
═══════════════════════════════════════════════════════════════

┌──────────────────┬──────────┬──────────────────────────────────┬──────────────────────┐
│ 文件             │ 格式     │ 内容                             │ 排序/淘汰            │
├──────────────────┼──────────┼──────────────────────────────────┼──────────────────────┤
│ sha_cache/*.pkl  │ pickle   │ {sha: datetime}                  │ 按时间戳升序分片     │
│                  │ 分片     │ 已下载文件的内容哈希              │ 超 MAX_ENTRIES 删旧 │
├──────────────────┼──────────┼──────────────────────────────────┼──────────────────────┤
│ seen_cache/*.pkl │ pickle   │ {repo: {pushed_at,hits,last_hit}}│ hits 降序+last_hit  │
│                  │ 分片     │ 已处理仓库及命中统计              │ 降序; 超 MAX 删尾   │
├──────────────────┼──────────┼──────────────────────────────────┼──────────────────────┤
│ seed_repos.json  │ JSON     │ {repos:{user/repo:{pushed_at,    │ pushed_at 降序       │
│                  │ 单文件   │   last_new_node}}}               │ 超 MAX_ENTRIES 删尾 │
├──────────────────┼──────────┼──────────────────────────────────┼──────────────────────┤
│ batch_dir/*.txt  │ 纯文本   │ 代理节点 URI，每行一个           │ 按批次号             │
│ no/              │          │ 批次分片文件                     │ 每次运行清空重建     │
├──────────────────┼──────────┼──────────────────────────────────┼──────────────────────┤
│ no_w_li.txt      │ 纯文本   │ 批次文件 raw 链接，每行一个      │ 追加                 │
│ no_li.txt        │ 纯文本   │ 去重源链接                       │ 每次运行覆写         │
│ failed_candidates│ 纯文本   │ 解析失败文件样本                 │ 每次运行覆写         │
└──────────────────┴──────────┴──────────────────────────────────┴──────────────────────┘

排序机制说明：所有文件在程序启动时加载到内存（dict/list），运行时只做内存操作。
保存时在内存中排序 → 淘汰 → 顺序写入磁盘。分片文件之间不维护严格排序，
每个分片内部按序排列。跨文件排序靠分片编号（0000=最旧/最低命中, N=最新/最高命中）。
═══════════════════════════════════════════════════════════════

队列仓库标志位
源头（[种子仓库]等）      → 追踪 fork + 父链 + user
[userN]/[404userN]        → 追踪 fork + raw（不追踪 user）
[forkN]/[rawN] N<MAX_TRACE_DEPTH → 追踪全部
[forkN]/[rawN] N≥MAX_TRACE_DEPTH → 直接处理

任何仓库 404（无论是否需追踪）→ 追踪用户（[404userN]）

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
# GA 默认超时 6 小时（21600s），提前 2 小时收尾留足队列清空 + 保存时间。
# 默认 14400（4 小时）。设为 0 或 None 表示不限制。
# 18000(5小时) 19800(5.5小时)
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
# 默认 100（GitHub API 上限）。越大翻页越少、搜索越快。
PER_PAGE = 100

# 仓库间休眠（秒），避免连续请求触发限流
# 默认 0.5，取值范围 0.1-2.0。
REPO_SLEEP_SECONDS = 0.5

# 翻页间休眠（秒）
# 默认 2.0，取值范围 1.0-5.0。
PAGE_SLEEP_SECONDS = 2.0

# 单仓库处理总超时（秒），None 表示不限制
# 作用：包裹 process_file_tree 回退路径，超时后跳过该仓库继续处理下一个。
# 默认 600（10 分钟）。取值范围 60-1200。
# 论证：极端 100MB 仓库的 Contents API 逐层遍历可能需要数分钟。
REPO_TIMEOUT_SECONDS = 600

# 仓库信息补查开关（info backfill）。
# 作用：process_repo 解析处理前，若判断所需的字段（pushed_at/language 等）
#       缺失，补查一次 GET /repos/{repo} 拿全信息再判断。
# 设为 False：不补查，缺什么用什么（缺 pushed_at 则跳过年龄/已解析判断直接处理）。
# 默认值：True。补查次数在 _finalize 统计输出，可据此评估 API 消耗。
INFO_BACKFILL_ENABLED = True

# 仓库入口跳过年龄阈值（小时）。
# 作用：超过此值的仓库跳过文件解析（省 Tree API），但仍追踪 fork/用户仓库。
# 原理：这是唯一年龄阈值。仓库太旧时文件基本没变（SHA 缓存会跳过），
#       但它的 fork/用户仓库可能活跃，所以仍追踪。
# 默认值：24。取值范围 1-720。设为 0 关闭此过滤（所有仓库都处理文件）。
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
# 作用：包裹 extract_all_strategies 解析，超时后跳过该文件。
# 默认 300（5 分钟）。
# 论证：100MB 文件 ≈ 1 亿字符，10+ 正则模式全量扫描约 100-200s，
#       再加 surrogate 清洗、订阅发现等，300s 是安全值。
FILE_PROCESS_TIMEOUT = 300

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
# 默认 True。可发现同一模板在不同 fork 中的不同节点。
# 注意：受 MAX_TRACE_DEPTH 控制，设为 True 后只有源头仓库触发追踪。
FORK_CHAIN_ENABLED = True

# --- 数量限制 ---

# 每个仓库最多查几个子 fork（从本仓库 fork 出去的仓库）
# 默认 30。子 fork 最直接相关，高产概率最高。
# 取值范围 5-100。
FORK_CHILD_MAX = 80

# 每个仓库最多查几个兄弟 fork（同父仓库下的其他 fork）
# 默认 20。兄弟 fork 相关度低于子 fork，配额省给子 fork。
# 取值范围 5-100。
FORK_SIBLING_MAX = 80

# 每个仓库最多查几个 fork（总上限，兜底用）
# 当单独限制未生效时使用此值。默认 30，取值范围 10-100。
FORK_CHAIN_MAX_FORKS = 100

# --- Fork 查询分页 ---

# Fork API 每页返回数（GitHub 最大 100）
# 默认 100。越大翻页越少、API 调用越省。
# 取值范围 10-100。
FORK_PER_PAGE = 100

# --- 父仓库追溯 ---

# 是否追溯父仓库（本仓库是从谁 fork 出来的）。
# Git fork 模型保证最多 1 个父仓库。查到父仓库后遍历其所有 fork（兄弟仓库）。
# 默认 True。
FORK_PARENT_TRACE_ENABLED = True

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

# 是否自动收录仓库到种子文件。
# 作用：任何渠道只要仓库产出了节点，自动加入 seed_repos.json。
# 原理：process_repo 处理完仓库后，如果 has_nodes_flag=True 且
#       新增节点数 ≥ AUTO_SEED_MIN_NODES_FOR_SEED → _update_seed_entry()
#       记录 pushed_at 和 last_new_node。
# 默认值：True。
# 设为 False：不自动收录，种子文件只减不增。
AUTO_SEED_ENABLED = True

# 种子仓库收录最少需要的节点产出数。
# 作用：低于此值的仓库即使有节点也不加入种子文件。
# 默认值：0（有任何节点即收录）。
# 建议值：0-10。设大可以过滤"碰巧有一个节点"的低价值仓库。
# 设为 0：有任何产出即收录。
AUTO_SEED_MIN_NODES_FOR_SEED = 1

# --- 种子排序 ---

# 排序方式：按 pushed_at（GitHub 最后推送时间）降序排列。
# 最近推送的种子排最前。pushed_at 为空或解析失败的 → 排最后。
# 每次运行结束时 _sort_seeds() 重排整个种子文件。
# 此排序不可配置（已固定为 pushed_at 降序）。

# --- 种子淘汰 ---

# 种子最大保留时间（小时）。
# 作用：pushed_at 超过此值的种子优先被淘汰。
# 原理：超 SEED_MAX_ENTRIES 时触发淘汰，优先删 pushed_at 超时的种子。
#       不会单独因超时而淘汰（需要同时满足条数超限）。
# 默认值：720（30天）。设为 0：不按时间淘汰，只按条数。
# 注意：种子记录的是 pushed_at（GitHub 最后推送时间），不是程序运行时间。
SEED_MAX_AGE_HOURS = 720

# 种子最大条数。超过时从尾部淘汰。
# 作用：控制种子文件大小，防止无限增长。
# 原理：超限时从尾部（pushed_at 最旧的）淘汰 1/SEED_EVICTION_RATIO。
#       优先淘汰 pushed_at > SEED_MAX_AGE_HOURS 的条目。
# 默认值：2000。设为 0 表示不限制。
SEED_MAX_ENTRIES = 5000000

# 种子淘汰比例（1/N）。超 SEED_MAX_ENTRIES 时淘汰末尾 1/N。
# 默认值：10（即 1/10）。越小淘汰越激进。
# 设为 0：超限时淘汰所有超龄条目，不限比例。
SEED_EVICTION_RATIO = 10

# ==================== 搜索阶段开关 ====================

# 阶段 1: 种子仓库阶段
SEED_STAGE_ENABLED = True
# 阶段 2: Code 文件搜索阶段
CODE_STAGE_ENABLED = True
# 阶段 3: 关键词搜索阶段
KEYWORD_STAGE_ENABLED = True

# 404 仓库持久化文件（跨运行跳过，避免每轮重复查询死链接）。
# 作用：repo info 返回 404 的仓库记录到文件，下轮直接跳过（_is_repo_dead），
#       不再重复消耗 API。文件在 _finalize 时重写（去重 + 上限 NOT_FOUND_REPOS_MAX）。
NOT_FOUND_REPOS_FILE = "not_found_repos.txt"
NOT_FOUND_REPOS_MAX = 5000

# ==================== 已处理仓库持久化 ====================

# 是否启用已处理仓库持久化（seen_cache）。
# 作用：跨运行跳过已处理的仓库，大幅节省 API 调用。
# 原理：启用后，每次遇到仓库会检查 seen_cache。
#       如果 {repo}.pushed_at 与缓存中的 pushed_at 相同 → 跳过整个 process_repo，
#       省去 tree API、commits API、raw 文件下载等所有后续步骤。
#       pushed_at 不同 → 重新处理（仓库有更新）。
#       不按时间淘汰——pushed_at 本身已是最精确的"过期"判断。
# 持久化文件：{SEEN_REPOS_DIR}/seen_XXXX.pkl（分片 pickle）
# 查看频率：process_repo 入口、种子/搜索/Code 入队前（_check_seen_cache 前置检查）
# 写入频率：process_repo 处理完毕（_mark_seen_cache）
# 默认值：True。强烈建议保持开启。
# 设为 False：每次运行从零开始，不跨运行跳过。
SEEN_REPOS_PERSIST_ENABLED = True

# 已处理仓库持久化目录（分片 pickle，和 sha_cache 相同格式）
SEEN_REPOS_DIR = "seen_cache"

# 已处理仓库每片最大字节数。
# 作用：超过时分片写入，控制单文件大小。
# 默认值：45MB（远低于 GitHub 100MB 硬限制）
SEEN_REPOS_MAX_BYTES = 45_000_000

# --- 排序与淘汰 ---

# 已处理仓库最大条目数。
# 作用：限制 seen_cache 总大小，防止无限增长。
# 原理：超限时从尾部（低命中 + 久未命中）淘汰 1/SEEN_CACHE_EVICTION_RATIO。
#       不按时间淘汰——pushed_at 比较已是最精确的过期判断。
#       即使缓存了半年的条目，只要 pushed_at 未变就正确跳过。
# 默认值：500,000。设为 0 表示不限制。
SEEN_CACHE_MAX_ENTRIES = 5000000

# 已处理仓库淘汰比例（1/N）。
# 作用：每次保存时，如果超过 SEEN_CACHE_MAX_ENTRIES，淘汰末尾 1/N 的低命中条目。
# 排序规则：hits（命中次数）降序 → last_hit（最后命中时间）降序。
#          高命中靠前（保留），低命中靠后（优先淘汰）。
# 默认值：20（即 1/20）。取值范围 5-100。越小淘汰越激进。
# 设为 0：超限时淘汰所有超限条目。
SEEN_CACHE_EVICTION_RATIO = 20

# ==================== 订阅链接发现 ====================

# 每个文件最多尝试几个发现的订阅链接（避免过度下载）
# 默认 50，取值范围 1-200。
SUB_URL_MAX_PER_FILE = 200

# ==================== 持久化安全 ====================

# 是否启用安全写入（先写 tmp 再 rename，防崩溃丢数据）
# 默认 True。
SAFE_WRITE_ENABLED = True

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
# 默认 10。08103 实证：串行(≤10 文件)仓库被 下载+解析 串行累积拖死
# （W-3 卡 3007s = 8 文件 × 375s），并行后总时间 ≈ 单文件时间。
# → 阈值降为 1：所有候选文件都并行下载（小仓库线程开销可忽略）。
PARALLEL_DOWNLOAD_THRESHOLD = 1

# 并行下载最大线程数（raw CDN 不限流，设大加速）
# 注意：总下载线程 = Worker 数 × 此值。24 Worker × 8 = 192 线程，
#       GA runner 7GB 内存安全（大仓库有 MB_HIGH/MED 动态降级）。
# 08102 试验 16 并发：触发 raw CDN 单连接限速（0.1MB/s，网络 7.65→0.31MB/s，
#       大文件 1000s+ 慢速下载不触发 read timeout）→ 回滚 8。
PARALLEL_DOWNLOAD_WORKERS = 8

# ==================== 共用线程池 ====================

# 共用线程池 Worker 数（处理仓库/fork/用户仓库等所有类型任务）。
# 作用：控制并发处理仓库的线程数。每个 Worker 独立持有 HttpClient。
# 原理：API 速率门（ApiRateGate）按 600/分钟 限速削峰，
#       Worker 数不受 API 速率约束，可放心加大。
#       并发上限 100（REST+GraphQL 共享），16 Worker × ~2 并发 = 32，安全。
# 默认值：24。建议范围 16-32。
# 次级限流保护：API 队列端点限速 + 遇到 403 自动降级。
# 注意：增大 Worker 时需同步调小 PARALLEL_DOWNLOAD_WORKERS，
#       避免并行下载线程总数（Worker × 下载线程）超出 GA runner 内存。
SHARED_POOL_WORKERS = 36

# 主队列最大长度（搜索渠道：种子/关键词/Code Search）。
# 作用：满时搜索线程阻塞（背压），防止搜索结果浪费 API 配额。
# 默认值：200。取值范围 50-500。
MAIN_QUEUE_SIZE = 200

# 发现队列最大长度（fork 链/用户仓库/raw 递归）。
# 作用：单源头仓库展开的全部扩展仓库的缓冲容量。
#       一次只处理一个主队列仓库（disc 清空才取下一个），
#       单源头展开量数学上界 ≈ MAX_TRACE_DEPTH 层 × 每层 ≤100 = 10000+。
# 注意：PriorityQueue，元素 (priority, ts, seq, item)，
#       priority 0 = 不需要追踪（先消费），1 = 需要追踪（后消费）。
# 默认值：20000。取值范围 5000-50000。
DISCOVERY_QUEUE_SIZE = 20000

# ==================== 队列调度策略 ====================

# 统一追踪层数（取代旧的 TRACE_ONCE_ONLY / fork、raw 分开限制）。
# 作用：控制扩展仓库的追踪深度。仓库标志位 [来源][层数] 决定是否继续追踪：
#   [userN]/[404userN] → 用户仓库，可追踪（只 fork/raw，不追踪 user）
#   [forkN]/[rawN]     → N < 此值：处理 + 继续追踪（像源头仓库）
#                        N ≥ 此值：直接处理不追踪
#   [种子仓库]/[kwN]/[cdN] → 源头，层数由 KEYWORD_TRACE_DEPTH/CODE_TRACE_DEPTH 决定
# 默认值：1（暂时只追踪一层，多层逻辑保留，后续可调大）。
# 取值范围 1-5。设 1 = 只追踪一层（等价旧 TRACE_ONCE_ONLY）。
MAX_TRACE_DEPTH = 1

# 关键词/Code 入主队列时的源头层级（行为与层级解耦）。
# 作用：决定这两类源头仓库入队后的行为——
#   0              = 按种子逻辑追踪到最大层级（记录 traced 0，30 天超期重追踪）
#   MAX_TRACE_DEPTH = 只解析这个仓库，不追踪（depth == MAX → 不再展开）
# 默认值：0。合法取值：0 或 MAX_TRACE_DEPTH。
KEYWORD_TRACE_DEPTH = 0
CODE_TRACE_DEPTH = 0

# 发现队列强制消费阈值。
# 作用：当发现队列超过此值时，所有 Worker 强制消费发现队列（不取主队列）。
# 原理：最后防线。正常情况（单源头 + 20000 容量）不会触发。
# 默认值：19000。取值范围 5000-{DISCOVERY_QUEUE_SIZE}。
# 设为 0：不强制消费，Worker 自由选择。
DISC_FORCE_CONSUME_AT = 19000

# 发现队列允许取主队列阈值。
# 作用：发现队列 ≤ 此值时，Worker 可以取主队列（补充源头）。
# 原理：disc 低于此值时队列快空了，Worker 取主队列补源头；
#       disc ≥ 此值时全部消费 disc（队列非空，Worker 满负荷不空转）。
#       源头数量自然受限：disc 增长后自动停止补充。
# 默认值：200（disc 低于 200 就积极补源头，扩展队列空的时间更少）。
DISC_MAIN_OK_AT = 1000

# 主队列取仓库冷却时间（秒）。
# 作用：Worker 取完一个主队列仓库后，必须等此时间才能再取。
# 原理：冷却期让源头处理产生的 disc 有时间积累（源头 ~60 秒展开 100 条），
#       disc 低于阈值（DISC_MAIN_OK_AT）且冷却结束才补充下一个源头。
#       锁（_main_take_lock）保证同一瞬间只有一个 Worker 执行取动作（原子性）。
# 默认值：60。建议 30-120。设 0 = 无冷却（不推荐，源头会过快补充）。
MAIN_TAKE_COOLDOWN = 10

# 同时允许几个源头仓库（从主队列取出正在处理）运行。
# 作用：限制扩展仓库的"产生方"数量，防队列膨胀。
# 原理：Semaphore(N)——Worker 取主队列前 acquire，处理完 release。
#       与 MAIN_TAKE_COOLDOWN（每 Worker 冷却）互补：冷却限频、此限并发。
# 默认值：2。设 0 = 不限制（等于 Worker 数）。
MAIN_SOURCE_LIMIT = 0

# 追踪重试间隔（天）。
# 作用：已追踪过（同层覆盖）的仓库，距上次追踪超过此天数 → 再追踪。
# 原理：仓库的 fork/用户/raw 会随时间变化，定期重追踪发现新扩展。
#       判断用 last_traced_at（最近一次任意层追踪时间）。
# 默认值：30。设 0 = 永不重追踪（只按层数覆盖）。
TRACE_RETRY_DAYS = 30

# 发现队列 put 背压阈值。
# 作用：队列 ≥ 此值时，_disc_put 等待（5 秒间隔）直到消费方腾出空间。
# 原理：最后防线（正常不会触发）。等待期间 item 不丢，条件满足后正常放入。
# 默认值：19500。取值范围 5000-{DISCOVERY_QUEUE_SIZE}。
DISC_PUT_BACKPRESSURE = 19500

# 次级限流自动降级。
# 作用：检测到 GitHub 次级限流（403 "secondary rate limit"）时，
#       自动降低 Worker 数到 DEGRADE_WORKERS，等 60 秒后恢复。
# 默认值：True。建议保持开启。
SECONDARY_RATE_LIMIT_DEGRADE = True

# 次级限流降级后的 Worker 数。
# 作用：触发次级限流时临时降到这个数量。
# 默认值：2。取值范围 1-4。
DEGRADE_WORKERS = 2

# ==================== API 速率门 ====================

# API 速率门全局速率上限（次/分钟）。
# 作用：所有 api.github.com 调用经滑动窗口限速，速率不超过此值。
# 原理：次级限流 900 点/分钟/端点（GET=1 点），600 留 33% 余量。
# 默认值：600。取值范围 300-900。
API_MAX_PER_MINUTE = 600

# API 速率门暂停阈值（次/分钟）。
# 作用：最近 60s 放行数 ≥ 此值 → Worker 暂停取新任务（削峰）。
# 原理：480 = 600 的 80%，接近上限前提前收手，在途请求消化后速率回落。
# 默认值：480。取值范围 300-{API_MAX_PER_MINUTE}。
API_PAUSE_AT_RATE = 480

# API 速率门恢复阈值（次/分钟）。
# 作用：速率 ≤ 此值 → Worker 恢复取任务。
# 原理：300 = 600 的 50%，滞回带（300-480）防止阈值附近反复抖动。
# 默认值：300。取值范围 100-{API_PAUSE_AT_RATE}。
API_RESUME_AT_RATE = 300

# ==================== API 配额管理 ====================

# 每小时最大 API 调用次数（留 200 余量给非关键调用）
# GitHub 认证用户限额 5000/小时，设为 4800 保证不触顶。
QUOTA_MAX_PER_HOUR = 4800

# ==================== API 请求超时设置 ====================

# 超时格式: (connect_timeout, read_timeout)，单位秒。
# connect_timeout: 建立 TCP 连接的最长等待。
# read_timeout:   收到第一个字节后，等待后续数据的最大时间。
#                 大响应/慢网络需要更长的 read_timeout。
# 注意：这些只影响单个 HTTP 请求。整体处理超时见 FILE_PROCESS_TIMEOUT /
#       REPO_TIMEOUT_SECONDS。

# 搜索 API（/search/repositories, /search/code）
# GitHub 搜索较慢（中文关键词 + 多页 + 100 条/页大 JSON）。
# connect=15s（GA 到 api.github.com 通常 <200ms，15s 容错），
# read=30s（搜索响应 JSON 可达数百 KB）。
SEARCH_TIMEOUT = (15, 30)

# 仓库信息 API（/repos/{owner}/{repo}）
# 单仓库元数据，响应小（~2KB），快速 API。
# 用途：种子信息、fork 链父仓库、raw 递归发现验证。
REPO_INFO_TIMEOUT = (8, 15)

# raw 文件下载（raw.githubusercontent.com，不计 API 配额）
# read=180s：极端 100MB 节点文件在慢网络（1MB/s）下需 100s+。
# 注意：下载超时 ≠ 解析超时（后者见 FILE_PROCESS_TIMEOUT）。
FILE_DOWNLOAD_TIMEOUT = (15, 180)

# 单个文件下载总时长上限（秒）。
# read timeout 只防"无数据"，CDN 慢速限速（0.1MB/s 持续送数据）不会触发——
# 100MB 文件能慢速下载 1000s+（08102 的 W-3 卡 2600s 根因）。
# 超过此上限放弃该文件（下次重试），避免 worker 卡死在慢速下载。
MAX_DOWNLOAD_SECONDS = 240

# 多进程解析池进程数。
# 解析(extract_all_strategies)是纯 Python CPU 密集——GIL 限制下多线程
# 解析永远只用 1 核（08104 实测 64 线程并发也仅 50% CPU）。多进程每个
# 进程独立 GIL，真正用满多核。2 核机器建议 2；content 经 pickle 传递
# （内存×2，~186MB/93MB 文件），配合 DOWNLOAD_MEMORY_BUDGET_MB 封顶。
# 影响：只影响解析速度与内存峰值，不影响正确性。可随时调整。
EXTRACT_PROCESSES = 2

# 下载/解析内存预算（MB）：正在解析 + 等待解析的文件 content 总大小上限。
# 08104：64-120 个文件并发解析，content(93MB×N) 占满 11GB → 内存爆。
# 超预算时下载线程等待（不发起新下载，URL 排队，不占内存）。
# 影响：只限制内存峰值，不影响正确性。可随时调整。
DOWNLOAD_MEMORY_BUDGET_MB = 2048

# 走进程池解析的最小文件大小（MB）。小于此值直接线程解析（pickle 开销
# 占比大，小文件进程池反而慢）；大于此值提交进程池（绕 GIL 用多核）。
EXTRACT_PROCESS_MIN_MB = 1

# Contents API（/repos/{repo}/contents/{path}）
# 回退路径，仅树 API 失败时使用。逐目录遍历，速度慢。
CONTENTS_API_TIMEOUT = (10, 20)

# Commits API（/repos/{repo}/commits）
# 获取分支最新 commit SHA + 24h 前 commit SHA。响应小（~1KB）。
COMMITS_API_TIMEOUT = (8, 12)

# Tree API（/repos/{repo}/git/trees/{branch}?recursive=1）
# 递归树，一次获取全仓库文件列表。大仓库树 JSON 可达数 MB。
# read=30s 覆盖大仓库。
TREE_API_TIMEOUT = (15, 30)

# ==================== 树 API 策略 ====================

# 是否使用递归树 API（`git/trees/{branch}?recursive=1`）
# 默认 True。一次调用获取全仓库文件树，失败时自动回退到 Contents API 逐层遍历。
USE_RECURSIVE_TREE = True

# ==================== 种子仓库 ====================

# 种子仓库文件路径（JSON 数组，每行一个 "owner/repo"）
# 作为数据文件独立存储，与代码分离，便于动态更新。
# 系统每次运行会将此文件中的仓库加入处理队列，并通过 sources.json 追踪产出。
SEED_REPOS_FILE = "seed_repos.json"

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

# ==================== 语言过滤 ====================

# 跳过主要语言在此集合中的仓库。
# 作用：纯 HTML 仓库（网站/静态页）几乎不含代理节点，跳过节省配额。
# 原理：GitHub 所有仓库 API 响应自带 language 字段（主要语言）：
#       - /search/repositories items
#       - /search/code repository
#       - /repos/{repo}（repo info）
#       - forks API、/users/{owner}/repos 条目
#       各渠道入队时带上 language，process_repo 入口统一过滤。
#       language 为空（None，GitHub 无法判断）→ 放行。
# 默认值：{"HTML"}。可加 "JavaScript"/"PHP" 等（误杀风险自担）。
SKIP_LANGUAGES = {"HTML"}

# ==================== Raw 链接递归发现 ====================

# 是否启用从 raw 链接反向发现仓库
# 默认 True。下载文件中引用的 raw 链接可能指向其他节点仓库。
ENABLE_RAW_RECURSIVE = True

# 最多递归发现的仓库数量
# 默认 500。README/聚合文件中的链接发现价值高，限制太狠会漏掉来源仓库。
# 注意：每个发现的仓库都会触发 repo info 查询（消耗 API 配额）。
# 递归深度不再单独配置——统一由 MAX_TRACE_DEPTH 控制（_should_trace），
# 防止 [raw2]/[raw3] 继续发现产生 [user3]/[raw4] 等超层条目。
MAX_RECURSIVE_REPOS = 500

# ==================== Partial Clone（大仓库处理） ====================

# 是否启用 git partial clone 处理 tree 截断的大仓库。
# 作用：tree API truncated（仓库 >10 万文件）时，
#       用 `git clone --filter=blob:none --depth 1` 只下载文件树（路径名），
#       `git ls-tree -r HEAD` 获取完整文件列表（零 API 配额），
#       然后 raw 下载候选文件（免费）。
# 原理：Contents API 逐目录遍历每个目录 1 次 API（大仓库 1000-3000 次），
#       partial clone 只消耗 git 网络流量（~100MB 级 tree 对象）。
# 默认值：True。建议保持开启。
PARTIAL_CLONE_ENABLED = True

# Clone-First 模式（试验开关）：所有需要解析的仓库直接 git clone 拿文件树，
# 跳过树 API 与 commits 过滤（零核心 API 消耗），符合后缀的文件全量下载解析。
# tree/commits 逻辑保留（False 时恢复原路径），便于对照效果。
# 默认值：False（原路径）。试验期开启。
CLONE_FIRST_MODE = True

# Partial Clone clone 超时（秒）。
# 默认 900（15 分钟）。大仓库 tree 对象 ~100MB 需 30-60s。
PARTIAL_CLONE_TIMEOUT = 900

# Partial Clone 同时并发数。
# 作用：限制同时进行的 git clone 数量，避免资源竞争（网络/磁盘/内存）。
# 原理：git clone --filter=blob:none 下载 tree 对象 + 本地索引，
#       并发过多会互相拖慢导致超时（曾 17 次 900s 超时）。
#       超时 kill 用进程组隔离（start_new_session），只杀自己的 git。
# 默认值：2。CLONE_FIRST_MODE 试验期曾设 30（08082 日志：2 核机器负载飙到
#       23.56 持续 100%，clone 失败 13 次）→ 降到 15 观察负载与吞吐平衡。
#       08091 分析：71 个任务耗时 >900s（clone 信号量排队），资源余量大
#       （CPU 1核/网络 7MB/s），ls-tree 请求风暴已修复 → 15→25 减排队。
PARTIAL_CLONE_CONCURRENCY = 25

# 是否回退到 Contents API 逐目录遍历。
# 作用：tree 失败 + Partial Clone 失败时的最后手段。
# 原理：tree+clone 都失败的仓库必然超大（几万目录），
#       Contents 遍历每目录 1 次核心 API → 配额黑洞（4800/小时被单仓库吃光）。
#       默认关闭：tree+clone 失败直接放弃（结果在汇总展示）。
# 默认值：False（关闭，避免配额黑洞）。开启需谨慎。
CONTENTS_API_FALLBACK_ENABLED = False

# 监控输出间隔（秒）。
# 作用：监控线程每此间隔打印一次系统状态（CPU/内存/网络/API/队列/Worker）。
# 默认值：60。建议 30-120。
MONITOR_INTERVAL = 60

# ==================== 输出配置 ====================

# 批次刷盘阈值（buffer 中累积到此数量自动写入文件）
# 默认 10000，取值范围 1000-50000。
# 08105 后 5000→10000：分片数量减半（1120 → ~560），减少小分片比例；
# 内存代价极小（buffer 多 5000 条字符串，约 1MB）。
BATCH_FLUSH_SIZE = 10000

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

# 队列满时等待超时（秒）。
# fork 链/用户仓库往队列放任务时，队列满最多等这么久。
# 超时 → 丢弃该任务（下次运行可能再发现）。
# 默认 10，取值范围 1-60。
QUEUE_PUT_TIMEOUT_SECONDS = 60

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
