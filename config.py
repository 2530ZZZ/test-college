"""
全局配置 — 集中管理所有可调参数。

设计原则：
  1. 纯常量 — import 本模块不会触发任何网络请求或副作用。
  2. 详细注释 — 每个参数注明：作用、原理、默认值、取值范围、配置建议。
  3. 分类清晰 — 按功能模块分组，便于查找。

版本号等需要网络获取的值统一通过环境变量传入，或在使用处动态获取。

⚠️ 阅读约定（重要）：
  - 注释中的"当前值"即赋值语句的值（已全部对齐）；"历史曾 X"只作
    演进线索，完整演化故事见 docs/DESIGN.md §7 事故时间线。
  - 注释里带日志编号（如 08102/08111）的是演进记录，说明"为什么
    从旧值改成新值"，新对话分析行为时以此为准。
  - 设计协同（改一个值会影响什么）见 docs/DESIGN.md §1-§6 子系统
    与 §8 配置值总表（连锁影响列）。


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
# 当前值 7200（取值上限）：配额耗尽时的策略是 wait_for_reset 等 UTC 整点
# 重置（§1），单次等待最多 ~60 分钟，7200 给满余量；本参数只兜底 403
# 处理路径的累计等待（历史曾推荐 3600）。
# 取值范围 600 - 7200。
MAX_TOTAL_RATE_LIMIT_WAIT = 7200

# 程序最大运行时间（秒），超出后停止搜集、开始保存。
# GA 默认超时 6 小时（21600s）。
# 当前值 19800（5.5 小时，历史曾 14400/18000）：只留 30 分钟收尾，
# 贴满 GA 6h 上限；兜底是 workflow"运行搜集"步骤 timeout=340 分钟——
# 卡死时 push 步骤（always()）仍会执行，不丢产出（§6）。设为 0/None 不限制。
MAX_RUNTIME_SECONDS = 19800

# ==================== 搜集渠道开关 ====================

# 是否启用 GitHub 搜索（搜索仓库 → 下载文件 → 提取节点）
# 默认 True。关闭时可单独调试网页/TG 渠道。
GITHUB_SEARCH_ENABLED = True

# ==================== GitHub 搜索配置 ====================

# 每个关键词搜索的最大页数
# 当前值 5。每页 100 条（PER_PAGE），5 页 = 最多 500 个仓库/词。
# 历史曾用 3 页（注释里的"30 条/90 个"为早期取值，PER_PAGE 也已调为 100）。
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
# 当前值 100（历史曾 20）：即"候选 >100 才做 commits 24h 过滤"，大仓库
# 只下增量控 raw 连接总量；下载量控制靠连接节流而非截断（§2）。
# 取值范围 5-100。
MAX_RAW_DOWNLOADS_PER_REPO = 100

# ==================== 文件收集配置 ====================

# 允许处理的文件扩展名（集合，包含空字符串支持无扩展名文件）
ALLOWED_EXTENSIONS = {
    '.yaml', '.yml', '.json', '.txt', '.md', '.conf', '.list', '.base64', ''
}

# 最大下载文件大小（字节），None 表示不限制
# 默认 None。限制值可设为 5MB = 5242880。
# 保持 None 不设上限是刻意的："解析节点是第一原则"的用户约定——
# 100MB 节点文件是正常产出，不能跳过（见 docs/DESIGN.md 用户偏好 4）。
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
# 当前值 80（历史曾 30）。子 fork 最直接相关，高产概率最高。
# 调大 → 每源头 forks API 消耗上升（计配额，§5 连锁警告）。
# 取值范围 5-100。
FORK_CHILD_MAX = 80

# 每个仓库最多查几个兄弟 fork（同父仓库下的其他 fork）
# 当前值 80（历史曾 20）。兄弟 fork 相关度低于子 fork，配额省给子 fork。
# 取值范围 5-100。
FORK_SIBLING_MAX = 80

# 每个仓库最多查几个 fork（总上限，兜底用）
# 当单独限制未生效时使用此值。当前值 100（历史曾 30）。取值范围 10-100。
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
# 当前值 1（历史曾 0）：至少产出 1 个节点才收录——过滤"解析了但啥也
# 没有"的仓库，但 1 个节点也是真实产出，予以保留。
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
# 当前值 500 万（历史曾 2000）：种子库大幅放宽，淘汰主要靠
# SEED_MAX_AGE_HOURS=720（30 天）时间维度，条数几乎不设限。设为 0 不限制。
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
# 当前值 500 万（历史曾 50 万）：每条约 100 字节，500 万条约 500MB 量级，
# 远低于 GA 15.6GB 内存；放宽可减少低命中仓库被过早淘汰后重复查询
# （每条淘汰后续又命中 = 重复 API 消耗）。设为 0 表示不限制。
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
# 当前值 200（历史曾 50）：订阅链接发现是零 API 的高价值路径（raw HTTP
# 直连第三方订阅源），放宽限制多抓几个；上限防单个文件带几百个链接
# 时连接风暴。取值范围 1-200。
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
# 当前值 5（历史曾 2；README 命中精度极高，放宽页数收益大）。取值范围 1-5。
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
# 当前值 5（历史曾 3；Code Search 命中精度几乎 100%，放宽页数直接
# 增加命中仓库数）。取值范围 1-5。
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
# 当前值 72（历史 8→4→36→72，演进见 §7）：为 72 worker 配套 576 个
# 潜在下载线程——下载不因信号量许可排队过狠；真正限吞吐的是连接节流
# （MAX_RAW_DOWNLOAD_CONNECTS_PER_SEC），Worker 数只管任务处理（§2）。
# 次级限流保护：API 队列端点限速 + 遇到 403 自动降级。
# 注意：增大 Worker 时需同步评估 PARALLEL_DOWNLOAD_WORKERS 与
#       MAX_DOWNLOAD_CONCURRENCY——三者乘积决定潜在下载线程与内存峰值。
SHARED_POOL_WORKERS = 200

# 主队列最大长度（搜索渠道：种子/关键词/Code Search）。
# 作用：满时搜索线程阻塞（背压），防止搜索结果浪费 API 配额。
# 08141：200→3000（work 200 时队列要够装需 API 任务，配合末段消耗策略）。
MAIN_QUEUE_SIZE = 3000

# 搜索入队阈值：主队列 ≥ MAIN_QUEUE_PAUSE_AT 暂停搜索翻页，
# < MAIN_QUEUE_RESUME_AT 恢复（_wait_queue_slot 用）。
# 08141：20/80 → 1000/2500（大队列下的滞回，防搜索频繁启停）。
MAIN_QUEUE_PAUSE_AT = 2500
MAIN_QUEUE_RESUME_AT = 1000

# 配额末段策略：距整点剩 QUOTA_ENDGAME_MINUTES 分钟且核心 API 还有剩余时，
# worker 允许取主队列 + 扩展队列需 API 仓库优先消费（消化配额，避免整点
# 浪费）。整点/配额耗尽后回到常规策略（零 API 优先、disc 优先）。
QUOTA_ENDGAME_MINUTES = 10

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
# 阈值方案来源（决策 14）：disc≥阈值强制消费 disc、低于 DISC_MAIN_OK_AT
# 优先主队列、中间自由竞争；早期队列容量 600 时阈值是 400/100，
# 容量扩到 20000 后按比例放大为 19000/1000。
DISC_FORCE_CONSUME_AT = 19000

# 发现队列允许取主队列阈值。
# 作用：发现队列 ≤ 此值时，Worker 可以取主队列（补充源头）。
# 原理：disc 低于此值时队列快空了，Worker 取主队列补源头；
#       disc ≥ 此值时全部消费 disc（队列非空，Worker 满负荷不空转）。
#       源头数量自然受限：disc 增长后自动停止补充。
# 当前值 1000（历史曾 200/100）：与 DISC_FORCE_CONSUME_AT=19000 配套，
# 早期队列容量 600 时阈值是 400/100，容量扩到 20000 后按比例放大（§4）。
DISC_MAIN_OK_AT = 1000

# 主队列取仓库冷却时间（秒）。
# 作用：Worker 取完一个主队列仓库后，必须等此时间才能再取。
# 原理：冷却期让源头处理产生的 disc 有时间积累（源头 ~60 秒展开 100 条），
#       disc 低于阈值（DISC_MAIN_OK_AT）且冷却结束才补充下一个源头。
#       锁（_main_take_lock）保证同一瞬间只有一个 Worker 执行取动作（原子性）。
# 当前值 1（历史 60→10→1）：72 worker 下冷却太长会让源头补充太慢；
# 且冷却只在 disc 非空时生效（disc 空 = 无追踪活动，冷却无意义——
# 08112 发现主队列满+disc 空时 16 个 work 空转 30 秒等冷却，§4）。
MAIN_TAKE_COOLDOWN = 0

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

# API 速率门 core 核心 API 速率上限（次/分钟）。
# 作用：**仅限 core 类端点**（repos/tree/contents/commits/forks/users）经
# 滑动窗口限速，速率不超过此值。search 端点只受端点级 30/min 限制
# （不占 core 窗口），raw 下载免费不限速（08171 修正：原 600 混算所有
# 类型且远高于均值 83/min，形同虚设——200 worker 突发 12.8/s 触发
# GitHub 次级限流 4 次，冻结 52 分钟）。
# 08173 修正：200→300——200 太紧导致启动瞬间 200 worker 的 tree 请求
# 排队 60s+（窗口满后新请求全等过期），自旋超时批量触发。300/min=5/s
# 持续，仍远低于 08171 触发线 12.8/s（768/min）的 40%；配合并发限制
# （API_MAX_CONCURRENCY）防瞬时峰值，总量防持续超发（08173 讨论确认：
# 只有并发时 50 并发×1s=50/s=3000/min 会超触发线，总量不能去掉）。
# 默认值：300。取值范围 100-600。
API_MAX_PER_MINUTE = 300

# API 速率门暂停阈值（次/分钟）。
# 作用：最近 60s core 放行数 ≥ 此值 → Worker 暂停取新任务（削峰）。
# 原理：240 = 300 的 80%，接近上限前提前收手，在途请求消化后速率回落。
# 默认值：240。取值范围 100-{API_MAX_PER_MINUTE}。
API_PAUSE_AT_RATE = 240

# API 速率门恢复阈值（次/分钟）。
# 作用：core 速率 ≤ 此值 → Worker 恢复取任务。
# 原理：150 = 300 的 50%，滞回带（150-240）防止阈值附近反复抖动。
# 默认值：150。取值范围 50-{API_PAUSE_AT_RATE}。
API_RESUME_AT_RATE = 150

# API 在途并发上限（08173 讨论确认）：已发出未收到响应的 API 请求数
# （进行中，含 core+search；raw 走独立 96 信号量不计入）。
# GitHub 次级限流：REST+GraphQL 并发 ≤100；官方建议 50 concurrent
# operations；且 CPU 时间 90s/60s（按总响应时间估算）约束下
# 50 并发 × 1.5s/请求 = 75s < 90s ✓（60 并发 × 1.5s = 90s 恰好撞线）。
# 08171 的 202 并发 403 即超 100 上限的根因修复。
# 默认值：50。取值范围 30-80。
API_MAX_CONCURRENCY = 50

# Search API 子端点限速（官方文档明确区分）：
# - search/code = 10/min（认证）——代码搜索限制更严
# - 其他 search（repositories/issues/commits）= 30/min（认证）
# 来源：https://docs.github.com/en/rest/search/search
# 当前值：search_code 10、search_other 30（官方硬限制，不可调高）。
SEARCH_CODE_PER_MINUTE = 10
SEARCH_OTHER_PER_MINUTE = 30

# 监控块落盘文件（08171：LogSink 消费者 print 崩溃 → stdout 静默 3.5 小时，
# 监控块双通道：除 stdout 外追加写此文件，日志通道死了也能确认程序存活）。
LOG_MONITOR_FILE = "log/monitor.log"

# API 速率门停摆超时（秒）：http_client 在 api_gate.acquire 拒绝时自旋等待，
# 只有当"速率门窗口完全停摆"（最近 10s 内零放行）持续超过此值 →
# 放弃本次请求（return None，不强制放行——08173 实证强制放行绕过限速；
# 08172 的 gate 计数 bug 曾致无限自旋 → 主线程卡死空转 3.5 小时）。
# 08241 修正语义：不再按"总等待 90s"放弃——200 worker 并发时请求积压
# 是正常排队（窗口 300/min 每 0.2s 滚动放行一个，排队深但窗口在动），
# 08241 实测 90s 等待误杀 1578 次（user repos 追踪全失效）。改成只盯
# "窗口是否在滚动"：滚动 = 正常排队，继续等；停摆 90s = 真异常（计数
# 泄漏），才放弃。GATE_STALL_TIMEOUT_SECONDS = 90（沿用原值，语义不同）。
GATE_STALL_TIMEOUT_SECONDS = 90

# 收尾时等下载队列清空的超时（秒）：_stop_download_workers 等队列空。
# 08241 实测：积压 5.5 万文件，join() 无超时 → 收尾卡 ~25 分钟，
# 拖到 GA 6h 上限被杀，_finalize 的统计/缓存（最后步骤）被截断。
# 081XX 修正 60→30s：收尾链路总预算 = SIGTERM 的 120s 兜底
# （main.py），join worker 20s + drain 30s = 50s，给 _finalize 留
# ~70s（否则 _finalize 被 os._exit 截断，stats/run_stats.txt 丢失——
# 08252 实测统计缺失的根因）。超时后放弃剩余任务：已解析分片已
# 落盘（不丢），未解析文件 SHA 未写缓存（下次运行重抓），数据安全。
DOWNLOAD_DRAIN_TIMEOUT_SECONDS = 30

# 收尾时 join worker 的超时（秒）：5.5h 自动终止后等 worker 退出。
# 终止时 worker 可能卡在当前仓库（future.result 300s / clone / 下载），
# 死等会拖到 GA 上限。081XX 修正 60→20s（与 drain 30s 合计 50s，
# 给 _finalize 留 ~70s，见 DOWNLOAD_DRAIN_TIMEOUT_SECONDS 注释）。
# 超时后直接进 _finalize——worker 是 daemon 线程，main.py 最后
# os._exit(0) 兜底强杀残留。
WORKER_JOIN_TIMEOUT_SECONDS = 20

# 看门狗/OOM 线程栈转储落盘文件：faulthandler 转储写 stderr 会被 GA
# 日志管道丢弃（08192 的 44 次看门狗转储全丢失，看不到卡在哪个正则）。
# 落盘后每轮都能拿到完整线程栈，定位卡死/慢解析的直接证据。
WATCHDOG_DUMP_FILE = "watchdog_dump.log"

# ── 源头背压（081XX 第 3 批）：下载 >> 解析时停止 worker 取新仓库 ──
# 背景：08241/08242/08243 实测下载队列积压 5.5 万-10 万（冲破 10 万上限），
# 下载 >> 解析吞吐（线程池 GIL 1 核），worker 卡在入队重试 → 后期 CPU
# 3-5% 空转、积压到 6h 都消化不完。
# 触发（任一）即背压：下载队列待处理 ≥ 5000 或 下载中+解析中内存 ≥ 2000MB
# 恢复（任一）即解除：队列 < 1000 或 内存 < 500MB
# 滞回：触发值 > 恢复值（5000→1000、2000→500），防临界点反复横跳。
# 背压只停"取新仓库"，结果队列照常消费（仓库完成事件不积压）。
DOWNLOAD_BACKPRESSURE_QUEUE = 5000          # 触发：下载队列文件数 ≥ 此值
DOWNLOAD_BACKPRESSURE_MEM_MB = 2000         # 触发：下载中+解析中内存 ≥ 此值(MB)
DOWNLOAD_BACKPRESSURE_RESUME_QUEUE = 1000   # 恢复：队列 < 此值
DOWNLOAD_BACKPRESSURE_RESUME_MEM_MB = 500   # 恢复：内存 < 此值(MB)

# 仓库级结果队列容量（081XX 第 3 批）：每仓库一条"处理完成"事件（量小，
# 一个仓库一条），2000 足够——事件积压说明 work 消费不过来，容量留余量。
RESULT_QUEUE_SIZE = 2000

# ==================== API 配额管理 ====================

# 每小时最大 API 调用次数（留 200 余量给非关键调用）
# GitHub 认证用户限额 5000/小时，设为 4800 保证不触顶。
# 留 200 余量的另一层原因：程序启动时配额可能已被共享 PAT 的其他任务
# 消耗（多轮 GA 共用 token），卡在 5000 边界会触发 403（决策 1）。
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

# 下载 0 字节窗口（秒）：连接建立后持续 idle_max_s 无任何数据 → 立即放弃。
# 08111 实测：raw CDN 限流时连接挂着但不给数据（0MB），read timeout 因
# 持续空 chunk 不触发，只能靠 MAX_DOWNLOAD_SECONDS 死等 240s×259 线程。
# 此窗口把"无数据挂起"快速判定为限流，配合退避（collector 内实现）。
# 影响：30s 内 0 字节基本就是挂了；正常下载首字节 <1s，不会误杀。
DOWNLOAD_IDLE_TIMEOUT = 30

# 全局下载并发上限：worker × 每仓库 8 线程（+clone 25）无上限叠加
# 可达 259 并发（08111 实测），高并发堆积触发 CDN 限流。此信号量封顶。
# 影响：只限制并发与排队时长，不影响正确性。可随时调整。
# 当前值 96（历史 64→96）：只是信号量许可数，为 72 worker 配套（576 个
# 潜在下载线程不因许可排队过狠）；真正限吞吐的是连接节流
# MAX_RAW_DOWNLOAD_CONNECTS_PER_SEC（§2）。acquire 必须带超时——08112
# 退避逻辑泄漏许可导致 36 worker 全部永久阻塞 2h。
MAX_DOWNLOAD_CONCURRENCY = 96

# 待下载队列容量（08174 异步下载管道）：worker 把确认要下载的 raw 链接
# 丢进队列，下载线程从队列取。队列放链接不占内存（10 万条 ≈ 几 MB）。
# 队列满 → worker 阻塞等待（可中断，收尾能退出）——背压反推，防止
# 无限积压。先进先出（旧文件先下载，防旧数据滞留）。
DOWNLOAD_QUEUE_SIZE = 100000

# 下载线程数（08174 异步下载管道）：从待下载队列取链接执行 raw 下载。
# 下载是 IO 等待（不占 CPU），2 核机器上 48 线程足够喂饱 30/s 连接节流；
# 原结构（96 信号量 + 每仓库 4-8 线程叠加）实测并发上限 259 触发 CDN
# 限流（08111），固定 48 线程从源头锁死并发。
DOWNLOAD_WORKER_THREADS = 48

# 解析共享线程池大小（08174）：小文件（<EXTRACT_PROCESS_MIN_MB）解析
# 不再"每文件临时开 1 线程"（无上限隐患），改为共享固定线程池。
# 依据 08181 数据：解析瞬时并发峰值 87（启动高峰）、单文件 0.02-0.05s
# ——32 线程足够覆盖（GIL 下线程解析不并行，再多不加速只增调度开销）。
PARSE_THREAD_POOL_SIZE = 32

# 解析看门狗超时（秒，08174）：文件内容进入 CPU 解析后开始计时，超过
# 此值未完成 → 触发线程转储（只打印不取消）。依据 08181 数据：小文件
# 解析 0.02-0.05s（近 60s 3171 文件/290MB），大文件（>1MB 进程池）几分钟
# 正常——300s 小文件必是卡死、大文件可能未完（打印等它自己完成）。
PARSE_WATCHDOG_SECONDS = 300

# 解析耗时分布的分位区间数（081XX）：每 (100/此值)% 文件一个区间。
# 目的：发现解析慢的文件——08241 实测 45MB 文件解析 21s+ 被"平均/最大"
# 统计掩盖（大量快文件拉低平均），分位分布能看到最慢 5% 的耗时区间；
# 最慢 10 个明细可直接复现改进解析算法（配合 watchdog_dump.log 线程栈）。
# 当前值：20 = 每 5% 一个区间。
PARSE_TIME_PERCENTILES = 20

# 限流检测：近 60 秒内下载失败（0 字节/连接错误/超时/HTTP 错误）总数
# 达到此值 → 触发退避。退避窗口（DOWNLOAD_THROTTLE_SECONDS）内下载并发
# 减半，且不重复触发（不续期）。
# 08111 的 20:37-20:41：raw CDN 限流 5 分钟全挂。退避降低连接压力，
# 避免"并发越高被限越狠"的恶性循环。
# 08112：原"连续 5 次"共享计数被 288 个下载线程共用 → 2 秒刷屏 12 次
# 退避 + 窗口续期 + 信号量泄漏；改为 60s 窗口失败总数检测（20 次/60s
# 才算限流，正常波动不触发）。
DOWNLOAD_STALL_THRESHOLD = 20
DOWNLOAD_THROTTLE_SECONDS = 60


# 多进程解析池进程数。
# 解析(extract_all_strategies)是纯 Python CPU 密集——GIL 限制下多线程
# 解析永远只用 1 核（08104 实测 64 线程并发也仅 50% CPU）。多进程每个
# 进程独立 GIL，真正用满多核。2 核机器建议 2；content 经 pickle 传递
# （内存×2，~186MB/93MB 文件），配合 DOWNLOAD_MEMORY_BUDGET_MB 封顶。
# 081XX：4→6——2 核物理上限下，进程数略多于核数让"等 pickle 传输"
# 的间隙被利用（子进程管道 I/O 等待时让出 CPU）；大文件占位时冗余
# 进程并行小文件。内存：6 子进程 × 各自任务副本，仍受 2GB 预算管控。
# 影响：只影响解析速度与内存峰值，不影响正确性。可随时调整。
EXTRACT_PROCESSES = 6

# 进程池排队降级阈值（08174）：进程池排队任务数 > 此值 → 新大文件改
# 线程解析（进程池被大文件占死、小文件进不去时，让池轮转起来）。
# 08191 实测：进程池 2/2 满、排队 305 个——大文件 pickle 传输慢（96MB
# content 传给进程），池被占死 → 小文件只能线程（GIL 1 核）→ CPU 51%。
# 排队 > 此值 → 新大文件走线程解析（慢但保证轮转）。
# 当前值 20；取值范围 5-100。
PROCESS_QUEUE_MAX = 20

# 下载/解析内存预算（MB）：下载中 + 正在解析 + 等待解析的文件 content
# 总字节上限（08174 修正：按字节计数——旧版用 len(str) 字符数当字节，
# 中文 1 字符占 3 字节，预算 2048 实际对应 4-6GB 内存）。
# 08104：64-120 个文件并发解析，content(93MB×N) 占满 11GB → 内存爆。
# 超预算时下载线程在"取链接前"检查（背压，见 DOWNLOAD_MEMORY_BACKPRESSURE_MB），
# 不够就不取（不发起新下载，链接留在队列，不占内存）。
# 影响：只限制内存峰值，不影响正确性。可随时调整。
DOWNLOAD_MEMORY_BUDGET_MB = 2048

# 内存背压余量（MB，08174）：下载线程取链接时的检查阈值 =
# DOWNLOAD_MEMORY_BUDGET_MB - 此值。留余量给"刚下载完还没提交解析"、
# "下载中 size 预估不准（partial clone size=0）"的浮动——避免实际内存
# 刚好顶在 2GB 上反复抖动（下载-释放-下载的振荡）。
# 当前值 256（预算的 12.5%）；取值范围 64-512。
DOWNLOAD_MEMORY_BACKPRESSURE_MB = 256

# 走进程池解析的最小文件大小（MB）。小于此值直接线程解析（pickle 开销
# 占比大，小文件进程池反而慢）；大于此值提交进程池（绕 GIL 用多核）。
EXTRACT_PROCESS_MIN_MB = 1

# ==================== 仓库处理分流（08113） ====================

# 仓库大小分流阈值（MB）：size < 阈值的仓库用 partial clone 拿文件列表
# （连接少、零 API）；≥ 阈值的仓库用 tree API 拿列表（大仓库 clone 元数据
# 大、tree 响应可承受；tree 失败回退 clone）。阈值先用 50MB，等
# _candidate_hist/_repo_size_hist 分布统计数据校准（_finalize 输出）。
SMALL_REPO_CLONE_MB = 500

# 全量下载阈值（MB）：size < 阈值的仓库 → 全量 clone（不 partial，
# checkout 工作区）→ 本地遍历候选后缀文件解析（零 API、不占 raw 速率）。
# 收益：配额耗尽时 work 的零 API 供给 +1 种；小仓库全量 clone 流量可控
# （≤阈值 MB/仓库，git 协议）。
# 阈值分层：< FULL_CLONE_MB 全量 clone / [FULL_CLONE_MB, SMALL_REPO_CLONE_MB)
# partial clone（拿列表 + raw 下载候选）/ ≥ SMALL_REPO_CLONE_MB tree。
FULL_CLONE_MB = 100

# 全量 clone 磁盘警戒（GB）：工作区可用 < 此值 → 暂停新的全量 clone
# （GA 磁盘 70GB+，全量 clone 处理完即删，正常不会到警戒线）。
FULL_CLONE_DISK_MIN_GB = 20

# ==================== 取样跳过无关仓库（08142） ====================

# 候选文件数超过此值 → 取样判断：各后缀取 SAMPLE_PER_EXT 个文件（不同
# 目录，代表性）下载解析；取样全部无节点 → 跳过该仓库并加入无节点
# 黑名单（no_node_repos.txt，30 天重试）。
# 背景：08142 的 deepseek-harness 系列（80+ fork × 3700-4355 候选文件）
# 是配置/数据文件（匹配后缀但无节点）——每仓库 30-90 分钟拖垮吞吐。
SAMPLE_THRESHOLD = 50

# 每后缀取样文件数（不同目录轮询选取）。
SAMPLE_PER_EXT = 10

# 无节点黑名单持久化文件（每行 repo<TAB>unix时间戳，超 NO_NODE_RETRY_DAYS
# 天重新尝试——仓库可能"这次无节点下次有"）。
NO_NODE_REPOS_FILE = "no_node_repos.txt"
NO_NODE_RETRY_DAYS = 30

# 全局 raw 下载连接速率（每秒新连接数）：08113 实测 36 连接/s 触发 CDN
# 慢速限速；30/s = 限速线的 80% 余量，动态降级（限速信号）时自动减半。
# 只限 raw 下载（_dl_rate_wait）——clone 走 git 协议另一条通道，不受此限。
# 下载信号量（MAX_DOWNLOAD_CONCURRENCY）控制并发，此节流控制连接建立
# 频率（全局令牌桶，见 collector._dl_rate_wait）。
MAX_RAW_DOWNLOAD_CONNECTS_PER_SEC = 30

# ==================== 收尾去重（no/） ====================

# 08141：7 天窗口（no_his）已回退——no_his 存储/提交过大，用户改为
# 本地自行合并 7 天节点。收尾只做"本轮批次单次去重"写 no/。
# 说明：no_his 相关代码已删除（模块级函数回退见 collector）。

# no/ 每个分片的节点行数。
NO_SPLIT_SIZE = 5000

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
# 当前值 900（15 分钟）。大仓库 tree 对象 ~100MB 需 30-60s。
PARTIAL_CLONE_TIMEOUT = 900

# Partial Clone 同时并发数。
# 作用：限制同时进行的 git clone 数量，避免资源竞争（网络/磁盘/内存）。
# 原理：git clone --filter=blob:none 下载 tree 对象 + 本地索引，
#       并发过多会互相拖慢导致超时（曾 17 次 900s 超时）。
#       超时 kill 用进程组隔离（start_new_session），只杀自己的 git。
# 当前值 25（演进：30→15→25，见 §7）。08082 日志 2 核机器负载飙到 23.56
# → 降到 15；08091 分析 71 个任务耗时 >900s（信号量排队而非超时）、资源
# 余量大（CPU 1 核/网络 7MB/s）→ 15→25 减排队。
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
# 实际间隔自适应（决策 34）：配额耗尽 + 60 秒无日志活动（_last_activity
# 信号）时降到 10 分钟一条，其余保持 60 秒——"静默"是真实信号，只有日志
# 活动能证明程序还活着；监控块本身不走 _wlog，不会自我污染。
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
# 当前值 5000（与 NO_SPLIT_SIZE 一致；注释中"默认 10000"为早期取值）。
# 取值范围 1000-50000。
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
# 当前值 60（历史曾 10/30）：put 必须带超时——无上限等待在队列满时
# 死锁直到 GA 6h 强杀（死锁史见 §4）。取值范围 1-60。
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
# ⚠️ 08174 异步管道后已失效：原"按仓库大小降并行下载线程"的代码被
# 待下载队列取代（worker 只入队不下载）。该职责现由全局内存预算承担
# （DOWNLOAD_MEMORY_BUDGET_MB + 背压检查，下载线程取链接时判断）——
# 保留此配置仅为历史说明，不再被代码读取。
PARALLEL_DOWNLOAD_MB_HIGH = 500
PARALLEL_DOWNLOAD_MB_MED = 200
