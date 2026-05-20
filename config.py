"""
全局配置 —— 可调参数集中管理
"""

# ==================== 测速配置 ====================
# mihomo 二进制版本（推荐稳定版）
MIHOMO_VERSION = "v1.18.7"
# 下载地址（根据平台自动选择，Linux amd64）
MIHOMO_URL = f"https://github.com/MetaCubeX/mihomo/releases/download/{MIHOMO_VERSION}/mihomo-linux-amd64-{MIHOMO_VERSION}.gz"
# 解压后的二进制文件名（脚本会自动处理）
MIHOMO_BIN = "mihomo"
# 本地代理端口（mihomo 启动的 HTTP 代理端口，用于测速时下载）
MIXED_PORT = 7890
# API 端口（用于控制测速）
API_PORT = 9090
# 测速用的 HTTP 延迟测试 URL（需稳定、快速）
LATENCY_TEST_URL = "https://www.gstatic.com/generate_204"
# 测速用的下载 URL（推荐 10MB 文件，可更换为自建或公共测速文件）
SPEED_TEST_URL = "https://speed.cloudflare.com/__down?bytes=10485760"
# 超时设置（毫秒）
LATENCY_TIMEOUT = 5000         # 延迟测试超时
SPEED_TIMEOUT = 15000          # 速度测试超时
# 最小下载字节数（5MB），低于此值视为测速失败
MIN_DOWNLOAD_BYTES = 5 * 1024 * 1024

# ==================== 过滤与输出 ====================
# 延迟阈值（毫秒），超过此值丢弃
MAX_LATENCY = 3000
# 速度阈值（MB/s），低于此值丢弃
MIN_SPEED_MB = 0.5             # 0.5 MB/s

# ==================== 路径配置 ====================
# no 文件夹内的分片大小（每文件最多节点数）
CHUNK_SIZE = 10000
# 输出文件名
ALIVE_NODE_FILE = "alive.txt"          # 存活节点原始链接列表
FILTERED_NODE_FILE = "fi_no.txt"        # 经过延迟和速度过滤后的节点文件
FILTERED_LINKS_FILE = "fi_no_w_li.txt"  # 过滤后节点的分片链接索引
