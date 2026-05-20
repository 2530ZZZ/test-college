"""
全局配置 —— 所有可调参数集中管理
"""

# ==================== 测速配置 ====================
MIHOMO_VERSION = "v1.18.7"
MIHOMO_URL = f"https://github.com/MetaCubeX/mihomo/releases/download/{MIHOMO_VERSION}/mihomo-linux-amd64-{MIHOMO_VERSION}.gz"
MIHOMO_BIN = "mihomo"
MIXED_PORT = 7890
API_PORT = 9090

# 测速 URL
LATENCY_TEST_URL = "https://www.gstatic.com/generate_204"
SPEED_TEST_URL = "https://speed.cloudflare.com/__down?bytes=10485760"

# 超时（毫秒）
LATENCY_TIMEOUT = 5000
SPEED_TIMEOUT = 15000
MIN_DOWNLOAD_BYTES = 5 * 1024 * 1024   # 5MB

# ==================== 过滤阈值 ====================
MAX_LATENCY = 3000          # 毫秒
MIN_SPEED_MB = 0.5          # MB/s

# ==================== GeoIP 国家识别 ====================
# GeoLite2-City.mmdb 下载地址（免费数据库，需遵守 MaxMind 许可）
GEOLITE_DB_URL = "https://git.io/GeoLite2-City.mmdb"   # 可能失效，请自行提供稳定链接
# 本地文件名
GEOLITE_DB_PATH = "GeoLite2-City.mmdb"
# 若无法获取离线库，是否使用在线 API（ip-api.com，免费限额 45次/分钟）
USE_ONLINE_IP_API = False   # 建议保持 False，使用离线库或域名推测

# ==================== 输出配置 ====================
CHUNK_SIZE = 10000
ALIVE_NODE_FILE = "alive.txt"          # 存活节点名称列表（带国旗）
FILTERED_NODE_FILE = "fi_no.txt"       # 过滤后的原始链接
FINAL_OUTPUT_FILE = "jd.txt"           # 最终输出：国旗 国家_序号 | 原始链接
