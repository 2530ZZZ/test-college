"""
全局配置 —— 集中管理所有可调参数和外部工具版本。
mihomo 和 subconverter 均使用 GitHub API 自动获取最新稳定版。
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
    返回格式如 "v1.19.24"。
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
    return "v1.18.7"  # 回退版本


def _fetch_latest_subconverter_version() -> str:
    """
    从 GitHub API 获取 subconverter 的最新 release 版本号。
    返回格式如 "v0.9.0"。
    """
    # 优先使用 subconverter 的 Rust 重写版 (更轻量)
    url = "https://api.github.com/repos/sub-store-org/subconverter-rs/releases/latest"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        version = resp.json().get("tag_name", "")
        if version:
            print(f"[{now_str()}] 获取到 subconverter 最新版本: {version}", flush=True)
            return version
    except Exception as e:
        print(f"[{now_str()}] 获取 subconverter 最新版本失败: {e}，回退到 v0.9.0", flush=True)
    return "v0.9.0"


MIHOMO_VERSION = os.getenv("MIHOMO_VERSION") or _fetch_latest_mihomo_version()
SUBCONVERTER_VERSION = os.getenv("SUBCONVERTER_VERSION") or _fetch_latest_subconverter_version()

# ==================== mihomo 配置 ====================
MIHOMO_URL = (
    f"https://github.com/MetaCubeX/mihomo/releases/download/"
    f"{MIHOMO_VERSION}/mihomo-linux-amd64-{MIHOMO_VERSION}.gz"
)
MIHOMO_BIN = "mihomo"
MIXED_PORT = 7890          # HTTP 代理端口（用于速度测试）
API_PORT = 9090            # External Controller 端口

# ==================== 测速 URL ====================
LATENCY_TEST_URL = "https://www.gstatic.com/generate_204"
SPEED_TEST_URL = "https://speed.cloudflare.com/__down?bytes=10485760"  # 10MB

# ==================== 超时配置（毫秒/秒） ====================
LATENCY_TIMEOUT = 5000          # 延迟测试超时（毫秒）
SPEED_TIMEOUT = 15              # 速度测试超时（秒）
MIN_DOWNLOAD_BYTES = 5 * 1024 * 1024   # 最小下载字节数（5MB），低于此视为失败

# ==================== 过滤阈值 ====================
MAX_LATENCY = 3000              # 最大允许延迟（毫秒）
MIN_SPEED_MB = 0.5              # 最低允许速度（MB/s）

# ==================== TCP 端口预筛选 ====================
TCP_SCAN_ENABLED = True         # 是否启用 TCP 预筛选（强烈建议开启）
TCP_SCAN_TIMEOUT = 1.5          # TCP 连接超时（秒）
TCP_SCAN_WORKERS = 200          # TCP 扫描并发线程数

# ==================== 测速分批 ====================
TEST_BATCH_SIZE = 3000          # 每批送入 mihomo 的节点数

# ==================== 输出文件 ====================
CHUNK_SIZE = 10000
ALIVE_NODE_FILE = "alive.txt"
FILTERED_NODE_FILE = "fi_no.txt"
FINAL_OUTPUT_FILE = "jd.txt"
