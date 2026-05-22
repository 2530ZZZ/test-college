"""
测速模块 —— TCP 预筛选 + mihomo 延迟测试 + 下载速度测试。
输出 alive.txt（标准 URI）和 mihomo.yaml（订阅配置文件）。
"""

import os
import time
import json
import gzip
import shutil
import socket
import subprocess
import requests
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    MIHOMO_URL, MIHOMO_BIN, MIXED_PORT, API_PORT,
    LATENCY_TEST_URL, LATENCY_TIMEOUT, SPEED_TEST_URL, SPEED_TIMEOUT,
    MIN_DOWNLOAD_BYTES, MAX_LATENCY, MIN_SPEED_MB,
    TCP_SCAN_ENABLED, TCP_SCAN_TIMEOUT, TCP_SCAN_WORKERS,
    TEST_BATCH_SIZE, ALIVE_NODE_FILE, MIHOMO_OUTPUT_FILE, MIHOMO_TEMPLATE_FILE,
)
from utils import now_str


# ==================== 辅助函数 ====================

def parse_host_port(node_str: str) -> Tuple[Optional[str], Optional[int]]:
    """从节点 URI 中提取 host 和 port。"""
    if "://" not in node_str:
        return None, None
    try:
        rest = node_str.split("://", 1)[1]
        if "#" in rest:
            rest = rest.split("#")[0]
        if "?" in rest:
            rest = rest.split("?")[0]
        if "@" in rest:
            hostport = rest.split("@")[1]
        else:
            hostport = rest
        if ":" in hostport:
            host, port_str = hostport.rsplit(":", 1)
            return host, int(port_str)
    except Exception:
        pass
    return None, None


def tcp_check(host: str, port: int) -> bool:
    """TCP 端口连通性检查。"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(TCP_SCAN_TIMEOUT)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def tcp_prescreen(node_list: List[str]) -> List[str]:
    """多线程 TCP 端口预筛选，返回端口可达的节点列表。"""
    print(f"[{now_str()}] 🔍 TCP 端口预筛选 ({len(node_list)} 个节点)...", flush=True)
    alive = []
    tasks = {}
    with ThreadPoolExecutor(max_workers=TCP_SCAN_WORKERS) as executor:
        for node in node_list:
            host, port = parse_host_port(node)
            if host and port:
                tasks[executor.submit(tcp_check, host, port)] = node
            else:
                alive.append(node)  # 无法解析的保留，交给 mihomo 判断
        for future in as_completed(tasks):
            node = tasks[future]
            try:
                if future.result():
                    alive.append(node)
            except Exception:
                pass
    print(f"[{now_str()}] TCP 预筛选后存活: {len(alive)}/{len(node_list)}", flush=True)
    return alive


def download_mihomo(bin_path: str):
    """下载并解压 mihomo 二进制。"""
    if os.path.exists(bin_path):
        return
    print(f"[{now_str()}] 下载 mihomo ...", flush=True)
    gz_path = bin_path + ".gz"
    try:
        resp = requests.get(MIHOMO_URL, timeout=120, stream=True)
        resp.raise_for_status()
        with open(gz_path, "wb") as f:
            shutil.copyfileobj(resp.raw, f)
        with gzip.open(gz_path, "rb") as f_in:
            with open(bin_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.chmod(bin_path, 0o755)
        os.remove(gz_path)
        print(f"[{now_str()}] mihomo 就绪", flush=True)
    except Exception as e:
        raise RuntimeError(f"下载 mihomo 失败: {e}")


# ==================== 分批测试核心 ====================

def _test_one_batch(nodes_batch: List[str], batch_id: int) -> Dict[str, Dict]:
    """
    测试一批节点：生成临时配置 -> 启动 mihomo -> 延迟测试 -> 速度测试（仅存活） -> 停止 mihomo。
    返回 {node_raw: {latency, speed, alive}}。
    """
    results = {}
    bin_path = os.path.join(os.getcwd(), MIHOMO_BIN)

    # 生成最小配置
    config = {
        "mixed-port": MIXED_PORT,
        "external-controller": f"127.0.0.1:{API_PORT}",
        "allow-lan": False,
        "mode": "rule",
        "log-level": "error",
        "proxies": [],
        "proxy-groups": [
            {"name": "auto", "type": "url-test", "proxies": [], "url": LATENCY_TEST_URL, "interval": 3600}
        ]
    }
    name_map = {}
    for i, raw in enumerate(nodes_batch):
        name = f"n{batch_id}_{i}"
        config["proxies"].append({"name": name, "link": raw})
        config["proxy-groups"][0]["proxies"].append(name)
        name_map[name] = raw

    tmp_conf = os.path.join(os.getcwd(), f"mihomo_batch_{batch_id}.yaml")
    with open(tmp_conf, "w", encoding="utf-8") as f:
        json.dump(config, f)  # 临时用 JSON，mihomo 兼容

    # 启动 mihomo
    proc = subprocess.Popen([bin_path, "-f", tmp_conf], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)
    if proc.poll() is not None:
        print(f"[{now_str()}] mihomo 批次 {batch_id} 启动失败", flush=True)
        os.remove(tmp_conf)
        return results

    api_base = f"http://127.0.0.1:{API_PORT}"
    try:
        resp = requests.get(f"{api_base}/proxies", timeout=5)
        all_proxies = resp.json().get("proxies", {})
        node_names = [n for n in all_proxies if n not in ("GLOBAL", "DIRECT", "auto")]
    except Exception:
        proc.terminate()
        proc.wait()
        os.remove(tmp_conf)
        return results

    # 延迟测试
    for name in node_names:
        raw = name_map.get(name, "")
        try:
            r = requests.get(f"{api_base}/proxies/{name}/delay?url={LATENCY_TEST_URL}&timeout={LATENCY_TIMEOUT}", timeout=8)
            if r.status_code == 200:
                lat = r.json().get("delay", -1)
                results[raw] = {"latency": lat, "speed": -1.0, "alive": lat > 0}
            else:
                results[raw] = {"latency": -1, "speed": -1.0, "alive": False}
        except Exception:
            results[raw] = {"latency": -1, "speed": -1.0, "alive": False}

    # 速度测试（仅对延迟合格的节点）
    alive_nodes = [(name, raw) for name, raw in name_map.items()
                   if raw in results and results[raw]["alive"]]
    for name, raw in alive_nodes:
        try:
            requests.put(f"{api_base}/proxies/auto", json={"name": name}, timeout=5)
            time.sleep(0.3)
            proxies = {"http": f"http://127.0.0.1:{MIXED_PORT}", "https": f"http://127.0.0.1:{MIXED_PORT}"}
            start = time.time()
            downloaded = 0
            r = requests.get(SPEED_TEST_URL, proxies=proxies, timeout=SPEED_TIMEOUT, stream=True)
            r.raise_for_status()
            for chunk in r.iter_content(8192):
                downloaded += len(chunk)
                if time.time() - start > SPEED_TIMEOUT:
                    break
            elapsed = time.time() - start
            if elapsed > 0 and downloaded >= MIN_DOWNLOAD_BYTES:
                results[raw]["speed"] = round(downloaded / (1024 * 1024) / elapsed, 2)
        except Exception:
            pass

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    if os.path.exists(tmp_conf):
        os.remove(tmp_conf)
    return results


# ==================== mihomo.yaml 生成 ====================

def generate_mihomo_yaml(nodes: List[str], template_path: str, output_path: str):
    """
    使用用户提供的模板 new.yaml 生成 mihomo 订阅文件。
    模板中 proxies 列表会被替换为存活节点，proxy-groups 中的 auto 组也会同步更新。
    如果模板不存在，则使用默认最小配置。
    """
    if not os.path.exists(template_path):
        # 模板不存在时生成一个极简模板
        default_config = {
            "mixed-port": 7890,
            "allow-lan": False,
            "mode": "rule",
            "log-level": "error",
            "proxies": [],
            "proxy-groups": [
                {"name": "auto", "type": "url-test", "proxies": [], "url": LATENCY_TEST_URL, "interval": 3600}
            ]
        }
        with open(template_path, "w", encoding="utf-8") as f:
            json.dump(default_config, f, indent=2)

    try:
        with open(template_path, "r", encoding="utf-8") as f:
            template_text = f.read()

        # 尝试解析为 YAML，如果 PyYAML 不可用则回退 JSON
        try:
            import yaml
            config = yaml.safe_load(template_text)
        except ImportError:
            config = json.loads(template_text)

        # 确保 proxies 字段存在
        if "proxies" not in config:
            config["proxies"] = []

        # 用存活节点填充 proxies 列表
        proxy_names = []
        new_proxies = []
        for idx, raw in enumerate(nodes):
            name = f"alive_{idx}"
            new_proxies.append({"name": name, "link": raw})
            proxy_names.append(name)
        config["proxies"] = new_proxies

        # 更新 proxy-groups 中的 auto 组（如果存在）
        if "proxy-groups" in config and isinstance(config["proxy-groups"], list):
            for group in config["proxy-groups"]:
                if group.get("name") == "auto":
                    group["proxies"] = proxy_names

        # 输出为 YAML 格式（优先 PyYAML，否则 JSON）
        try:
            import yaml
            with open(output_path, "w", encoding="utf-8") as f:
                yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
        except ImportError:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=2, ensure_ascii=False)

        print(f"[{now_str()}] 已生成 mihomo.yaml ({len(nodes)} 个节点)", flush=True)

    except Exception as e:
        print(f"[{now_str()}] 生成 mihomo.yaml 失败: {e}", flush=True)


# ==================== 主测试流程 ====================

def run_full_test(node_strings: List[str], work_dir: str = "."):
    """
    测速主入口：
    1. TCP 端口预筛选（可选）
    2. 分批测延迟+速度
    3. 过滤存活节点，输出 alive.txt 和 mihomo.yaml
    """
    if not node_strings:
        print(f"[{now_str()}] 无节点可供测速", flush=True)
        return

    bin_path = os.path.join(os.getcwd(), MIHOMO_BIN)
    download_mihomo(bin_path)

    # TCP 预筛选
    if TCP_SCAN_ENABLED:
        nodes = tcp_prescreen(node_strings)
    else:
        nodes = node_strings

    if not nodes:
        print(f"[{now_str()}] 无存活节点", flush=True)
        # 生成空文件
        open(ALIVE_NODE_FILE, "w").close()
        generate_mihomo_yaml([], MIHOMO_TEMPLATE_FILE, MIHOMO_OUTPUT_FILE)
        return

    # 分批测试
    all_results = {}
    for i in range(0, len(nodes), TEST_BATCH_SIZE):
        batch = nodes[i:i + TEST_BATCH_SIZE]
        batch_id = i // TEST_BATCH_SIZE + 1
        print(f"[{now_str()}] 测速批次 {batch_id}: {len(batch)} 个节点", flush=True)
        results = _test_one_batch(batch, batch_id)
        all_results.update(results)

    # 过滤出同时满足延迟和速度要求的节点
    filtered_raws = []
    for raw, info in all_results.items():
        if not info["alive"]:
            continue
        if info["latency"] > MAX_LATENCY:
            continue
        if info["speed"] != -1 and info["speed"] < MIN_SPEED_MB:
            continue
        filtered_raws.append(raw)

    if not filtered_raws:
        print(f"[{now_str()}] 无节点通过过滤", flush=True)
        open(ALIVE_NODE_FILE, "w").close()
        generate_mihomo_yaml([], MIHOMO_TEMPLATE_FILE, MIHOMO_OUTPUT_FILE)
        return

    # 写入 alive.txt
    with open(ALIVE_NODE_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(filtered_raws))
    print(f"[{now_str()}] 保存 alive.txt ({len(filtered_raws)} 个节点)", flush=True)

    # 生成 mihomo.yaml
    generate_mihomo_yaml(filtered_raws, MIHOMO_TEMPLATE_FILE, MIHOMO_OUTPUT_FILE)
