"""
测速模块 —— TCP 预筛选 + mihomo 延迟测试 + 下载速度测试。
所有可调参数统一由 config.py 管理，包括：
  - MIHOMO_URL/MIHOMO_BIN/MIXED_PORT/API_PORT：mihomo 连接配置
  - LATENCY_TEST_URL/LATENCY_TIMEOUT/SPEED_TEST_URL/SPEED_TIMEOUT：测速目标与超时
  - MIN_DOWNLOAD_BYTES/MAX_LATENCY/MIN_SPEED_MB：过滤阈值
  - TCP_SCAN_ENABLED/TCP_SCAN_TIMEOUT/TCP_SCAN_WORKERS：TCP 预筛选参数
  - TEST_BATCH_SIZE：每批送入 mihomo 的节点数
  - ALIVE_NODE_FILE/MIHOMO_OUTPUT_FILE/MIHOMO_TEMPLATE_FILE：输出文件路径

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
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(TCP_SCAN_TIMEOUT)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def tcp_prescreen(node_list: List[str]) -> List[str]:
    print(f"[{now_str()}] 🔍 TCP 端口预筛选 ({len(node_list)} 个节点)...", flush=True)
    alive = []
    tasks = {}
    with ThreadPoolExecutor(max_workers=TCP_SCAN_WORKERS) as executor:
        for node in node_list:
            host, port = parse_host_port(node)
            if host and port:
                tasks[executor.submit(tcp_check, host, port)] = node
            else:
                alive.append(node)
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


def _test_one_batch(nodes_batch: List[str], batch_id: int) -> Dict[str, Dict]:
    results = {}
    bin_path = os.path.join(os.getcwd(), MIHOMO_BIN)

    # 过滤无效节点：确保每个节点都有内容（长度>10）
    valid_nodes = [raw.strip() for raw in nodes_batch if raw and len(raw.strip()) > 10]
    if not valid_nodes:
        print(f"[{now_str()}] mihomo 批次 {batch_id}: 无有效节点，跳过", flush=True)
        return results

    config = {
        "mixed-port": MIXED_PORT,
        "external-controller": f"127.0.0.1:{API_PORT}",
        "allow-lan": False,
        "mode": "rule",
        "log-level": "error",
        "proxies": [],
        "proxy-groups": [
            {"name": "auto", "type": "url-test", "proxies": [],
             "url": LATENCY_TEST_URL, "interval": 3600}
        ]
    }
    name_map = {}
    for i, raw in enumerate(valid_nodes):
        # 生成唯一名称，避免空格和特殊字符
        name = f"n{batch_id}_{i}"
        config["proxies"].append({"name": name, "link": raw})
        config["proxy-groups"][0]["proxies"].append(name)
        name_map[name] = raw

    tmp_conf = os.path.join(os.getcwd(), f"mihomo_batch_{batch_id}.yaml")
    try:
        with open(tmp_conf, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[{now_str()}] 写入配置文件失败: {e}", flush=True)
        return results

    print(f"[{now_str()}] 配置文件大小: {os.path.getsize(tmp_conf)} 字节", flush=True)

    # 启动 mihomo，捕获 stderr
    proc = subprocess.Popen(
        [bin_path, "-f", tmp_conf],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True
    )
    time.sleep(3)
    if proc.poll() is not None:
        stderr_output = proc.stderr.read() if proc.stderr else ""
        print(f"[{now_str()}] mihomo 批次 {batch_id} 启动失败", flush=True)
        if stderr_output:
            print(f"[{now_str()}] mihomo 错误输出: {stderr_output[:500]}", flush=True)
        else:
            print(f"[{now_str()}] mihomo 无错误输出，可能二进制文件损坏或权限不足", flush=True)
        os.remove(tmp_conf)
        return results

    api_base = f"http://127.0.0.1:{API_PORT}"
    try:
        resp = requests.get(f"{api_base}/proxies", timeout=5)
        all_proxies = resp.json().get("proxies", {})
        node_names = [n for n in all_proxies if n not in ("GLOBAL", "DIRECT", "auto")]
        print(f"[{now_str()}] mihomo 成功加载 {len(node_names)} 个节点", flush=True)
    except Exception as e:
        print(f"[{now_str()}] 获取代理列表失败: {e}", flush=True)
        proc.terminate()
        proc.wait()
        os.remove(tmp_conf)
        return results

    # 延迟测试
    for name in node_names:
        raw = name_map.get(name, "")
        try:
            r = requests.get(
                f"{api_base}/proxies/{name}/delay"
                f"?url={LATENCY_TEST_URL}&timeout={LATENCY_TIMEOUT}",
                timeout=8
            )
            if r.status_code == 200:
                lat = r.json().get("delay", -1)
                results[raw] = {"latency": lat, "speed": -1.0, "alive": lat > 0}
            else:
                results[raw] = {"latency": -1, "speed": -1.0, "alive": False}
        except Exception:
            results[raw] = {"latency": -1, "speed": -1.0, "alive": False}

    # 速度测试（仅存活节点）
    alive_nodes = [(name, raw) for name, raw in name_map.items()
                   if raw in results and results[raw]["alive"]]
    for name, raw in alive_nodes:
        try:
            requests.put(f"{api_base}/proxies/auto", json={"name": name}, timeout=5)
            time.sleep(0.3)
            proxies = {
                "http": f"http://127.0.0.1:{MIXED_PORT}",
                "https": f"http://127.0.0.1:{MIXED_PORT}"
            }
            start = time.time()
            downloaded = 0
            r = requests.get(SPEED_TEST_URL, proxies=proxies,
                             timeout=SPEED_TIMEOUT, stream=True)
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


def generate_mihomo_yaml(nodes: List[str], template_path: str, output_path: str):
    if not os.path.exists(template_path):
        default_config = {
            "mixed-port": 7890,
            "allow-lan": False,
            "mode": "rule",
            "log-level": "error",
            "proxies": [],
            "proxy-groups": [
                {"name": "auto", "type": "url-test", "proxies": [],
                 "url": LATENCY_TEST_URL, "interval": 3600}
            ]
        }
        with open(template_path, "w", encoding="utf-8") as f:
            json.dump(default_config, f, indent=2)

    try:
        with open(template_path, "r", encoding="utf-8") as f:
            template_text = f.read()
        try:
            import yaml
            config = yaml.safe_load(template_text)
        except ImportError:
            config = json.loads(template_text)

        if "proxies" not in config:
            config["proxies"] = []

        proxy_names = []
        new_proxies = []
        for idx, raw in enumerate(nodes):
            name = f"alive_{idx}"
            new_proxies.append({"name": name, "link": raw})
            proxy_names.append(name)
        config["proxies"] = new_proxies

        if "proxy-groups" in config and isinstance(config["proxy-groups"], list):
            for group in config["proxy-groups"]:
                if group.get("name") == "auto":
                    group["proxies"] = proxy_names

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


def run_full_test(node_strings: List[str], work_dir: str = "."):
    if not node_strings:
        print(f"[{now_str()}] 无节点可供测速", flush=True)
        return

    bin_path = os.path.join(os.getcwd(), MIHOMO_BIN)
    download_mihomo(bin_path)

    if TCP_SCAN_ENABLED:
        nodes = tcp_prescreen(node_strings)
    else:
        nodes = node_strings

    if not nodes:
        print(f"[{now_str()}] 无存活节点", flush=True)
        open(ALIVE_NODE_FILE, "w").close()
        generate_mihomo_yaml([], MIHOMO_TEMPLATE_FILE, MIHOMO_OUTPUT_FILE)
        return

    all_results = {}
    for i in range(0, len(nodes), TEST_BATCH_SIZE):
        batch = nodes[i:i + TEST_BATCH_SIZE]
        batch_id = i // TEST_BATCH_SIZE + 1
        print(f"[{now_str()}] 测速批次 {batch_id}: {len(batch)} 个节点", flush=True)
        results = _test_one_batch(batch, batch_id)
        all_results.update(results)

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

    with open(ALIVE_NODE_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(filtered_raws))
    print(f"[{now_str()}] 保存 alive.txt ({len(filtered_raws)} 个节点)", flush=True)

    generate_mihomo_yaml(filtered_raws, MIHOMO_TEMPLATE_FILE, MIHOMO_OUTPUT_FILE)
