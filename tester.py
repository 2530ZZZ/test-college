"""
测速模块 —— 基于 Clash.Meta (mihomo) 内核
顺序执行延迟测试和速度测试，输出存活节点。
"""

import os
import time
import json
import gzip
import shutil
import subprocess
import requests
import signal
import sys
from typing import List, Dict, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from proxy_model import StandardProxy
from config import (
    MIHOMO_URL, MIHOMO_BIN, MIXED_PORT, API_PORT,
    LATENCY_TEST_URL, LATENCY_TIMEOUT,
    SPEED_TEST_URL, SPEED_TIMEOUT, MIN_DOWNLOAD_BYTES,
    MAX_LATENCY, MIN_SPEED_MB,
    CHUNK_SIZE, ALIVE_NODE_FILE, FILTERED_NODE_FILE, FILTERED_LINKS_FILE
)

# 全局日志时间戳（复用 utils 中的北京时间）
from utils import now_str


class MihomoTester:
    def __init__(self, proxies: List[StandardProxy], work_dir: str = "."):
        """
        proxies: 标准化节点列表
        work_dir: mihomo 工作目录，默认当前
        """
        self.proxies = proxies
        self.work_dir = os.path.abspath(work_dir)
        self.bin_path = os.path.join(self.work_dir, MIHOMO_BIN)
        self.config_path = os.path.join(self.work_dir, "mihomo_config.yaml")
        self.process: Optional[subprocess.Popen] = None
        self.api_base = f"http://127.0.0.1:{API_PORT}"

    def download_mihomo(self):
        """下载并解压 mihomo 二进制（如果不存在）"""
        if os.path.exists(self.bin_path):
            print(f"[{now_str()}] mihomo 二进制已存在，跳过下载", flush=True)
            return

        print(f"[{now_str()}] 正在下载 mihomo ...", flush=True)
        gz_path = self.bin_path + ".gz"
        try:
            # 使用 requests 下载
            resp = requests.get(MIHOMO_URL, timeout=120, stream=True)
            resp.raise_for_status()
            with open(gz_path, "wb") as f:
                shutil.copyfileobj(resp.raw, f)
            # 解压
            with gzip.open(gz_path, "rb") as f_in:
                with open(self.bin_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.chmod(self.bin_path, 0o755)
            os.remove(gz_path)
            print(f"[{now_str()}] mihomo 下载解压完成", flush=True)
        except Exception as e:
            print(f"[{now_str()}] mihomo 下载失败: {e}", flush=True)
            raise

    def _convert_proxy_to_mihomo(self, proxy: StandardProxy) -> dict:
        """将 StandardProxy 转换为 mihomo 出站配置 dict"""
        # 简化字段映射，覆盖主要协议
        base = {
            "name": proxy.remark or f"{proxy.protocol}-{proxy.server}:{proxy.port}",
            "type": proxy.protocol,
            "server": proxy.server,
            "port": proxy.port,
            "uuid": proxy.uuid,
            "password": proxy.uuid,          # ss/trojan 共用
            "security": proxy.security or "auto",
            "sni": proxy.sni,
            "skip-cert-verify": proxy.allow_insecure,
        }
        # 移除空字段
        base = {k: v for k, v in base.items() if v is not None and v != ""}
        # 传输层等高级参数可在此扩展，目前保留 raw_link 即可让 mihomo 自行解析
        # 如果 raw_link 有效，mihomo 支持直接使用 "link" 字段
        if proxy.raw_link:
            base["link"] = proxy.raw_link
        return base

    def generate_config(self):
        """生成 mihomo 配置文件，包含所有节点和自动测速组"""
        proxies = []
        for p in self.proxies:
            try:
                proxies.append(self._convert_proxy_to_mihomo(p))
            except Exception as e:
                print(f"[{now_str()}] 节点转换失败 {p.remark or p.server}: {e}", flush=True)

        config = {
            "mixed-port": MIXED_PORT,
            "external-controller": f"127.0.0.1:{API_PORT}",
            "allow-lan": False,
            "mode": "rule",
            "log-level": "error",
            "proxies": proxies,
            "proxy-groups": [
                {
                    "name": "auto",
                    "type": "url-test",
                    "proxies": [p["name"] for p in proxies],
                    "url": LATENCY_TEST_URL,
                    "interval": 3600,    # 不自动切换，手动控制
                }
            ],
        }

        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        print(f"[{now_str()}] 已生成 mihomo 配置，共 {len(proxies)} 个节点", flush=True)

    def start_mihomo(self):
        """启动 mihomo 进程"""
        if self.process is not None:
            return
        cmd = [self.bin_path, "-f", self.config_path]
        try:
            self.process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2)  # 等待启动
            # 检查进程是否存活
            if self.process.poll() is not None:
                raise RuntimeError("mihomo 启动失败")
            print(f"[{now_str()}] mihomo 已启动，PID: {self.process.pid}", flush=True)
        except Exception as e:
            print(f"[{now_str()}] mihomo 启动异常: {e}", flush=True)
            raise

    def stop_mihomo(self):
        """停止 mihomo 进程"""
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None
            print(f"[{now_str()}] mihomo 已停止", flush=True)
            # 清理配置
            if os.path.exists(self.config_path):
                os.remove(self.config_path)

    def _api_get(self, path: str, timeout: int = 10) -> Optional[dict]:
        """调用 mihomo API"""
        try:
            resp = requests.get(f"{self.api_base}{path}", timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return None

    def measure_latency(self, proxy_name: str) -> int:
        """测试单个节点的 HTTP 延迟（毫秒），失败返回 -1"""
        path = f"/proxies/{requests.utils.quote(proxy_name)}/delay?url={requests.utils.quote(LATENCY_TEST_URL)}&timeout={LATENCY_TIMEOUT}"
        # mihomo 的 delay API 会返回 { delay: 123 }
        result = self._api_get(path, timeout=LATENCY_TIMEOUT // 1000 + 3)
        if result and "delay" in result:
            return result["delay"]
        return -1

    def measure_speed(self, proxy_name: str) -> float:
        """测试单个节点的下载速度（MB/s），失败返回 -1"""
        # 使用本地 HTTP 代理通过指定节点下载
        proxies = {
            "http": f"http://127.0.0.1:{MIXED_PORT}",
            "https": f"http://127.0.0.1:{MIXED_PORT}"
        }
        # 切换节点的临时规则：使用 API 将特定节点设为 "auto" 组的选中节点
        # 这样流量就会经过该节点
        switch_path = f"/proxies/auto"
        put_data = json.dumps({"name": proxy_name})
        try:
            requests.put(f"{self.api_base}{switch_path}", data=put_data, timeout=5)
        except Exception:
            return -1

        # 等待规则生效
        time.sleep(0.5)

        start = time.time()
        downloaded = 0
        try:
            with requests.get(SPEED_TEST_URL, proxies=proxies, timeout=SPEED_TIMEOUT / 1000, stream=True) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    downloaded += len(chunk)
                    if time.time() - start > SPEED_TIMEOUT / 1000:
                        break
        except Exception:
            return -1

        elapsed = time.time() - start
        if elapsed <= 0:
            return -1
        speed_mb = (downloaded / (1024 * 1024)) / elapsed
        if downloaded < MIN_DOWNLOAD_BYTES:
            return -1
        return round(speed_mb, 2)

    def test_all(self) -> Dict[str, Dict]:
        """
        顺序测试所有节点，返回结果字典：
        { proxy_name: { "latency": int, "speed": float, "alive": bool } }
        """
        # 获取所有节点名称
        proxies_list = self._api_get("/proxies")
        if not proxies_list:
            print(f"[{now_str()}] 无法获取代理列表，mihomo 可能未就绪", flush=True)
            return {}

        all_proxies = proxies_list.get("proxies", {})
        node_names = [name for name, info in all_proxies.items() if name not in ("GLOBAL", "DIRECT", "auto")]

        results = {}
        total = len(node_names)
        print(f"[{now_str()}] 开始延迟测试（共 {total} 个节点）...", flush=True)

        for idx, name in enumerate(node_names, 1):
            lat = self.measure_latency(name)
            alive = lat > 0
            if alive:
                print(f"  [{idx:4d}/{total}] {name:30s} 延迟: {lat}ms", flush=True)
            else:
                print(f"  [{idx:4d}/{total}] {name:30s} 延迟: 超时", flush=True)
            results[name] = {"latency": lat, "speed": -1.0, "alive": alive}

        print(f"[{now_str()}] 开始速度测试（仅存活节点）...", flush=True)
        alive_count = sum(1 for v in results.values() if v["alive"])
        idx = 0
        for name, info in results.items():
            if not info["alive"]:
                continue
            idx += 1
            spd = self.measure_speed(name)
            info["speed"] = spd
            if spd > 0:
                print(f"  [{idx:4d}/{alive_count}] {name:30s} 速度: {spd:.2f} MB/s", flush=True)
            else:
                print(f"  [{idx:4d}/{alive_count}] {name:30s} 速度: 测速失败", flush=True)

        return results

    def filter_and_save(self, results: Dict[str, Dict], original_proxies: List[StandardProxy]):
        """
        根据配置过滤节点，生成 alive.txt、fi_no.txt、fi_no_w_li.txt 和分片文件夹
        """
        # 构建 name -> proxy 映射
        name_map = {}
        for p in original_proxies:
            remark = p.remark or f"{p.protocol}-{p.server}:{p.port}"
            name_map[remark] = p

        alive_nodes = []
        filtered_nodes = []
        for name, info in results.items():
            if not info["alive"]:
                continue
            if info["latency"] > MAX_LATENCY:
                continue
            if info["speed"] != -1 and info["speed"] < MIN_SPEED_MB:
                continue
            # 存活
            alive_nodes.append(name)
            # 获取原始 raw_link
            proxy = name_map.get(name)
            if proxy:
                filtered_nodes.append(proxy.to_node_line())
            else:
                # 备用：记录名称
                filtered_nodes.append(name)

        # 保存 alive.txt（节点名称列表）
        with open(ALIVE_NODE_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(alive_nodes))
        print(f"[{now_str()}] 存活节点 {len(alive_nodes)} 个，已保存至 {ALIVE_NODE_FILE}", flush=True)

        # 保存 fi_no.txt（过滤后的节点 raw_link）
        with open(FILTERED_NODE_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(filtered_nodes))
        print(f"[{now_str()}] 过滤后节点 {len(filtered_nodes)} 条，已保存至 {FILTERED_NODE_FILE}", flush=True)

        # 生成 fi_no_w_li.txt 和分片文件夹
        filtered_dir = "fi_no_chunks"
        if os.path.exists(filtered_dir):
            shutil.rmtree(filtered_dir)
        os.makedirs(filtered_dir, exist_ok=True)

        chunk_size = CHUNK_SIZE
        file_count = 0
        fi_w_links = []
        repo_name = os.getenv("GITHUB_REPOSITORY", "2530ZZZ/cooo")
        branch_name = os.getenv("GITHUB_REF_NAME", "main")

        for i in range(0, len(filtered_nodes), chunk_size):
            chunk = filtered_nodes[i:i + chunk_size]
            file_count += 1
            filename = f"{file_count}.txt"
            filepath = os.path.join(filtered_dir, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write("\n".join(chunk))
            raw_url = f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/fi_no_chunks/{filename}"
            fi_w_links.append(raw_url)

        with open(FILTERED_LINKS_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(fi_w_links))
        print(f"[{now_str()}] 过滤节点分片共 {file_count} 个，索引保存至 {FILTERED_LINKS_FILE}", flush=True)


def run_full_test(proxies: List[StandardProxy], work_dir: str = "."):
    """
    对外暴露的主函数：下载 mihomo、生成配置、启动、测速、过滤、保存结果。
    """
    tester = MihomoTester(proxies, work_dir)
    try:
        tester.download_mihomo()
        tester.generate_config()
        tester.start_mihomo()
        # 等待 mihomo 完全就绪（可多次检查 API）
        for _ in range(10):
            if tester._api_get("/version"):
                break
            time.sleep(1)
        else:
            raise RuntimeError("mihomo API 未就绪")

        results = tester.test_all()
        if results:
            tester.filter_and_save(results, proxies)
        else:
            print(f"[{now_str()}] 测速未产生任何结果，跳过过滤", flush=True)
    finally:
        tester.stop_mihomo()
