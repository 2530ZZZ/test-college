"""
测速模块 —— 基于 Clash.Meta (mihomo)
集成延迟测试、速度测试、国家识别（离线/在线）、排序输出。
输出文件：alive.txt (存活节点名), fi_no.txt (原始链接), jd.txt (国旗+编号+链接)
"""

import os
import time
import json
import gzip
import shutil
import subprocess
import requests
from typing import List, Dict, Tuple, Optional
from config import (
    MIHOMO_URL, MIHOMO_BIN, MIXED_PORT, API_PORT,
    LATENCY_TEST_URL, LATENCY_TIMEOUT,
    SPEED_TEST_URL, SPEED_TIMEOUT, MIN_DOWNLOAD_BYTES,
    MAX_LATENCY, MIN_SPEED_MB,
    CHUNK_SIZE, ALIVE_NODE_FILE, FILTERED_NODE_FILE, FINAL_OUTPUT_FILE,
    GEOLITE_DB_URL, GEOLITE_DB_PATH, USE_ONLINE_IP_API
)
from utils import now_str

# 尝试导入 geoip2，若失败则使用域名推测
try:
    import geoip2.database
    GEOIP2_AVAILABLE = True
except ImportError:
    GEOIP2_AVAILABLE = False


# ==================== 国旗与国家代码映射 ====================
COUNTRY_FLAG = {
    "US": "🇺🇸", "GB": "🇬🇧", "DE": "🇩🇪", "JP": "🇯🇵", "KR": "🇰🇷",
    "SG": "🇸🇬", "HK": "🇭🇰", "TW": "🇹🇼", "CN": "🇨🇳", "FR": "🇫🇷",
    "CA": "🇨🇦", "AU": "🇦🇺", "IN": "🇮🇳", "NL": "🇳🇱", "RU": "🇷🇺",
    "BR": "🇧🇷", "IT": "🇮🇹", "ES": "🇪🇸", "CH": "🇨🇭", "SE": "🇸🇪",
    "NO": "🇳🇴", "DK": "🇩🇰", "FI": "🇫🇮", "PL": "🇵🇱", "TR": "🇹🇷",
    "UA": "🇺🇦", "VN": "🇻🇳", "TH": "🇹🇭", "ID": "🇮🇩", "MY": "🇲🇾",
    "PH": "🇵🇭", "AE": "🇦🇪", "SA": "🇸🇦", "EG": "🇪🇬", "ZA": "🇿🇦",
    "AR": "🇦🇷", "CL": "🇨🇱", "CO": "🇨🇴", "MX": "🇲🇽",
    # 可根据需要继续扩展
}

def get_flag(country_code: str) -> str:
    """根据两位国家代码返回国旗emoji，未知返回🏳️"""
    return COUNTRY_FLAG.get(country_code.upper(), "🏳️")


# ==================== 国家识别类 ====================
class GeoIdentifier:
    def __init__(self):
        self.reader = None
        if GEOIP2_AVAILABLE:
            self.load_geolite_db()

    def load_geolite_db(self):
        """加载离线数据库（若存在），否则尝试下载"""
        if os.path.exists(GEOLITE_DB_PATH):
            try:
                self.reader = geoip2.database.Reader(GEOLITE_DB_PATH)
                print(f"[{now_str()}] 已加载 GeoLite2 离线数据库", flush=True)
                return
            except Exception as e:
                print(f"[{now_str()}] 加载离线数据库失败: {e}", flush=True)

        # 尝试下载
        print(f"[{now_str()}] 未找到 GeoLite2 数据库，尝试下载...", flush=True)
        try:
            resp = requests.get(GEOLITE_DB_URL, timeout=30)
            if resp.status_code == 200:
                with open(GEOLITE_DB_PATH, "wb") as f:
                    f.write(resp.content)
                self.reader = geoip2.database.Reader(GEOLITE_DB_PATH)
                print(f"[{now_str()}] 下载成功", flush=True)
            else:
                print(f"[{now_str()}] 下载失败（状态码：{resp.status_code}），将使用域名推测", flush=True)
        except Exception as e:
            print(f"[{now_str()}] 下载异常: {e}，将使用域名推测", flush=True)

    def lookup(self, ip_or_host: str) -> str:
        """
        查询 IP 或主机名对应的两位国家代码。
        优先使用离线数据库，其次域名推测，最后在线 API（若启用）。
        """
        # 尝试使用离线数据库
        if self.reader:
            try:
                response = self.reader.city(ip_or_host)
                return response.country.iso_code or "未知"
            except Exception:
                pass

        # 域名推测：根据顶级域或常见后缀
        host = ip_or_host.split("/")[0] if "/" in ip_or_host else ip_or_host
        if "." in host:
            parts = host.split(".")
            tld = parts[-1].upper()
            tld_map = {
                "US": "US", "UK": "GB", "DE": "DE", "JP": "JP", "KR": "KR",
                "SG": "SG", "HK": "HK", "TW": "TW", "CN": "CN", "FR": "FR",
                "CA": "CA", "AU": "AU", "IN": "IN", "NL": "NL", "RU": "RU",
                "BR": "BR", "IT": "IT", "ES": "ES", "CH": "CH", "SE": "SE",
                "COM": None, "NET": None, "ORG": None, "INFO": None
            }
            if tld in tld_map and tld_map[tld]:
                return tld_map[tld]
            # 如果二级域名为国别 (例如 example.co.jp)
            if len(parts) >= 3 and parts[-2].upper() in COUNTRY_FLAG:
                return parts[-2].upper()

        # 在线 API（若启用）
        if USE_ONLINE_IP_API:
            try:
                resp = requests.get(f"http://ip-api.com/json/{ip_or_host}?fields=countryCode", timeout=3)
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get("countryCode", "未知")
            except Exception:
                pass

        return "未知"


# ==================== 测速核心类 ====================
class MihomoTester:
    def __init__(self, proxies: List[str], work_dir: str = "."):
        """
        proxies: 节点字符串列表（URI 或 JSON，mihomo 可识别的格式）
        """
        self.proxies = proxies
        self.work_dir = os.path.abspath(work_dir)
        self.bin_path = os.path.join(self.work_dir, MIHOMO_BIN)
        self.config_path = os.path.join(self.work_dir, "mihomo_config.yaml")
        self.process: Optional[subprocess.Popen] = None
        self.api_base = f"http://127.0.0.1:{API_PORT}"
        self.geo = GeoIdentifier()

    def download_mihomo(self):
        """下载并解压 mihomo 二进制"""
        if os.path.exists(self.bin_path):
            return
        print(f"[{now_str()}] 下载 mihomo ...", flush=True)
        gz_path = self.bin_path + ".gz"
        try:
            resp = requests.get(MIHOMO_URL, timeout=120, stream=True)
            resp.raise_for_status()
            with open(gz_path, "wb") as f:
                shutil.copyfileobj(resp.raw, f)
            with gzip.open(gz_path, "rb") as f_in:
                with open(self.bin_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.chmod(self.bin_path, 0o755)
            os.remove(gz_path)
            print(f"[{now_str()}] mihomo 就绪", flush=True)
        except Exception as e:
            raise RuntimeError(f"下载 mihomo 失败: {e}")

    def generate_config(self):
        """生成 mihomo 配置文件，包含所有节点"""
        config = {
            "mixed-port": MIXED_PORT,
            "external-controller": f"127.0.0.1:{API_PORT}",
            "allow-lan": False,
            "mode": "rule",
            "log-level": "error",
            "proxies": [],
            "proxy-groups": [
                {
                    "name": "auto",
                    "type": "url-test",
                    "proxies": [],
                    "url": LATENCY_TEST_URL,
                    "interval": 3600,
                }
            ],
        }
        for i, raw in enumerate(self.proxies):
            name = f"node_{i}"
            proxy_item = {"name": name, "link": raw}
            config["proxies"].append(proxy_item)
            config["proxy-groups"][0]["proxies"].append(name)

        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        print(f"[{now_str()}] 配置已生成，共 {len(self.proxies)} 个节点", flush=True)

    def start_mihomo(self):
        """启动 mihomo 进程"""
        if self.process:
            return
        cmd = [self.bin_path, "-f", self.config_path]
        self.process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(2)
        if self.process.poll() is not None:
            raise RuntimeError("mihomo 启动失败")
        print(f"[{now_str()}] mihomo 已启动 (PID: {self.process.pid})", flush=True)

    def stop_mihomo(self):
        """停止 mihomo"""
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None
            print(f"[{now_str()}] mihomo 已停止", flush=True)

    def _api_get(self, path, timeout=10) -> Optional[dict]:
        try:
            resp = requests.get(f"{self.api_base}{path}", timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return None

    def measure_latency(self, proxy_name: str) -> int:
        """返回延迟（毫秒），失败返回 -1"""
        path = f"/proxies/{requests.utils.quote(proxy_name)}/delay?url={requests.utils.quote(LATENCY_TEST_URL)}&timeout={LATENCY_TIMEOUT}"
        result = self._api_get(path, timeout=LATENCY_TIMEOUT // 1000 + 3)
        if result and "delay" in result:
            return result["delay"]
        return -1

    def measure_speed(self, proxy_name: str) -> float:
        """返回速度（MB/s），失败返回 -1"""
        proxies = {
            "http": f"http://127.0.0.1:{MIXED_PORT}",
            "https": f"http://127.0.0.1:{MIXED_PORT}"
        }
        switch_path = f"/proxies/auto"
        put_data = json.dumps({"name": proxy_name})
        try:
            requests.put(f"{self.api_base}{switch_path}", data=put_data, timeout=5)
        except Exception:
            return -1

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
        if elapsed <= 0 or downloaded < MIN_DOWNLOAD_BYTES:
            return -1
        return round(downloaded / (1024 * 1024) / elapsed, 2)

    def test_all(self) -> Dict[str, Dict]:
        """测试所有节点，返回 {name: {latency, speed, alive}}"""
        proxies_info = self._api_get("/proxies")
        if not proxies_info:
            return {}

        all_proxies = proxies_info.get("proxies", {})
        node_names = [n for n in all_proxies if n not in ("GLOBAL", "DIRECT", "auto")]

        results = {}
        total = len(node_names)
        print(f"[{now_str()}] 延迟测试（共 {total} 个）...", flush=True)
        for idx, name in enumerate(node_names, 1):
            lat = self.measure_latency(name)
            alive = lat > 0
            results[name] = {"latency": lat, "speed": -1.0, "alive": alive}
            status = f"{lat}ms" if alive else "超时"
            print(f"  [{idx:4d}/{total}] {name:20s} 延迟: {status}", flush=True)

        alive_nodes = [n for n, v in results.items() if v["alive"]]
        print(f"[{now_str()}] 速度测试（{len(alive_nodes)} 个存活）...", flush=True)
        for idx, name in enumerate(alive_nodes, 1):
            spd = self.measure_speed(name)
            results[name]["speed"] = spd
            status = f"{spd:.2f} MB/s" if spd > 0 else "测速失败"
            print(f"  [{idx:4d}/{len(alive_nodes)}] {name:20s} 速度: {status}", flush=True)

        return results

    def filter_and_save(self, results: Dict[str, Dict], original_list: List[str]):
        """
        过滤、国家识别、排序，保存最终文件
        """
        # 创建 name -> original_link 映射
        name_map = {}
        for i, raw in enumerate(original_list):
            name_map[f"node_{i}"] = raw

        # 初步过滤
        filtered = []
        for name, info in results.items():
            if not info["alive"]:
                continue
            if info["latency"] > MAX_LATENCY:
                continue
            if info["speed"] != -1 and info["speed"] < MIN_SPEED_MB:
                continue
            filtered.append((name, info["latency"], info["speed"]))

        if not filtered:
            print(f"[{now_str()}] 无存活节点通过过滤", flush=True)
            open(ALIVE_NODE_FILE, "w").close()
            open(FILTERED_NODE_FILE, "w").close()
            open(FINAL_OUTPUT_FILE, "w").close()
            return

        # 国家识别 + 结构化
        proxy_entries = []
        for name, lat, spd in filtered:
            raw = name_map.get(name, "")
            # 提取服务器地址（简单提取 host）
            server = "未知"
            if "://" in raw:
                try:
                    uri = raw.split("://", 1)[1]
                    if "#" in uri:
                        uri = uri.split("#")[0]
                    if "?" in uri:
                        uri = uri.split("?")[0]
                    if "@" in uri:
                        host = uri.split("@")[1]
                    else:
                        host = uri
                    if ":" in host and not host.startswith("["):
                        server = host.rsplit(":", 1)[0]
                    else:
                        server = host
                except Exception:
                    pass
            country_code = self.geo.lookup(server)
            proxy_entries.append({
                "name": name,
                "lat": lat,
                "spd": spd,
                "raw": raw,
                "country": country_code,
                "server": server
            })

        # 按国家排序，同一国家内按延迟升序
        proxy_entries.sort(key=lambda x: (x["country"], x["lat"]))

        # 国家内编号
        country_counts = {}
        final_lines = []       # jd.txt
        alive_names = []       # alive.txt
        filtered_links = []    # fi_no.txt
        for entry in proxy_entries:
            cc = entry["country"]
            country_counts[cc] = country_counts.get(cc, 0) + 1
            idx = country_counts[cc]
            flag = get_flag(cc)
            alias = f"{flag} {cc}_{idx}"
            alive_names.append(alias)
            filtered_links.append(entry["raw"])
            final_lines.append(f"{alias} | {entry['raw']}")

        with open(ALIVE_NODE_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(alive_names))
        with open(FILTERED_NODE_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(filtered_links))
        with open(FINAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(final_lines))

        print(f"[{now_str()}] 最终存活节点: {len(alive_names)} 个", flush=True)
        print(f"[{now_str()}] 输出文件: {ALIVE_NODE_FILE}, {FILTERED_NODE_FILE}, {FINAL_OUTPUT_FILE}", flush=True)

        # 可选分片输出
        self.save_chunks(filtered_links)

    def save_chunks(self, links: List[str]):
        """分片保存过滤后的原始链接，生成 fi_no_w_li.txt"""
        chunk_dir = "fi_no_chunks"
        if os.path.exists(chunk_dir):
            shutil.rmtree(chunk_dir)
        os.makedirs(chunk_dir, exist_ok=True)

        chunk_size = CHUNK_SIZE
        file_count = 0
        w_links = []
        repo_name = os.getenv("GITHUB_REPOSITORY", "2530ZZZ/cooo")
        branch_name = os.getenv("GITHUB_REF_NAME", "main")

        for i in range(0, len(links), chunk_size):
            chunk = links[i:i + chunk_size]
            file_count += 1
            filename = f"{file_count}.txt"
            filepath = os.path.join(chunk_dir, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write("\n".join(chunk))
            raw_url = f"https://raw.githubusercontent.com/{repo_name}/{branch_name}/fi_no_chunks/{filename}"
            w_links.append(raw_url)

        with open("fi_no_w_li.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(w_links))
        print(f"[{now_str()}] 分片索引保存至 fi_no_w_li.txt", flush=True)


def run_full_test(node_strings: List[str], work_dir: str = "."):
    """主流程：下载、配置、测速、过滤、输出"""
    tester = MihomoTester(node_strings, work_dir)
    try:
        tester.download_mihomo()
        tester.generate_config()
        tester.start_mihomo()

        for _ in range(10):
            if tester._api_get("/version"):
                break
            time.sleep(1)
        else:
            raise RuntimeError("mihomo API 未就绪")

        results = tester.test_all()
        if results:
            tester.filter_and_save(results, node_strings)
        else:
            print(f"[{now_str()}] 无测试结果", flush=True)
    finally:
        tester.stop_mihomo()
