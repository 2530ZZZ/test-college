"""
结构化格式提取模块。

从 Clash YAML、Surge 配置、Sing-box JSON 等结构化格式中提取代理节点信息。
与 uri_parser.py 分工：
  - uri_parser.py: 负责 URI 格式（vmess://, trojan://, ss:// 等）的发现和解析
  - parsers.py:    负责结构化配置格式（YAML/JSON 中的 proxies 数组）的提取

设计原则：
  1. 每个提取策略一个独立函数，可单独测试
  2. 直接产出 StandardProxy 列表（已解析、已验证）
  3. 不处理 URI（交给 uri_parser.py）

设计背景（多级兜底哲学）：
  - 全网配置格式没有标准可依：同一份 Clash 配置可能是标准 YAML、
    压缩单行 flow、JSON 数组、甚至带语法错误的残缺块。每个格式的
    提取函数内部按"最可靠 → 最宽松"的次序逐级尝试，前一级失败
    才进下一级（yaml.safe_load 失败 → 正则兜底 → 逐行解析）。
  - 代价权衡：误杀一个节点 = 永久丢失一个可用节点；误收一个噪音
    会在后续去重/测速被滤掉。所以宁多勿漏（"解析节点是第一原则"）。
  - 参考实现：Clash / Sing-box / Surge 各生态的字段语义参考了
    sing-box / v2rayN / subconverter / mihomo 开源解析器分析记录。
"""

import re
import json
import os
from typing import List, Set, Optional
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

from models import StandardProxy, dict_to_standard_proxy
from uri_parser import discover_candidates, validate_candidate
from utils import safe_base64_decode


# ==================== 预编译正则 ====================

# URI 格式的 scheme 扫描（用于从结构化文本中查找嵌入的 URI）
URI_SCHEMES = r'(?:vmess|vless|trojan|ss|ssr|hysteria|hysteria2|hy2|tuic|reality|wireguard|sing-box)'
URI_RE = re.compile(rf'(?i){URI_SCHEMES}://\S{{1,4000}}')

# YAML 中 Clash 代理的定义模式
# 例如: - {name: xx, type: vmess, server: xx, port: 443, ...}
CLASH_SINGLE_RE = re.compile(
    r'-\s*\{[^}]{1,15000}?'
    r'(?:name|server|port|type|uuid|password|ps|flow|sni|fp|reality-opts)'
    r'[^}]{0,15000}\}',
    re.DOTALL
)

# YAML 多行代理定义: - name: xxx\n  type: vmess\n  server: xxx\n  ...
CLASH_MULTI_RE = re.compile(
    r'-\s*name:[^\n]*\n'
    r'(?:\s+[a-zA-Z_-]+:[^\n]*\n){2,100}',
    re.DOTALL
)

# Surge / Quantumult 代理格式: ProxyName = protocol, server, port, ...
SURGE_PROXY_RE = re.compile(
    r'(?i)(?:^|\n)\s*'
    r'([^\s=,]+)\s*=\s*'
    r'(vmess|vless|trojan|ss|ssr|hysteria2?|hy2|tuic|wireguard|shadowsocks|http|https|socks5)'
    r'\s*,\s*[^\n]{10,4000}',
    re.MULTILINE
)

# JSON 中的 proxies/outbounds 数组
JSON_PROXY_ARRAY_RE = re.compile(
    r'"(?:proxies|outbounds)"\s*:\s*\[([\s\S]{1,2000000}?)\]',
    re.IGNORECASE
)

# 代码块（Markdown 等）
CODE_BLOCK_RE = re.compile(
    r'(?:```(?:[\w]*)\n?)([\s\S]{1,1000000}?)(?:\n?```)'
    r'|`([^`\n]{1,10000})`'
)

# JSON 对象退避扫描（用于未匹配到常规格式的场景）
JSON_FALLBACK_RE = re.compile(
    r'\{[^{}]*?"server"\s*:\s*"[^"]*"[^{}]*?(?:port|server_port)"\s*:\s*\d+[^{}]*?\}',
    re.IGNORECASE | re.DOTALL
)

# Base64 长块（用于递归解码）
BASE64_RE = re.compile(r'[A-Za-z0-9+/=]{100,200000}')


# ==================== 策略函数 ====================

def extract_embedded_uris(text: str) -> List[StandardProxy]:
    """从文本中提取嵌入式 URI 节点。

    使用 uri_parser.discover_candidates 进行递归发现和协议验证。

    Args:
        text: 任意文本内容

    Returns:
        已验证的 StandardProxy 列表
    """
    candidates = discover_candidates(text)
    proxies = []
    seen = set()
    for cand in candidates:
        proxy = validate_candidate(cand)
        if proxy and proxy.is_valid():
            key = proxy.dedup_key("server_port_protocol")
            if key not in seen:
                seen.add(key)
                proxies.append(proxy)
    return proxies


def extract_clash_yaml(text: str) -> List[StandardProxy]:
    """从 Clash YAML 格式文本中提取代理节点。

    处理三种 Clash 代理定义格式：
      - 标准多行 YAML:  - name: xx\n  type: vmess\n  ...
      - 单行 JSON:      - {name: xx, type: vmess, ...}
      - 压缩 YAML:      {port: ..., proxies: [{name: xx, ...}, ...]}

    Args:
        text: YAML 格式文本

    Returns:
        StandardProxy 列表
    """
    proxies = []
    seen = set()

    # ── 策略 1: 整块 YAML 解析 ──
    try:
        import yaml
        doc = yaml.safe_load(text)
        if isinstance(doc, dict):
            _walk_dict_for_proxies(doc, proxies, seen)
        elif isinstance(doc, list):
            # 直接就是代理列表（如 sub/list/00.txt）
            for item in doc:
                if isinstance(item, dict):
                    proxy = dict_to_standard_proxy(item)
                    if proxy and proxy.is_valid():
                        key = proxy.dedup_key("server_port_protocol")
                        if key not in seen:
                            seen.add(key)
                            proxies.append(proxy)
        # 整块 YAML 解析成功（产出节点）就短路返回：yaml.safe_load 是
        # 最可靠的路径，正则策略只是它的兜底，不重复执行
        if proxies:
            return proxies
    except ImportError:
        pass
    except Exception:
        pass  # YAML 解析失败，回退到正则

    # ── 逻辑 2: 压缩 YAML flow 格式（minified，无换行） ──
    # 格式: {..., proxies: [{name: ..., server: ..., port: ..., type: ..., ...}, ...], ...}
    # 找到 proxies: [ 到 ] 之间的内容，逐个提取 proxy 对象
    try:
        # 找到 proxies 数组的起始和结束位置
        pm = re.search(r'proxies\s*:\s*\[', text, re.IGNORECASE)
        if pm:
            start = pm.end()
            depth = 1
            i = start
            while i < len(text) and depth > 0:
                if text[i] == '[': depth += 1
                elif text[i] == ']': depth -= 1
                i += 1
            if depth == 0:
                arr_content = text[start:i-1]
                # 逐个提取 proxy 对象 {name: ..., type: ..., ...}
                for obj_match in re.finditer(r'\{[^{}]*?\}', arr_content):
                    obj_str = obj_match.group(0)
                    # 检查是否是 proxy 对象（包含 type 或 server 关键字段）
                    if re.search(r'(?:^|,)\s*(?:type|server|port|name)\s*:', obj_str, re.IGNORECASE):
                        # 尝试转为标准格式（加引号做 JSON 解析）
                        try:
                            # 先把 YAML flow 值转为 JSON：给 key 加引号
                            json_str = re.sub(
                                r'(?<=[\s,:]|^)(\b[a-zA-Z_][a-zA-Z0-9_-]*)\s*:',
                                r'"\1":', obj_str
                            )
                            # 给未加引号的字符串值加引号（但跳过 true/false/null/数字）
                            json_str = re.sub(
                                r':\s*([a-zA-Z_][a-zA-Z0-9_\-/.@+%]*?)(?=[,}])',
                                r': "\1"', json_str
                            )
                            obj = json.loads(json_str)
                            proxy = dict_to_standard_proxy(obj)
                            if proxy and proxy.is_valid():
                                key = proxy.dedup_key("server_port_protocol")
                                if key not in seen:
                                    seen.add(key)
                                    proxies.append(proxy)
                        except Exception:
                            # JSON 解析失败，用 dict_to_standard_proxy 直接处理
                            # 先转为 Python dict
                            try:
                                pairs = re.findall(
                                    r'(\w[\w-]*)\s*:\s*"([^"]*)"|(\w[\w-]*)\s*:\s*(\S+)',
                                    obj_str
                                )
                                d = {}
                                for p in pairs:
                                    key = p[0] or p[2]
                                    val = p[1] or p[3]
                                    d[key.strip()] = val.strip().strip('"').strip("'")
                                proxy = dict_to_standard_proxy(d)
                                if proxy and proxy.is_valid():
                                    key = proxy.dedup_key("server_port_protocol")
                                    if key not in seen:
                                        seen.add(key)
                                        proxies.append(proxy)
                            except Exception:
                                pass
    except Exception:
        pass

    # ── 策略 3: 单行 JSON 对象正则 ──
    for m in CLASH_SINGLE_RE.finditer(text):
        clean = re.sub(r'\s+', ' ', m.group(0).strip())
        if len(clean) > 30:
            try:
                obj = json.loads(clean)
                proxy = dict_to_standard_proxy(obj)
                if proxy and proxy.is_valid():
                    key = proxy.dedup_key("server_port_protocol")
                    if key not in seen:
                        seen.add(key)
                        proxies.append(proxy)
            except Exception:
                pass

    # ── 策略 3b: 逐行 YAML flow 格式（- {key: value, ...}，兜底） ──
    # YAML 库对某些包含特殊字符的文件会静默失败，用逐行正则作为保底
    if not proxies:
        for line in text.split('\n'):
            line = line.strip()
            if not line.startswith('- {'):
                continue
            # 去掉前导 "- " 和首尾空格
            obj_str = line[2:].strip()
            if obj_str.startswith('{') and obj_str.endswith('}'):
                proxy = _parse_flow_object(obj_str)
                if proxy and proxy.is_valid():
                    key = proxy.dedup_key("server_port_protocol")
                    if key not in seen:
                        seen.add(key)
                        proxies.append(proxy)

    # ── 策略 4: 多行 YAML 代理块 ──
    for m in CLASH_MULTI_RE.finditer(text):
        block = m.group(0)
        try:
            import yaml
            wrapped = "proxies:\n" + block
            doc = yaml.safe_load(wrapped)
            if doc and "proxies" in doc:
                for item in doc["proxies"]:
                    if isinstance(item, dict):
                        proxy = dict_to_standard_proxy(item)
                        if proxy and proxy.is_valid():
                            key = proxy.dedup_key("server_port_protocol")
                            if key not in seen:
                                seen.add(key)
                                proxies.append(proxy)
        except Exception:
            pass

    return proxies


def extract_singbox_json(text: str) -> List[StandardProxy]:
    """从 Sing-box JSON 配置中提取 outbounds 代理节点。

    尝试整块 JSON 解析，失败后回退到正则提取 proxies/outbounds 数组。

    Args:
        text: JSON 格式文本

    Returns:
        StandardProxy 列表
    """
    proxies = []
    seen = set()

    # 尝试整块 JSON 解析
    try:
        doc = json.loads(text)
        if isinstance(doc, dict):
            _walk_dict_for_proxies(doc, proxies, seen)
            return proxies
    except Exception:
        pass

    # 回退：正则提取
    for arr_match in JSON_PROXY_ARRAY_RE.finditer(text):
        arr = arr_match.group(1)
        # 在每个 JSON 对象上尝试解析
        for obj_match in re.finditer(r'\{[\s\S]{1,5000}?\}', arr):
            obj_str = obj_match.group(0)
            try:
                obj = json.loads(obj_str)
                proxy = dict_to_standard_proxy(obj)
                if proxy and proxy.is_valid():
                    key = proxy.dedup_key("server_port_protocol")
                    if key not in seen:
                        seen.add(key)
                        proxies.append(proxy)
            except Exception:
                pass

    # 兜底：退避正则
    for m in JSON_FALLBACK_RE.finditer(text):
        try:
            obj = json.loads(m.group(0))
            proxy = dict_to_standard_proxy(obj)
            if proxy and proxy.is_valid():
                key = proxy.dedup_key("server_port_protocol")
                if key not in seen:
                    seen.add(key)
                    proxies.append(proxy)
        except Exception:
            pass

    return proxies


def extract_surge_format(text: str) -> List[StandardProxy]:
    """从 Surge / Quantumult 配置格式中提取代理节点。

    格式: ProxyName = protocol, server, port, [options...]

    Args:
        text: Surge 格式文本

    Returns:
        StandardProxy 列表
    """
    proxies = []
    seen = set()

    for m in SURGE_PROXY_RE.finditer(text):
        full_line = m.group(0).strip()
        if len(full_line) < 20:
            continue

        name = m.group(1).strip()
        protocol = m.group(2).strip().lower()
        args_part = full_line.split("=", 1)[1].strip()
        if "," in args_part:
            # 移除协议名称后的逗号
            args = args_part.split(",", 1)[1].strip().split(",")
            args = [a.strip() for a in args]
        else:
            args = []

        # Surge 参数顺序: protocol, server, port, [username/uuid], [password], [extra...]
        server = args[0] if len(args) > 0 else ""
        port_str = args[1] if len(args) > 1 else ""
        port = int(port_str) if port_str.isdigit() else 0

        if not server or port <= 0:
            continue

        uuid = ""
        # Surge 参数后半段是凭据/选项混排（uuid、password、tls=true、sni=xx...），
        # 没有标准顺序——取第一个"不是选项"的参数当凭据（tls=/sni= 前缀是选项）
        for arg in args[2:]:
            if arg and not arg.startswith("tls=") and not arg.startswith("sni="):
                uuid = arg
                break

        # 检测 TLS
        tls = any(a.lower().startswith("tls=true") or a.lower() == "tls"
                  for a in args if "=" in a)

        # SNI
        sni = ""
        for arg in args:
            if arg.lower().startswith("sni="):
                sni = arg.split("=", 1)[1]
                break

        proxy = StandardProxy(
            protocol=protocol,
            server=server,
            port=port,
            uuid=uuid,
            remark=name,
        )
        if tls:
            proxy.tls = True
            proxy.sni = sni or server

        if proxy.is_valid():
            key = proxy.dedup_key("server_port_protocol")
            if key not in seen:
                seen.add(key)
                proxies.append(proxy)

    return proxies


def extract_from_code_blocks(text: str, max_depth: int = 3) -> List[StandardProxy]:
    """从 Markdown 代码块中递归提取节点。

    代码块中的内容可能是 Base64 编码的、YAML 的或纯文本 URI。

    Args:
        text: 包含 Markdown 代码块的文本
        max_depth: 最大递归深度（默认 3）

    Returns:
        StandardProxy 列表
    """
    if max_depth <= 0:
        return []

    proxies = []
    for m in CODE_BLOCK_RE.finditer(text):
        block = m.group(1) or m.group(2)
        if block and block.strip():
            proxies.extend(extract_all_strategies(block, max_depth - 1))
    return proxies


def extract_from_base64_text(text: str, max_depth: int = 3) -> List[StandardProxy]:
    """从 Base64 编码的文本中递归提取节点。

    找到文本中所有长 Base64 块，解码后递归提取。

    Args:
        text: 可能包含 Base64 块的文本
        max_depth: 最大递归深度（默认 3）

    Returns:
        StandardProxy 列表
    """
    if max_depth <= 0:
        return []

    proxies = []
    for m in BASE64_RE.finditer(text):
        decoded = safe_base64_decode(m.group(0))
        if decoded:
            proxies.extend(extract_all_strategies(decoded, max_depth - 1))
    return proxies


# ==================== 辅助函数 ====================

def _walk_dict_for_proxies(doc: dict, proxies: List[StandardProxy],
                           seen: Set[tuple], max_depth: int = 5):
    """递归遍历字典树，查找 proxies/outbounds 数组并提取节点。

    Args:
        doc: 配置字典
        proxies: 结果列表（原地修改）
        seen: 去重集合（原地修改）
        max_depth: 最大递归深度（默认 5）
    """
    if max_depth <= 0:
        return

    # 查找代理数组
    # 注意 'proxy-groups' 是 Clash 策略组（url-test/load-balance 等），
    # 不是节点——但 dict_to_standard_proxy 要求 type 在 supported 集合里，
    # 策略组的 type 永远不在其中，会返回 None，天然被过滤，不会误收
    for key in ('proxies', 'outbounds', 'proxy-groups'):
        if key in doc and isinstance(doc[key], list):
            for item in doc[key]:
                if isinstance(item, dict):
                    proxy = dict_to_standard_proxy(item)
                    if proxy and proxy.is_valid():
                        dedup_key = proxy.dedup_key("server_port_protocol")
                        if dedup_key not in seen:
                            seen.add(dedup_key)
                            proxies.append(proxy)

    # 递归进子字典
    for key, value in doc.items():
        if isinstance(value, dict):
            _walk_dict_for_proxies(value, proxies, seen, max_depth - 1)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _walk_dict_for_proxies(item, proxies, seen, max_depth - 1)


# ==================== 主入口 ====================

def extract_all_strategies(text: str, max_depth: int = 3) -> List[StandardProxy]:
    """使用所有策略从文本中提取代理节点。

    按顺序尝试：嵌入式 URI → Clash YAML → Sing-box JSON → Surge → 代码块 → Base64 递归。
    每种策略独立运行，最后由调用者做全局去重。

    Args:
        text: 任意文本内容
        max_depth: 递归深度（默认 3）

    Returns:
        StandardProxy 列表（当前文本内已去重）
    """
    # 文件大小限制：默认 100MB。超大文件跳过避免 OOM
    # 注意与 config.MAX_FILE_SIZE=None 的关系（两个不同的层）：
    #   MAX_FILE_SIZE=None  → 下载阶段不设上限（100MB 节点文件正常下载）
    #   此处的 100MB 兜底    → 解析阶段防 OOM（10+ 正则策略×超大文本会爆内存）
    # 即"下载不限制、解析兜底限制"，两者不矛盾
    from config import MAX_FILE_SIZE
    limit = MAX_FILE_SIZE or 100_000_000
    if not text or len(text) > limit:
        return []

    all_proxies = []
    seen = set()

    def _add(proxies: List[StandardProxy]):
        for p in proxies:
            key = p.dedup_key("server_port_protocol")
            if key not in seen:
                seen.add(key)
                all_proxies.append(p)

    # 策略 1: 嵌入式 URI（最可靠）
    _add(extract_embedded_uris(text))

    # 策略 2: Clash YAML
    _add(extract_clash_yaml(text))

    # 策略 3: Sing-box JSON
    _add(extract_singbox_json(text))

    # 策略 4: Surge / Quantumult
    _add(extract_surge_format(text))

    # 策略 5: 代码块递归
    _add(extract_from_code_blocks(text, max_depth))

    # 策略 6: Base64 递归
    _add(extract_from_base64_text(text, max_depth))

    return all_proxies


# ==================== 兼容旧接口 ====================

def _parse_flow_object(obj_str: str) -> Optional[StandardProxy]:
    """逐行解析 YAML flow 对象 {key: value, ...}。

    处理嵌套花括号（如 ws-opts: {path: /xxx, headers: {Host: yyy}}），
    用括号计数找到真正的键值对边界。
    """
    if not (obj_str.startswith('{') and obj_str.endswith('}')):
        return None
    inner = obj_str[1:-1].strip()
    pairs = _split_flow_kv(inner)
    d = {}
    for k, v in pairs:
        # 值可能是嵌套对象，尝试 YAML 解析，失败就用原始字符串
        if v.startswith('{') and v.endswith('}'):
            try:
                import yaml
                v = yaml.safe_load(v)
            except Exception:
                pass
        elif v in ('true', 'True'):
            v = True
        elif v in ('false', 'False'):
            v = False
        else:
            try:
                v = int(v)
            except ValueError:
                v = v.strip('"').strip("'")
        d[k] = v
    return dict_to_standard_proxy(d)


def _split_flow_kv(s: str) -> list:
    """在 YAML flow 对象字符串中分割 key: value 对，容忍嵌套花括号。"""
    pairs = []
    i = 0
    while i < len(s):
        # 跳过前导空格和逗号
        while i < len(s) and s[i] in ' ,':
            i += 1
        if i >= len(s):
            break
        # 找到 key（':' 之前）
        j = i
        while j < len(s) and s[j] != ':':
            j += 1
        key = s[i:j].strip()
        j += 1  # 跳过 ':'
        # 跳过空格
        while j < len(s) and s[j] == ' ':
            j += 1
        # 找到 value
        if j < len(s) and s[j] == '{':
            # 嵌套对象：数括号找到匹配的 }
            depth = 0
            start = j
            while j < len(s):
                if s[j] == '{':
                    depth += 1
                elif s[j] == '}':
                    depth -= 1
                    if depth == 0:
                        j += 1
                        break
                j += 1
            val = s[start:j]
        else:
            # 简单值：到下一个 ',' 或字符串结束
            start = j
            while j < len(s) and s[j] != ',':
                j += 1
            val = s[start:j].strip().rstrip(',')
        pairs.append((key, val.strip()))
        i = j
    return pairs


def extract_raw_candidates(text: str) -> List[str]:
    """提取 所有候选 URI 字符串（原始格式，未经协议解析）。

    保留此接口用于向后兼容。新代码应使用 extract_all_strategies()。
    """
    candidates = discover_candidates(text)
    return list(dict.fromkeys(candidates))  # 保持顺序去重
