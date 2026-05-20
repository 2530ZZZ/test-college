"""
节点解析器模块。
每个解析器函数负责一种或一类格式，
返回 StandardProxy 对象或 None。
所有解析器最终汇总到 parse_line() 函数供外部调用。
"""

import re
import json
from typing import List, Optional
from proxy_model import StandardProxy
from utils import safe_base64_decode


# ==================== 协议链接正则（预编译） ====================
# 匹配 vmess://, vless://, trojan://, ss://, hysteria2://, hy2://, tuic:// 等
PROTOCOL_URI_RE = re.compile(
    r'(?i)(vmess|vless|trojan|ss|ssr|hysteria2?|hy2|tuic|reality)://[^\s<>"\']+'
)

# Shadowsocks SIP002 格式（带 plugin）
SS_URI_RE = re.compile(r'ss://[A-Za-z0-9+/=]+(?:\?[^\s<>"\']*)?(?:#[^\s<>"\']*)?', re.IGNORECASE)

# Clash 单行 YAML 代理
CLASH_YAML_SINGLE_RE = re.compile(
    r'-\s*\{[^}]*?(?:name|server|port|type|uuid|password|ps|flow)[^}]*\}', re.IGNORECASE | re.DOTALL
)

# Clash 多行 YAML 代理
CLASH_YAML_MULTI_RE = re.compile(
    r'-\s*name:.*?(?=-\s*name:|\Z)', re.IGNORECASE | re.DOTALL | re.MULTILINE
)

# Base64 大段数据（长度大于100）
BASE64_BLOB_RE = re.compile(r'[A-Za-z0-9+/=]{100,}')


def extract_raw_links(text: str) -> List[str]:
    """从文本中提取所有可能的原始订阅链接或协议链接（尚未转换为 StandardProxy）"""
    raw_nodes = []

    # 1. Markdown 代码块
    for match in re.findall(r'(?:```(?:[\w]*)\n?)([\s\S]*?)(?:\n?```)|`([^`\n]+)`', text):
        block = match[0] or match[1]
        if block and block.strip():
            raw_nodes.extend(extract_raw_links(block))

    # 2. 大段 Base64 解码后再提取
    for candidate in BASE64_BLOB_RE.findall(text):
        decoded = safe_base64_decode(candidate)
        if decoded:
            raw_nodes.extend(extract_raw_links(decoded))

    # 3. 协议链接
    raw_nodes.extend(PROTOCOL_URI_RE.findall(text))
    raw_nodes.extend(SS_URI_RE.findall(text))

    # 4. Clash YAML 单行
    for match in CLASH_YAML_SINGLE_RE.findall(text):
        clean = re.sub(r'\s+', ' ', match.strip())
        if len(clean) > 40:
            raw_nodes.append(clean)

    # 5. Clash YAML 多行
    for match in CLASH_YAML_MULTI_RE.findall(text):
        clean = re.sub(r'\s+', ' ', match.strip())
        if len(clean) > 30 and ('server:' in clean or 'type:' in clean):
            raw_nodes.append(clean)

    # 6. JSON proxies 数组
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            proxies = data.get("proxies") or data.get("outbounds")
            if isinstance(proxies, list):
                for p in proxies:
                    if isinstance(p, dict):
                        raw_nodes.append(json.dumps(p, ensure_ascii=False))
                    elif isinstance(p, str):
                        raw_nodes.append(p)
    except:
        pass

    # 7. 文本中 "proxies": [...] 数组
    for arr in re.findall(r'"proxies"\s*:\s*\[([\s\S]*?)\]', text, re.IGNORECASE):
        for obj in re.findall(r'\{[\s\S]*?\}', arr):
            if any(k in obj.lower() for k in ["server", "port", "type", "uuid"]):
                raw_nodes.append(obj.strip())

    # 去重、过滤空行和过短行
    seen = set()
    clean_lines = []
    for line in raw_nodes:
        line = line.strip()
        if not line or len(line) < 15 or line in seen:
            continue
        seen.add(line)
        clean_lines.append(line)

    return clean_lines


def parse_line(line: str) -> Optional[StandardProxy]:
    """
    将一行文本解析为 StandardProxy 对象。
    这是所有解析器的统一入口，依次尝试不同的解析策略。
    """
    # 跳过纯注释或明显不是节点的行
    if line.startswith('//') or line.startswith('#') or len(line) < 10:
        return None

    # ==================== 各协议解析器 ====================
    parser = None
    line_lower = line.lower()

    # 简单判断协议类型，分发给具体解析函数
    if line_lower.startswith(('vmess://', 'vless://', 'trojan://', 'hysteria2://', 'hy2://', 'tuic://', 'reality://')):
        parser = parse_uri_based
    elif line_lower.startswith('ss://'):
        parser = parse_ss
    elif line_lower.startswith('ssr://'):
        parser = parse_ssr
    elif line.startswith('- {') and ('name' in line_lower or 'type' in line_lower):
        parser = parse_clash_yaml_single
    elif line.startswith('- name:') and ('server:' in line_lower or 'type:' in line_lower):
        parser = parse_clash_yaml_multi
    else:
        # 最后尝试 JSON 对象
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and 'server' in obj:
                parser = parse_json_proxy
            else:
                return None
        except:
            return None

    if parser:
        return parser(line)
    return None


# ==================== 具体解析函数 ====================

def parse_uri_based(uri: str) -> Optional[StandardProxy]:
    """解析 vmess/vless/trojan/hysteria2/tuic 等 URI"""
    # 简化实现：直接返回一个仅包含 raw_link 的对象，实际解析可根据需要细化
    protocol = uri.split('://')[0].lower()
    proxy = StandardProxy(protocol=protocol, server="", port=0, raw_link=uri)
    # 可以在此补充更多字段提取，但为了简单，保留 raw_link 即可
    return proxy


def parse_ss(ss_uri: str) -> Optional[StandardProxy]:
    """解析 Shadowsocks 链接"""
    proxy = StandardProxy(protocol="ss", server="", port=0, raw_link=ss_uri)
    return proxy


def parse_ssr(ssr_uri: str) -> Optional[StandardProxy]:
    """解析 SSR 链接"""
    proxy = StandardProxy(protocol="ssr", server="", port=0, raw_link=ssr_uri)
    return proxy


def parse_clash_yaml_single(line: str) -> Optional[StandardProxy]:
    """解析 Clash 单行 YAML 代理 - {name: xx, server: xx, ...}"""
    proxy = StandardProxy(protocol="unknown", server="", port=0, raw_link=line)
    return proxy


def parse_clash_yaml_multi(line: str) -> Optional[StandardProxy]:
    """解析 Clash 多行 YAML 代理"""
    proxy = StandardProxy(protocol="unknown", server="", port=0, raw_link=line)
    return proxy


def parse_json_proxy(json_str: str) -> Optional[StandardProxy]:
    """解析 JSON 代理对象"""
    proxy = StandardProxy(protocol="unknown", server="", port=0, raw_link=json_str)
    return proxy
