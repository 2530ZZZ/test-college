"""
节点解析器模块 —— 支持从任意文本中提取所有主流协议节点。
支持格式：URI链接、Base64编码、Clash YAML、Sing-box JSON、Markdown代码块等。
每个解析函数返回 Optional[StandardProxy]，填充 server, port, protocol 等字段。
"""

import re
import json
import base64
import urllib.parse
from typing import List, Optional
from proxy_model import StandardProxy
from utils import safe_base64_decode


# ==================== 预编译正则 ====================
# 协议 URI 匹配 (支持更多变体)
URI_RE = re.compile(
    r'(?i)(vmess|vless|trojan|ss|ssr|hysteria2?|hy2|tuic|reality|wireguard|sing-box)://[^\s<>"\'`]+'
)
# ss:// 格式 (处理特殊字符、注释)
SS_URI_RE = re.compile(
    r'ss://[A-Za-z0-9+/=]+(?:\?[^\s<>"\'`]*)?(?:#[^\s<>"\'`]*)?'
)
# Base64 大块数据
BASE64_RE = re.compile(r'[A-Za-z0-9+/=]{100,}')
# Markdown 代码块
CODE_BLOCK_RE = re.compile(r'(?:```(?:[\w]*)\n?)([\s\S]*?)(?:\n?```)|`([^`\n]+)`')
# Clash 单行 YAML 代理 (大括号格式: - { name: ... })
CLASH_SINGLE_RE = re.compile(r'-\s*\{[^}]*?(?:name|server|port|type|uuid|password|ps|flow)[^}]*\}', re.DOTALL)
# --- 修复：用于拆分单行 YAML 节点的新正则表达式 ---
# 匹配以 "- name:" 或 "- " 开头的行，并匹配到下一个 "- " 或字符串末尾
CLASH_SINGLE_YAML_RE = re.compile(r'(?:^|\n)\s*(-\s+name:.*?)(?=\n\s*-|\Z)', re.DOTALL)
# Clash 多行 YAML 代理 (直到下一个 - name: 或文件结尾)
CLASH_MULTI_RE = re.compile(r'-\s*name:.*?(?=-\s*name:|\Z)', re.DOTALL)
# JSON 格式 proxies/outbounds 提取
JSON_PROXY_ARRAY_RE = re.compile(r'"(?:proxies|outbounds)"\s*:\s*\[([\s\S]*?)\]', re.IGNORECASE)


def extract_raw_candidates(text: str) -> List[str]:
    """
    从任意文本中提取所有可能的节点候选字符串（未解析）。
    会递归处理 Markdown 代码块和 Base64 解码后的内容。
    返回去重后的候选行列表。
    """
    candidates = []

    # 1. 先处理 Markdown 代码块（递归调用自身）
    for match in CODE_BLOCK_RE.finditer(text):
        block = match.group(1) or match.group(2)
        if block and block.strip():
            candidates.extend(extract_raw_candidates(block))

    # 2. 处理大段 Base64（解码后可能包含更多节点）
    for b64 in BASE64_RE.findall(text):
        decoded = safe_base64_decode(b64)
        if decoded:
            candidates.extend(extract_raw_candidates(decoded))

    # 3. 提取 URI 链接
    for uri in URI_RE.findall(text):
        candidates.append(uri)
    for ss in SS_URI_RE.findall(text):
        candidates.append(ss)

    # 4. Clash YAML 单行代理 (大括号格式)
    for m in CLASH_SINGLE_RE.findall(text):
        clean = re.sub(r'\s+', ' ', m.strip())
        if len(clean) > 40:
            candidates.append(clean)

    # 5. Clash YAML 多行代理 (传统的多行格式)
    for m in CLASH_MULTI_RE.findall(text):
        clean = re.sub(r'\s+', ' ', m.strip())
        # 只有当它看起来像是一个多行块时才添加，避免误判
        if '\n' in m and len(clean) > 30 and ('server:' in clean or 'type:' in clean):
            candidates.append(clean)

    # 5.1. --- 新增：提取单行 YAML 节点 ---
    # 匹配以 "- name:" 开头的行，直到下一个 "- " 或字符串末尾
    for m in CLASH_SINGLE_YAML_RE.findall(text):
        clean = re.sub(r'\s+', ' ', m.strip())
        if len(clean) > 30 and ('server:' in clean or 'type:' in clean):
            candidates.append(clean)

    # 6. JSON 格式的 proxies/outbounds 数组
    for arr in JSON_PROXY_ARRAY_RE.findall(text):
        for obj in re.findall(r'\{[\s\S]*?\}', arr):
            try:
                proxy_dict = json.loads(obj)
                candidates.append(json.dumps(proxy_dict, ensure_ascii=False))
            except Exception:
                clean_obj = re.sub(r'\s+', ' ', obj.strip())
                if any(k in clean_obj.lower() for k in ['server', 'port', 'type', 'uuid']):
                    candidates.append(clean_obj)

    # 7. 尝试解析整个文本作为 JSON（整个文件就是一个 Clash/Sing-box 配置）
    try:
        config = json.loads(text)
        if isinstance(config, dict):
            for key in ('proxies', 'outbounds'):
                if key in config and isinstance(config[key], list):
                    for item in config[key]:
                        if isinstance(item, dict):
                            candidates.append(json.dumps(item, ensure_ascii=False))
                        elif isinstance(item, str):
                            candidates.append(item)
    except Exception:
        pass

    # 去重、过滤空行和过短行
    seen = set()
    clean_candidates = []
    for c in candidates:
        c = c.strip()
        if not c or len(c) < 15 or c in seen:
            continue
        seen.add(c)
        clean_candidates.append(c)

    return clean_candidates


def parse_single_line(line: str) -> Optional[StandardProxy]:
    """
    将单行候选字符串解析为 StandardProxy。
    根据开头特征自动选择解析器。
    """
    if not line:
        return None

    lower = line.lower()
    # 标准 URI 链接
    if any(lower.startswith(f'{p}://') for p in ('vmess', 'vless', 'trojan', 'ss', 'ssr',
                                                  'hysteria', 'hysteria2', 'hy2', 'tuic',
                                                  'reality', 'wireguard', 'sing-box')):
        return parse_uri(line)
    # Clash YAML 单行对象 (大括号)
    if line.startswith('- {') and ('name' in lower or 'type' in lower):
        return parse_clash_single(line)
    # --- 修复：区分单行和多行 Clash YAML ---
    # 如果看起来像是一个YAML节点（以 "- name:" 开头），先检查它是否包含换行符
    if line.startswith('- name:') and ('server:' in lower or 'type:' in lower):
        if '\n' in line:
            # 包含换行符，仍然尝试作为多行块处理
            return parse_clash_multi(line)
        else:
            # 不包含换行符，直接作为单行解析
            return parse_clash_multi(line)  # parse_clash_multi 内部的正则已修复
    # JSON 对象
    if line.startswith('{') and ('server' in lower or 'type' in lower):
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                return parse_json_proxy(obj)
        except Exception:
            pass
    # 最后尝试作为原始字符串解析（比如某些纯 base64 的 vmess 链接）
    return parse_raw_link(line)


def parse_raw_link(raw: str) -> Optional[StandardProxy]:
    """处理不含协议头但实际上是 vmess:// 之类的原始链接（如 base64 解码后）"""
    for prefix in ('vmess://', 'vless://', 'trojan://', 'ss://', 'ssr://',
                   'hysteria://', 'hysteria2://', 'hy2://', 'tuic://', 'reality://'):
        if raw.startswith(prefix):
            return parse_uri(raw)
    return None


# ==================== 具体解析器 ====================

def parse_uri(uri: str) -> Optional[StandardProxy]:
    """解析 vmess://, vless://, trojan://, ss://, hysteria2:// 等 URI"""
    try:
        protocol, rest = uri.split('://', 1)
        protocol = protocol.lower()
        proxy = StandardProxy(protocol=protocol, raw_link=uri)

        if protocol == 'vmess':
            decoded = safe_base64_decode(rest)
            if decoded:
                try:
                    info = json.loads(decoded)
                    proxy.server = info.get('add', '')
                    proxy.port = int(info.get('port', 0))
                    proxy.uuid = info.get('id', '')
                    proxy.security = info.get('scy', 'auto')
                    proxy.transport = info.get('net', 'tcp')
                    proxy.tls = info.get('tls', '') == 'tls'
                    proxy.sni = info.get('sni', '')
                    proxy.remark = info.get('ps', '') or urllib.parse.unquote(rest.split('#')[-1]) if '#' in rest else ''
                except Exception:
                    pass
        elif protocol in ('vless', 'trojan'):
            note = ''
            if '#' in rest:
                rest, note = rest.split('#', 1)
                proxy.remark = urllib.parse.unquote(note)
            params = {}
            if '?' in rest:
                rest, query = rest.split('?', 1)
                for item in query.split('&'):
                    if '=' in item:
                        k, v = item.split('=', 1)
                        params[k] = v
            if '@' in rest:
                userinfo, hostport = rest.split('@', 1)
                proxy.uuid = urllib.parse.unquote(userinfo)
            else:
                hostport = rest
            if '[' in hostport and ']' in hostport:  # IPv6
                host, port_part = hostport.rsplit(':', 1)
            else:
                parts = hostport.rsplit(':', 1)
                if len(parts) == 2:
                    host, port_part = parts
                else:
                    host = parts[0]
                    port_part = '443'
            proxy.server = host
            proxy.port = int(port_part)
            proxy.transport = params.get('type', 'tcp')
            proxy.security = params.get('security', 'none')
            proxy.sni = params.get('sni', '')
            proxy.tls = params.get('security', '') == 'tls' or 'sni' in params
            if not proxy.remark and proxy.server:
                proxy.remark = f"{protocol}-{proxy.server}:{proxy.port}"
        elif protocol == 'ss':
            note = ''
            rest_no_note = rest
            if '#' in rest:
                rest_no_note, note = rest.split('#', 1)
                proxy.remark = urllib.parse.unquote(note)
            params = {}
            if '?' in rest_no_note:
                rest_no_note, query = rest_no_note.split('?', 1)
                for item in query.split('&'):
                    if '=' in item:
                        k, v = item.split('=', 1)
                        params[k] = v
            if '@' in rest_no_note:
                userinfo, hostport = rest_no_note.split('@', 1)
                decoded_userinfo = safe_base64_decode(userinfo)
                if decoded_userinfo and ':' in decoded_userinfo:
                    method, pwd = decoded_userinfo.split(':', 1)
                    proxy.security = method
                    proxy.uuid = pwd
                else:
                    if ':' in userinfo:
                        proxy.security, proxy.uuid = userinfo.split(':', 1)
            else:
                hostport = rest_no_note
            if '[' in hostport and ']' in hostport:
                host, port_part = hostport.rsplit(':', 1)
            else:
                parts = hostport.rsplit(':', 1)
                if len(parts) == 2:
                    host, port_part = parts
                else:
                    host = parts[0]
                    port_part = '8388'
            proxy.server = host
            proxy.port = int(port_part)
            if 'plugin' in params:
                plugin_info = urllib.parse.unquote(params['plugin'])
                if ';' in plugin_info:
                    plugin_name, plugin_opts = plugin_info.split(';', 1)
                    proxy.transport = plugin_name
        elif protocol == 'ssr':
            decoded = safe_base64_decode(rest)
            if decoded:
                parts = decoded.split(':')
                if len(parts) >= 6:
                    proxy.server = parts[0]
                    proxy.port = int(parts[1])
                    proxy.transport = parts[2]
                    proxy.security = parts[3]
                    pwd_raw = parts[5].split('/?')[0].split('&')[0] if '?' in parts[5] else parts[5]
                    pwd_decoded = safe_base64_decode(pwd_raw)
                    proxy.uuid = pwd_decoded or pwd_raw
                    if '#' in uri:
                        proxy.remark = urllib.parse.unquote(uri.split('#')[1])
        elif protocol in ('hysteria', 'hysteria2', 'hy2'):
            note = ''
            if '#' in rest:
                rest, note = rest.split('#', 1)
                proxy.remark = urllib.parse.unquote(note)
            params = {}
            if '?' in rest:
                rest, query = rest.split('?', 1)
                for item in query.split('&'):
                    if '=' in item:
                        k, v = item.split('=', 1)
                        params[k] = v
            if '@' in rest:
                password, hostport = rest.split('@', 1)
                proxy.uuid = password
            else:
                hostport = rest
            if '[' in hostport and ']' in hostport:
                host, port_part = hostport.rsplit(':', 1)
            else:
                parts = hostport.rsplit(':', 1)
                if len(parts) == 2:
                    host, port_part = parts
                else:
                    host = parts[0]
                    port_part = '443'
            proxy.server = host
            proxy.port = int(port_part)
            proxy.sni = params.get('sni', '')
            proxy.tls = True
        elif protocol == 'tuic':
            note = ''
            if '#' in rest:
                rest, note = rest.split('#', 1)
                proxy.remark = urllib.parse.unquote(note)
            params = {}
            if '?' in rest:
                rest, query = rest.split('?', 1)
                for item in query.split('&'):
                    if '=' in item:
                        k, v = item.split('=', 1)
                        params[k] = v
            if '@' in rest:
                userinfo, hostport = rest.split('@', 1)
                if ':' in userinfo:
                    uid, pwd = userinfo.split(':', 1)
                    proxy.uuid = uid
                    proxy.security = pwd
            else:
                hostport = rest
            if '[' in hostport and ']' in hostport:
                host, port_part = hostport.rsplit(':', 1)
            else:
                parts = hostport.rsplit(':', 1)
                if len(parts) == 2:
                    host, port_part = parts
                else:
                    host = parts[0]
                    port_part = '443'
            proxy.server = host
            proxy.port = int(port_part)
            proxy.sni = params.get('sni', '')
            proxy.tls = True
        elif protocol == 'reality':
            return parse_uri(uri.replace('reality://', 'vless://'))
        elif protocol == 'wireguard':
            pass

        return proxy
    except Exception as e:
        return StandardProxy(raw_link=uri, protocol=uri.split('://')[0])


def parse_clash_single(line: str) -> Optional[StandardProxy]:
    """解析 Clash 单行 YAML 代理 - {name: xx, server: xx, type: vless, ...}"""
    try:
        fields = {}
        for key in ['name', 'type', 'server', 'port', 'uuid', 'password', 'sni', 'servername']:
            # 同样适用修复后的正则表达式
            match = re.search(rf'{key}:\s*"?([^"]*?)"?(?=\s+\S+:|$)', line, re.IGNORECASE)
            if match:
                fields[key] = match.group(1).strip().strip('"')
        if 'server' not in fields or 'port' not in fields:
            return None
        proxy = StandardProxy(
            protocol=fields.get('type', '').lower(),
            server=fields['server'],
            port=int(fields['port']),
            uuid=fields.get('uuid', fields.get('password', '')),
            remark=fields.get('name', ''),
            raw_link=line
        )
        return proxy
    except Exception:
        return None


def parse_clash_multi(line: str) -> Optional[StandardProxy]:
    """解析 Clash 多行 YAML 代理片段，同时兼容单行 YAML 格式。"""
    fields = {}
    for key in ['name', 'type', 'server', 'port', 'uuid', 'password', 'sni', 'servername']:
        # --- 修复核心：使用非贪婪捕获并添加前瞻断言 ---
        # 这个正则表达式会匹配键后面的值，并在遇到下一个键（非空格字符 + 冒号）或行尾时停止。
        match = re.search(rf'{key}:\s*"?([^"]*?)"?(?=\s+\S+:|$)', line, re.IGNORECASE)
        if match:
            fields[key] = match.group(1).strip().strip('"')
    
    if 'server' not in fields or 'port' not in fields:
        return None
    
    try:
        proxy = StandardProxy(
            protocol=fields.get('type', '').lower(),
            server=fields['server'],
            port=int(fields['port']),
            uuid=fields.get('uuid', fields.get('password', '')),
            remark=fields.get('name', ''),
            raw_link=line
        )
        return proxy
    except (ValueError, TypeError):
        return None


def parse_json_proxy(obj: dict) -> Optional[StandardProxy]:
    """解析 JSON 格式的代理对象"""
    try:
        protocol = obj.get('type', obj.get('protocol', 'unknown')).lower()
        return StandardProxy(
            protocol=protocol,
            server=obj.get('server', obj.get('host', '')),
            port=int(obj.get('port', 0)),
            uuid=obj.get('uuid', obj.get('password', obj.get('id', ''))),
            security=obj.get('security', obj.get('cypher', '')),
            transport=obj.get('network', obj.get('type', 'tcp')),
            tls=obj.get('tls', False) in (True, 'true', 'tls'),
            sni=obj.get('sni', obj.get('servername', '')),
            remark=obj.get('name', obj.get('ps', '')),
            raw_link=json.dumps(obj, ensure_ascii=False)
        )
    except Exception:
        return None


def extract_and_parse(text: str, source_url: str = "") -> List[StandardProxy]:
    """
    对外主函数：从任意文本中提取所有节点，并解析为标准对象。
    source_url 用于记录来源，后续测速可以追溯。
    """
    candidates = extract_raw_candidates(text)
    proxies = []
    for cand in candidates:
        p = parse_single_line(cand)
        if p and p.server and p.port:
            p.source_url = source_url
            proxies.append(p)
        elif p and p.raw_link:
            p.source_url = source_url
            proxies.append(p)
    return proxies
