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
# Clash 单行 YAML 代理
CLASH_SINGLE_RE = re.compile(r'-\s*\{[^}]*?(?:name|server|port|type|uuid|password|ps|flow)[^}]*\}', re.DOTALL)
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

    # 4. Clash YAML 单行代理
    for m in CLASH_SINGLE_RE.findall(text):
        clean = re.sub(r'\s+', ' ', m.strip())
        if len(clean) > 40:
            candidates.append(clean)

    # 5. Clash YAML 多行代理
    for m in CLASH_MULTI_RE.findall(text):
        clean = re.sub(r'\s+', ' ', m.strip())
        if len(clean) > 30 and ('server:' in clean or 'type:' in clean):
            candidates.append(clean)

    # 6. JSON 格式的 proxies/outbounds 数组
    for arr in JSON_PROXY_ARRAY_RE.findall(text):
        # 分割 JSON 对象
        for obj in re.findall(r'\{[\s\S]*?\}', arr):
            # 尝试解析为 dict，若失败则保留原字符串
            try:
                proxy_dict = json.loads(obj)
                # 如果是标准 JSON 代理，直接序列化回去（保持统一）
                candidates.append(json.dumps(proxy_dict, ensure_ascii=False))
            except Exception:
                # 不完整的 JSON 片段，尝试清理后保留
                clean_obj = re.sub(r'\s+', ' ', obj.strip())
                if any(k in clean_obj.lower() for k in ['server', 'port', 'type', 'uuid']):
                    candidates.append(clean_obj)

    # 7. 尝试解析整个文本作为 JSON（整个文件就是一个 Clash/Sing-box 配置）
    try:
        config = json.loads(text)
        if isinstance(config, dict):
            # 提取 proxies / outbounds
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

    # 尝试判断格式
    lower = line.lower()
    # 标准 URI 链接
    if any(lower.startswith(f'{p}://') for p in ('vmess', 'vless', 'trojan', 'ss', 'ssr',
                                                  'hysteria', 'hysteria2', 'hy2', 'tuic',
                                                  'reality', 'wireguard', 'sing-box')):
        return parse_uri(line)
    # Clash YAML 单行对象
    if line.startswith('- {') and ('name' in lower or 'type' in lower):
        return parse_clash_single(line)
    # Clash 多行代理片段
    if line.startswith('- name:') and ('server:' in lower or 'type:' in lower):
        return parse_clash_multi(line)
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
        # 分割协议和其余部分
        protocol, rest = uri.split('://', 1)
        protocol = protocol.lower()
        proxy = StandardProxy(protocol=protocol, raw_link=uri)

        if protocol == 'vmess':
            # vmess 链接是 base64 编码的 JSON
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
            # vless://uuid@server:port?params#remark
            # trojan://password@server:port?params#remark
            # 分离出 用户信息、主机端口、参数、备注
            # 1. 去掉备注
            note = ''
            if '#' in rest:
                rest, note = rest.split('#', 1)
                proxy.remark = urllib.parse.unquote(note)
            # 2. 分离参数部分
            params = {}
            if '?' in rest:
                rest, query = rest.split('?', 1)
                for item in query.split('&'):
                    if '=' in item:
                        k, v = item.split('=', 1)
                        params[k] = v
            # 3. 分离用户信息和主机端口
            if '@' in rest:
                userinfo, hostport = rest.split('@', 1)
                proxy.uuid = urllib.parse.unquote(userinfo)
            else:
                hostport = rest
            # 4. 解析主机和端口
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
            # 5. 参数解析
            proxy.transport = params.get('type', 'tcp')
            proxy.security = params.get('security', 'none')
            proxy.sni = params.get('sni', '')
            proxy.tls = params.get('security', '') == 'tls' or 'sni' in params
            if not proxy.remark and proxy.server:
                proxy.remark = f"{protocol}-{proxy.server}:{proxy.port}"
        elif protocol == 'ss':
            # ss://base64(method:password)@server:port?plugin=...
            # 去掉备注
            note = ''
            rest_no_note = rest
            if '#' in rest:
                rest_no_note, note = rest.split('#', 1)
                proxy.remark = urllib.parse.unquote(note)
            # 分离参数
            params = {}
            if '?' in rest_no_note:
                rest_no_note, query = rest_no_note.split('?', 1)
                for item in query.split('&'):
                    if '=' in item:
                        k, v = item.split('=', 1)
                        params[k] = v
            # 处理用户信息部分（可能经过 base64 编码）
            if '@' in rest_no_note:
                userinfo, hostport = rest_no_note.split('@', 1)
                # userinfo 一般是 base64(method:password)
                decoded_userinfo = safe_base64_decode(userinfo)
                if decoded_userinfo and ':' in decoded_userinfo:
                    method, pwd = decoded_userinfo.split(':', 1)
                    proxy.security = method
                    proxy.uuid = pwd
                else:
                    # 可能是明文
                    if ':' in userinfo:
                        proxy.security, proxy.uuid = userinfo.split(':', 1)
            else:
                hostport = rest_no_note
            # 解析主机端口
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
            # 插件参数
            if 'plugin' in params:
                plugin_info = urllib.parse.unquote(params['plugin'])
                if ';' in plugin_info:
                    plugin_name, plugin_opts = plugin_info.split(';', 1)
                    # 简化：记录 transport 为 plugin 名
                    proxy.transport = plugin_name
                    # 实际插件参数可能包含 obfs, tls 等，这里不深入
        elif protocol == 'ssr':
            # ssr:// 为 base64 编码的复杂结构，暂时保留 raw_link 并尝试解码
            decoded = safe_base64_decode(rest)
            if decoded:
                # 格式 server:port:protocol:method:obfs:password_base64?...
                parts = decoded.split(':')
                if len(parts) >= 6:
                    proxy.server = parts[0]
                    proxy.port = int(parts[1])
                    proxy.transport = parts[2]
                    proxy.security = parts[3]
                    # 密码为 base64 编码
                    pwd_raw = parts[5].split('/?')[0].split('&')[0] if '?' in parts[5] else parts[5]
                    pwd_decoded = safe_base64_decode(pwd_raw)
                    proxy.uuid = pwd_decoded or pwd_raw
                    # 备注
                    if '#' in uri:
                        proxy.remark = urllib.parse.unquote(uri.split('#')[1])
        elif protocol in ('hysteria', 'hysteria2', 'hy2'):
            # hysteria://password@server:port?params#remark
            # 类似 trojan
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
            proxy.tls = True  # hysteria 默认 TLS
        elif protocol == 'tuic':
            # tuic://uuid:password@server:port?params#remark
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
                    # TUIC 密码通常就是第二个部分
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
            # vless 的一种，但具有 reality 特性，格式类似 vless
            # vless://uuid@server:port?type=tcp&security=reality&...
            # 直接复用 vless 解析
            return parse_uri(uri.replace('reality://', 'vless://'))
        elif protocol == 'wireguard':
            # wireguard 通常以配置文件形式，URI 较少见
            pass
        # 补齐其他协议...

        # 如果 server 为空，则尝试从 raw_link 进一步提取（兜底）
        if not proxy.server:
            # 简单尝试从 remark 或 link 猜测
            pass

        return proxy
    except Exception as e:
        # 解析失败，至少返回包含 raw_link 的基础对象
        return StandardProxy(raw_link=uri, protocol=uri.split('://')[0])


def parse_clash_single(line: str) -> Optional[StandardProxy]:
    """解析 Clash 单行 YAML 代理 - {name: xx, server: xx, type: vless, ...}"""
    try:
        # 尝试将其转为 JSON 处理
        obj_str = line.strip()[1:].strip()  # 移除开头的 '-'
        # 简单替换 YAML 风格的键值对为 JSON 风格（容错）
        # 更可靠的方式是用 yaml 库，但这里为了轻量，手动处理常见情况
        # 转换为 JSON 格式：去除注释，替换冒号+空格为 JSON
        # 直接使用正则提取关键字段
        fields = {}
        for key in ['name', 'type', 'server', 'port', 'uuid', 'password', 'sni', 'servername']:
            match = re.search(rf'{key}:\s*"?([^",\n}}]*)', line, re.IGNORECASE)
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
    """解析 Clash 多行 YAML 代理片段"""
    fields = {}
    for key in ['name', 'type', 'server', 'port', 'uuid', 'password', 'sni', 'servername']:
        match = re.search(rf'{key}:\s*"?([^",\n}}]*)', line, re.IGNORECASE)
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
        if p and p.server and p.port:   # 必须至少包含服务器和端口
            p.source_url = source_url
            proxies.append(p)
        elif p and p.raw_link:  # 如果至少提取到了 raw_link，也可保留（测速时可用 raw_link）
            p.source_url = source_url
            proxies.append(p)
    return proxies
