"""
节点候选提取模块 —— 只负责从任意文本中提取"看起来像节点"的候选块。
核心设计思路：
  - 本模块不做协议解析，只做文本提取。
  - 所有协议解析（ss://、vmess://、trojan:// 等）交给 mihomo 的 link 字段处理。
  - mihomo 原生支持 Surge/Loon/Clash YAML 格式，只需把原始文本块传入即可。

参考成熟项目：
  - Sub-Store: 50+ 解析器，策略模式，test() 判断格式，parse() 提取字段
  - subconverter-rs: 前缀匹配，explode() 路由，统一 Proxy 模型
  - mihomo: 原生支持 link 字段直接加载几乎所有主流格式
"""

import re
import json
from typing import List
from utils import safe_base64_decode

# ==================== 预编译正则（长度限制，避免灾难性回溯） ====================

# 协议 URI（每个协议单独匹配，便于后续扩展）
URI_SCHEMES = r'(?:vmess|vless|trojan|ss|ssr|hysteria|hysteria2|hy2|tuic|reality|wireguard|sing-box)'
URI_RE = re.compile(rf'(?i){URI_SCHEMES}://\S{{1,800}}')

# ss:// 格式（SIP002，支持 plugin 参数）
SS_URI_RE = re.compile(
    r'ss://[A-Za-z0-9+/=]{1,500}(?:\?[^\s<>"\'`]{1,300})?(?:#[^\s<>"\'`]{1,200})?'
)

# ssr:// 格式（Base64 编码）
SSR_URI_RE = re.compile(r'ssr://[A-Za-z0-9+/=]{1,800}')

# hysteria2:// 或 hy2:// 格式
HY2_URI_RE = re.compile(r'(?i)hysteria2://\S{1,600}')
HY2_SHORT_RE = re.compile(r'(?i)hy2://\S{1,600}')

# tuic:// 格式
TUIC_URI_RE = re.compile(r'(?i)tuic://\S{1,600}')

# Base64 大块数据（可能是整个订阅）
BASE64_RE = re.compile(r'[A-Za-z0-9+/=]{100,8000}')

# Markdown 代码块
CODE_BLOCK_RE = re.compile(
    r'(?:```(?:[\w]*)\n?)([\s\S]{1,200000}?)(?:\n?```)'  # 最多200KB
    r'|`([^`\n]{1,2000})`'  # 行内代码
)

# Clash YAML 单行代理 (- { name: ..., type: ..., server: ..., port: ... })
CLASH_SINGLE_RE = re.compile(
    r'-\s*\{[^}]{1,3000}?'
    r'(?:name|server|port|type|uuid|password|ps|flow|sni|fp|reality-opts)'
    r'[^}]{0,3000}\}',
    re.DOTALL
)

# Clash YAML 多行代理 (- name: ... \n   server: ... \n   port: ...)
CLASH_MULTI_RE = re.compile(
    r'-\s*name:[^\n]*\n'           # 第一行必须有 name:
    r'(?:\s+[a-zA-Z_-]+:[^\n]*\n){2,20}',  # 后续至少2行属性
    re.DOTALL
)

# Surge/Loon 格式 (Proxy = ss, server, port, ...)
SURGE_PROXY_RE = re.compile(
    r'(?i)(?:^|\n)\s*'
    r'([^\s=,]+)\s*=\s*'           # 节点名称
    r'(vmess|vless|trojan|ss|ssr|hysteria2?|hy2|tuic|wireguard|shadowsocks|http|https|socks5)'
    r'\s*,\s*[^\n]{10,500}',       # 剩余参数
    re.MULTILINE
)

# JSON proxies/outbounds 数组
JSON_PROXY_ARRAY_RE = re.compile(
    r'"(?:proxies|outbounds)"\s*:\s*\[([\s\S]{1,100000}?)\]',
    re.IGNORECASE
)

# 单行中可能包含多个 URI（用分隔符隔开）
MULTI_URI_SEP_RE = re.compile(r'[|,;\n]+')


def _is_proxy_uri(line: str) -> bool:
    """快速判断一行文本是否是代理协议 URI。"""
    lower = line.lower()
    return any(lower.startswith(f'{p}://') for p in [
        'vmess', 'vless', 'trojan', 'ss', 'ssr',
        'hysteria', 'hysteria2', 'hy2', 'tuic', 'reality',
        'wireguard', 'sing-box'
    ])


def _split_multi_uri_line(line: str) -> List[str]:
    """将一行中包含多个 URI 的文本拆分成独立的 URI 列表。"""
    parts = MULTI_URI_SEP_RE.split(line)
    result = []
    for part in parts:
        part = part.strip()
        if _is_proxy_uri(part):
            result.append(part)
    return result


def extract_raw_candidates(text: str) -> List[str]:
    """
    从任意文本中提取所有可能的节点候选块。

    覆盖场景：
    - Markdown 代码块中（递归处理）
    - 纯文字中夹杂的 URI（逐行扫描 + 正则提取）
    - Base64 编码的整段订阅（递归解码）
    - Clash YAML 单行/多行代理
    - Surge/Loon 格式 (Proxy = ss, server, port, ...)
    - JSON proxies/outbounds 数组
    - 整份 JSON 配置（Clash / Sing-box）

    返回：去重后的候选字符串列表，不做协议解析。
    """
    # 保护：跳过空文本和超大文件（>2MB）
    if not text or len(text) > 2_000_000:
        return []

    candidates = []
    seen = set()

    # ---- 第一阶段：预处理 ----

    # 1. Markdown 代码块（递归处理内部内容）
    for match in CODE_BLOCK_RE.finditer(text):
        block = match.group(1) or match.group(2)
        if block and block.strip():
            candidates.extend(extract_raw_candidates(block))

    # 2. 大段 Base64（可能是整个订阅，递归解码）
    for b64 in BASE64_RE.findall(text):
        decoded = safe_base64_decode(b64)
        if decoded:
            candidates.extend(extract_raw_candidates(decoded))

    # ---- 第二阶段：按行逐行扫描（处理混合文字场景） ----

    for line in text.split('\n'):
        line = line.strip()
        if not line or len(line) < 10:
            continue

        # 跳过纯注释行
        if line.startswith('#') or line.startswith('//'):
            continue

        # 检测是否包含多个 URI（用 | 、; 等分隔）
        if len(line) > 100 and ('://' in line) and any(sep in line for sep in ['|', ';', ',']):
            uris = _split_multi_uri_line(line)
            for uri in uris:
                if uri not in seen:
                    seen.add(uri)
                    candidates.append(uri)
            continue

        # 标准协议 URI
        if _is_proxy_uri(line):
            if line not in seen:
                seen.add(line)
                candidates.append(line)

    # ---- 第三阶段：结构化格式提取 ----

    # 3.1 Clash YAML 单行代理 (- { ... })
    for m in CLASH_SINGLE_RE.findall(text):
        clean = re.sub(r'\s+', ' ', m.strip())
        if len(clean) > 30 and clean not in seen:
            seen.add(clean)
            candidates.append(clean)

    # 3.2 Clash YAML 多行代理
    for m in CLASH_MULTI_RE.findall(text):
        clean = re.sub(r'\s+', ' ', m.strip())
        if len(clean) > 30 and ('server:' in clean.lower() or 'type:' in clean.lower()):
            if clean not in seen:
                seen.add(clean)
                candidates.append(clean)

    # 3.3 Surge/Loon 格式 (Proxy = ss, server, port, ...)
    for m in SURGE_PROXY_RE.finditer(text):
        full_line = m.group(0).strip()
        if full_line not in seen and len(full_line) > 20:
            seen.add(full_line)
            candidates.append(full_line)

    # 3.4 JSON proxies/outbounds 数组
    for arr in JSON_PROXY_ARRAY_RE.findall(text):
        for obj in re.findall(r'\{[\s\S]{1,3000}?\}', arr):
            try:
                proxy_dict = json.loads(obj)
                candidates.append(json.dumps(proxy_dict, ensure_ascii=False))
            except Exception:
                clean_obj = re.sub(r'\s+', ' ', obj.strip())
                if any(k in clean_obj.lower() for k in ['server', 'port', 'type', 'uuid']):
                    if clean_obj not in seen:
                        seen.add(clean_obj)
                        candidates.append(clean_obj)

    # 3.5 整份文件作为 JSON 配置（Clash / Sing-box）
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

    # ---- 第四阶段：补充扫描（处理 URI 出现在非标准行中的情况） ----

    for uri_match in URI_RE.findall(text):
        if uri_match not in seen:
            seen.add(uri_match)
            candidates.append(uri_match)

    for ss_match in SS_URI_RE.findall(text):
        if ss_match not in seen:
            seen.add(ss_match)
            candidates.append(ss_match)

    for ssr_match in SSR_URI_RE.findall(text):
        if ssr_match not in seen:
            seen.add(ssr_match)
            candidates.append(ssr_match)

    for hy2_match in HY2_URI_RE.findall(text):
        if hy2_match not in seen:
            seen.add(hy2_match)
            candidates.append(hy2_match)

    for hy2s in HY2_SHORT_RE.findall(text):
        if hy2s not in seen:
            seen.add(hy2s)
            candidates.append(hy2s)

    for tuic_match in TUIC_URI_RE.findall(text):
        if tuic_match not in seen:
            seen.add(tuic_match)
            candidates.append(tuic_match)

    # ---- 最终过滤 ----

    final = []
    for c in candidates:
        c = c.strip()
        if not c or len(c) < 15:
            continue
        # 过滤掉明显不是节点的文本（如纯 URL、纯数字等）
        if c.startswith('http://') or c.startswith('https://'):
            if 'raw.githubusercontent.com' in c:
                # 保留 raw 链接（可能是订阅源）
                pass
            elif '://' in c and c.count('/') <= 3:
                # 可能是没有协议前缀的代理链接
                pass
            else:
                continue
        if c in seen:
            continue
        seen.add(c)
        final.append(c)

    return final
