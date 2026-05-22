"""
节点候选提取模块 —— 只负责从任意文本中提取"看起来像节点"的候选块。
所有正则均已限制匹配长度，避免灾难性回溯。
"""

import re
import json
from typing import List
from utils import safe_base64_decode

# 预编译正则（长度已限制）
URI_RE = re.compile(
    r'(?i)(vmess|vless|trojan|ss|ssr|hysteria2?|hy2|tuic|reality|wireguard|sing-box)://\S{1,500}'
)
SS_URI_RE = re.compile(
    r'ss://[A-Za-z0-9+/=]{1,500}(?:\?[^\s<>"\'`]{1,200})?(?:#[^\s<>"\'`]{1,200})?'
)
BASE64_RE = re.compile(r'[A-Za-z0-9+/=]{100,5000}')   # 最大 5000 字符
CODE_BLOCK_RE = re.compile(r'(?:```(?:[\w]*)\n?)([\s\S]*?)(?:\n?```)|`([^`\n]+)`')
CLASH_SINGLE_RE = re.compile(
    r'-\s*\{[^}]{1,2000}?(?:name|server|port|type|uuid|password|ps|flow)[^}]{0,2000}\}', re.DOTALL
)
CLASH_MULTI_RE = re.compile(r'-\s*name:[^\n]*(?:\n[^\n]+){0,20}', re.DOTALL)
JSON_PROXY_ARRAY_RE = re.compile(r'"(?:proxies|outbounds)"\s*:\s*\[([\s\S]{1,50000}?)\]', re.IGNORECASE)


def extract_raw_candidates(text: str) -> List[str]:
    """
    从任意文本中提取所有可能的节点候选块。
    递归处理 Markdown 代码块和 Base64 编码的内容。
    """
    # 保护：跳过超大文本（订阅文件不可能超过 1MB）
    if len(text) > 1_000_000:
        return []

    candidates = []

    # 1. Markdown 代码块（递归）
    for match in CODE_BLOCK_RE.finditer(text):
        block = match.group(1) or match.group(2)
        if block and block.strip():
            candidates.extend(extract_raw_candidates(block))

    # 2. 大段 Base64（递归）
    for b64 in BASE64_RE.findall(text):
        decoded = safe_base64_decode(b64)
        if decoded:
            candidates.extend(extract_raw_candidates(decoded))

    # 3. 协议 URI
    for uri in URI_RE.findall(text):
        candidates.append(uri)
    for ss in SS_URI_RE.findall(text):
        candidates.append(ss)

    # 4. Clash YAML 单行/多行
    for m in CLASH_SINGLE_RE.findall(text):
        clean = re.sub(r'\s+', ' ', m.strip())
        if len(clean) > 40:
            candidates.append(clean)
    for m in CLASH_MULTI_RE.findall(text):
        clean = re.sub(r'\s+', ' ', m.strip())
        if len(clean) > 30 and ('server:' in clean or 'type:' in clean):
            candidates.append(clean)

    # 5. JSON proxies/outbounds 数组
    for arr in JSON_PROXY_ARRAY_RE.findall(text):
        for obj in re.findall(r'\{[\s\S]{1,2000}?\}', arr):
            try:
                proxy_dict = json.loads(obj)
                candidates.append(json.dumps(proxy_dict, ensure_ascii=False))
            except Exception:
                clean_obj = re.sub(r'\s+', ' ', obj.strip())
                if any(k in clean_obj.lower() for k in ['server', 'port', 'type', 'uuid']):
                    candidates.append(clean_obj)

    # 6. 整个文件作为 JSON（Clash/Sing-box 配置）
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
    clean = []
    for c in candidates:
        c = c.strip()
        if not c or len(c) < 15 or c in seen:
            continue
        seen.add(c)
        clean.append(c)
    return clean
