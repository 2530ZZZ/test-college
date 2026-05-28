"""
节点候选提取模块 —— 只负责从任意文本中提取“看起来像节点”的候选块。
所有正则长度上限已放宽，避免遗漏长 URI 或大文件。
"""

import re
import json
from typing import List
from utils import safe_base64_decode

URI_SCHEMES = r'(?:vmess|vless|trojan|ss|ssr|hysteria|hysteria2|hy2|tuic|reality|wireguard|sing-box)'
URI_RE = re.compile(rf'(?i){URI_SCHEMES}://\S{{1,4000}}')          # 放宽至 4000 字符
SS_URI_RE = re.compile(r'ss://[A-Za-z0-9+/=]{1,500}(?:\?[^\s<>"\'`]{1,2000})?(?:#[^\s<>"\'`]{1,1000})?')
SSR_URI_RE = re.compile(r'ssr://[A-Za-z0-9+/=]{1,4000}')
HY2_URI_RE = re.compile(r'(?i)hysteria2://\S{1,2000}')
HY2_SHORT_RE = re.compile(r'(?i)hy2://\S{1,2000}')
TUIC_URI_RE = re.compile(r'(?i)tuic://\S{1,2000}')
BASE64_RE = re.compile(r'[A-Za-z0-9+/=]{100,200000}')               # 200KB 内的 Base64 块
CODE_BLOCK_RE = re.compile(r'(?:```(?:[\w]*)\n?)([\s\S]{1,1000000}?)(?:\n?```)|`([^`\n]{1,10000})`')
CLASH_SINGLE_RE = re.compile(r'-\s*\{[^}]{1,15000}?(?:name|server|port|type|uuid|password|ps|flow|sni|fp|reality-opts)[^}]{0,15000}\}', re.DOTALL)
CLASH_MULTI_RE = re.compile(r'-\s*name:[^\n]*\n(?:\s+[a-zA-Z_-]+:[^\n]*\n){2,100}', re.DOTALL)
CLASH_INLINE_RE = re.compile(r'- name:\s*\S[^\n]{0,10000}', re.DOTALL)
SURGE_PROXY_RE = re.compile(r'(?i)(?:^|\n)\s*([^\s=,]+)\s*=\s*(vmess|vless|trojan|ss|ssr|hysteria2?|hy2|tuic|wireguard|shadowsocks|http|https|socks5)\s*,\s*[^\n]{10,4000}', re.MULTILINE)
JSON_PROXY_ARRAY_RE = re.compile(r'"(?:proxies|outbounds)"\s*:\s*\[([\s\S]{1,2000000}?)\]', re.IGNORECASE)
CONCAT_URI_SPLIT_RE = re.compile(rf'(?={URI_SCHEMES}://)')
JSON_FALLBACK_RE = re.compile(r'\{[^{}]*?"server"\s*:\s*"[^"]*"[^{}]*?"port"\s*:\s*\d+[^{}]*?\}', re.IGNORECASE | re.DOTALL)

def _is_proxy_uri(line: str) -> bool:
    lower = line.lower()
    return any(lower.startswith(f'{p}://') for p in ['vmess','vless','trojan','ss','ssr','hysteria','hysteria2','hy2','tuic','reality','wireguard','sing-box'])

def _split_concatenated_uris(text: str) -> List[str]:
    parts = CONCAT_URI_SPLIT_RE.split(text)
    uris = []
    for part in parts:
        part = part.strip()
        if part and _is_proxy_uri(part):
            uris.append(part)
    return uris

def extract_raw_candidates(text: str) -> List[str]:
    if not text or len(text) > 10_000_000:   # 10MB 硬上限
        return []
    candidates = []
    seen = set()
    for match in CODE_BLOCK_RE.finditer(text):
        block = match.group(1) or match.group(2)
        if block and block.strip():
            candidates.extend(extract_raw_candidates(block))
    for b64 in BASE64_RE.findall(text):
        decoded = safe_base64_decode(b64)
        if decoded:
            candidates.extend(extract_raw_candidates(decoded))
    for line in text.split('\n'):
        line = line.strip()
        if not line or len(line) < 10: continue
        if line.startswith('#') or line.startswith('//'): continue
        if '://' in line:
            uris = _split_concatenated_uris(line)
            if uris:
                for uri in uris:
                    if uri not in seen:
                        seen.add(uri)
                        candidates.append(uri)
                continue
            if len(line) > 100 and any(sep in line for sep in ['|',';',',']):
                parts = re.split(r'[|,;\s]+', line)
                for part in parts:
                    part = part.strip()
                    if _is_proxy_uri(part):
                        if part not in seen:
                            seen.add(part)
                            candidates.append(part)
                continue
        if len(line) > 100 and not _is_proxy_uri(line):
            decoded = safe_base64_decode(line)
            if decoded and '://' in decoded:
                candidates.extend(extract_raw_candidates(decoded))
                continue
    yaml_text = re.sub(r'&\w+\s+', '', text)
    yaml_text = re.sub(r'\*\w+', '', yaml_text)
    for m in CLASH_SINGLE_RE.findall(yaml_text):
        clean = re.sub(r'\s+', ' ', m.strip())
        if len(clean) > 30 and clean not in seen:
            seen.add(clean)
            candidates.append(clean)
    for m in CLASH_MULTI_RE.findall(yaml_text):
        clean = re.sub(r'\s+', ' ', m.strip())
        if len(clean) > 30 and ('server:' in clean.lower() or 'type:' in clean.lower()):
            if clean not in seen:
                seen.add(clean)
                candidates.append(clean)
    for m in CLASH_INLINE_RE.findall(yaml_text):
        clean = re.sub(r'\s+', ' ', m.strip())
        if len(clean) > 30 and ('name:' in clean.lower() and ('server:' in clean.lower() or 'type:' in clean.lower())):
            if clean not in seen:
                seen.add(clean)
                candidates.append(clean)
    for m in SURGE_PROXY_RE.finditer(text):
        full_line = m.group(0).strip()
        if full_line not in seen and len(full_line) > 20:
            seen.add(full_line)
            candidates.append(full_line)
    for arr in JSON_PROXY_ARRAY_RE.findall(text):
        for obj in re.findall(r'\{[\s\S]{1,5000}?\}', arr):
            try:
                proxy_dict = json.loads(obj)
                candidates.append(json.dumps(proxy_dict, ensure_ascii=False))
            except Exception:
                clean_obj = re.sub(r'\s+', ' ', obj.strip())
                if any(k in clean_obj.lower() for k in ['server','port','type','uuid']):
                    if clean_obj not in seen:
                        seen.add(clean_obj)
                        candidates.append(clean_obj)
    try:
        config = json.loads(text)
        if isinstance(config, dict):
            for key in ('proxies','outbounds'):
                if key in config and isinstance(config[key], list):
                    for item in config[key]:
                        if isinstance(item, dict):
                            candidates.append(json.dumps(item, ensure_ascii=False))
                        elif isinstance(item, str):
                            candidates.append(item)
    except Exception:
        for match in JSON_FALLBACK_RE.finditer(text):
            obj_str = match.group().strip()
            if obj_str not in seen:
                seen.add(obj_str)
                candidates.append(obj_str)
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
    final = []
    for c in candidates:
        c = c.strip()
        if not c or len(c) < 15: continue
        if c.startswith('http://') or c.startswith('https://'):
            if 'raw.githubusercontent.com' not in c: continue
        if c in seen: continue
        seen.add(c)
        final.append(c)
    return final
