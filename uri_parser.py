"""
URI 协议解析层 — 将原始代理链接解析为 StandardProxy 对象。

支持的协议：
  - vmess://   (Base64 JSON → StandardProxy)
  - trojan://  (URL-parsed → StandardProxy)
  - vless://   (URL-parsed → StandardProxy)
  - ss://      (Base64 + URL-parsed → StandardProxy)
  - ssr://     (Base64 复合编码 → StandardProxy)
  - hysteria2:// / hy2://  (URL-parsed → StandardProxy)
  - tuic://    (URL-parsed → StandardProxy)

设计原则：
  1. 每个协议一个独立函数，协议自身的语法就是边界检测器。
  2. 解析失败返回 None（不抛异常），由调用者丢弃即可。
  3. 不依赖网络请求，纯本地 CPU 操作。
  4. 每个函数可独立测试、独立优化。

使用：
  from uri_parser import validate_candidate
  proxy = validate_candidate("vmess://eyJhZGQiOiIxLjIuMy40Iiw...")
  if proxy:
      print(proxy.server, proxy.port)
"""

import re
import json
import base64
from typing import Optional, List
from urllib.parse import urlparse, parse_qs, unquote

from models import StandardProxy


# ==================== URI 发现 ====================

# 支持的协议 scheme 列表（小写）
_SUPPORTED_SCHEMES = [
    "vmess", "vless", "trojan", "ss", "ssr",
    "hysteria", "hysteria2", "hy2", "tuic",
]

# 发现 URI：匹配 scheme:// 到下一个空白字符或 scheme:// 之前
# 使用前瞻切分，不依赖分隔符
_URI_PATTERN = (
    r'(?:' + '|'.join(_SUPPORTED_SCHEMES) + r')://'
    r'[^\s"\'`<>]*'
)
URI_RE = re.compile(_URI_PATTERN, re.IGNORECASE)

# 按 scheme 切分粘在一起的 URI
_SPLIT_RE = re.compile(
    r'(?=' + '|'.join(f'{s}://' for s in _SUPPORTED_SCHEMES) + r')',
    re.IGNORECASE
)

# Base64 块（用于递归解码发现）
BASE64_BLOCK_RE = re.compile(r'[A-Za-z0-9+/=]{100,200000}')

# 代码块
CODE_BLOCK_RE = re.compile(
    r'(?:```(?:[\w]*)\n?)([\s\S]{1,1000000}?)(?:\n?```)'
    r'|`([^`\n]{1,10000})`'
)


def discover_candidates(text: str, max_depth: int = 3) -> List[str]:
    """暴力发现文本中所有可能的代理 URI 候选。

    递归策略：
      1. 直接扫描 URI scheme
      2. 找到 Base64 块 → 解码 → 递归
      3. 找到代码块 → 递归

    不负责验证，只管"发现"。验证交给 validate_candidate()。

    Args:
        text: 任意文本内容
        max_depth: 最大递归深度（默认 3，防止无限递归）

    Returns:
        候选 URI 字符串列表（可能包含噪音，需后续验证）
    """
    if max_depth <= 0 or not text:
        return []

    candidates = []
    seen = set()

    # 1. URI scheme 直接扫描
    for m in URI_RE.finditer(text):
        uri = m.group(0).rstrip(',;|')
        uri = _trim_trailing_non_uri(uri)
        if uri not in seen and len(uri) > 15:
            seen.add(uri)
            candidates.append(uri)

    # 2. 按 scheme 切分粘连的 URI
    if len(candidates) == 0:
        parts = _SPLIT_RE.split(text)
        if len(parts) > 1:
            for part in parts[1:]:
                uri = _SPLIT_RE.pattern.split(part)[0] if _SPLIT_RE.search(part) else part
                uri = uri.strip().rstrip(',;|\n')
                uri = _trim_trailing_non_uri(uri)
                if uri not in seen and len(uri) > 15 and '://' in uri:
                    scheme = uri.split('://')[0].lower()
                    if scheme in _SUPPORTED_SCHEMES:
                        seen.add(uri)
                        candidates.append(uri)

    # 3. Base64 块 → 解码 → 递归发现
    for m in BASE64_BLOCK_RE.finditer(text):
        decoded = _safe_b64_decode(m.group(0))
        if decoded:
            sub = discover_candidates(decoded, max_depth - 1)
            for uri in sub:
                if uri not in seen:
                    seen.add(uri)
                    candidates.append(uri)

    # 4. 代码块 → 递归发现
    for m in CODE_BLOCK_RE.finditer(text):
        block = m.group(1) or m.group(2)
        if block:
            sub = discover_candidates(block.strip(), max_depth - 1)
            for uri in sub:
                if uri not in seen:
                    seen.add(uri)
                    candidates.append(uri)

    return candidates


def _trim_trailing_non_uri(uri: str) -> str:
    """去掉 URI 末尾的非 URI 合法字符。

    协议 URI 允许的字符集较宽，但遇到明显的"分隔符号 + 非协议内容"时可截断。
    核心策略：base64 字符集 + URL-safe 字符就是边界。
    """
    # 如果以一个已知的 scheme 开头（粘在一起的情况），在第二个 scheme 前截断
    for scheme in _SUPPORTED_SCHEMES:
        # 查找 scheme:// 在 URI 中间出现的位置
        scheme_prefix = scheme + "://"
        idx = uri.find(scheme_prefix, len(scheme_prefix))
        if idx > 0:
            return uri[:idx].rstrip()
    return uri


def _safe_b64_decode(s: str) -> Optional[str]:
    """安全 Base64 解码。"""
    try:
        s = s.strip().replace('-', '+').replace('_', '/')
        if not s:
            return None
        padding = len(s) % 4
        if padding:
            s += '=' * (4 - padding)
        return base64.b64decode(s, validate=False).decode('utf-8', errors='replace')
    except Exception:
        return None


# ==================== 协议解析器注册表 ====================

PARSER_REGISTRY = {}


def register(schemes: List[str]):
    """装饰器：将解析函数注册到 PARSER_REGISTRY。"""
    def decorator(func):
        for scheme in schemes:
            PARSER_REGISTRY[scheme] = func
        return func
    return decorator


# ==================== vmess ====================

@register(["vmess"])
def parse_vmess(uri: str) -> Optional[StandardProxy]:
    """解析 vmess://base64(JSON) 格式。

    格式: vmess://eyJ2IjoiMiIsInBzIjoi...base64...

    JSON 字段：
      add  — 服务器地址
      port — 端口
      id   — UUID
      aid  — alterId（可选，默认 0）
      ps   — 备注
      scy  — 加密方式（可选，默认 "auto"）
      net  — 传输协议（可选，默认 "tcp"）
      type — 伪装类型（ws 时为 "none"）
      host — 伪装域名
      path — WS 路径
      tls  — TLS（可选，默认 ""）
      sni  — SNI（可选）
      alpn — ALPN（可选）
      fp   — 指纹（可选）
    """
    try:
        payload = uri.removeprefix("vmess://").strip()

        # 提取 Base64 部分：只取合法 Base64 字符
        b64_match = re.match(r'[A-Za-z0-9+/=_-]+', payload)
        if not b64_match:
            return None
        b64 = b64_match.group(0)

        # 标准化为 Base64 标准字符集
        b64 = b64.replace('-', '+').replace('_', '/')
        if len(b64) % 4:
            b64 += '=' * (4 - len(b64) % 4)

        decoded = base64.b64decode(b64, validate=False)
        obj = json.loads(decoded)

        # 核心字段验证
        server = obj.get("add", "")
        port = int(obj.get("port", 0))
        uuid = obj.get("id", "")

        if not server or port <= 0 or port > 65535 or not uuid:
            return None

        return StandardProxy(
            protocol="vmess",
            server=server,
            port=port,
            uuid=uuid,
            security=obj.get("scy", "auto"),
            transport=obj.get("net", "tcp"),
            tls=(obj.get("tls") == "tls"),
            sni=obj.get("sni", ""),
            allow_insecure=False,
            remark=obj.get("ps", ""),
            raw_link=uri,
            extra={
                k: v for k, v in obj.items()
                if k not in ("add", "port", "id", "ps", "scy", "net", "tls", "sni", "v")
            }
        )
    except Exception:
        return None


# ==================== trojan ====================

@register(["trojan"])
def parse_trojan(uri: str) -> Optional[StandardProxy]:
    """解析 trojan://password@host:port?params#remark 格式。

    标准 URL 结构：trojan://password@server:port?key=value#remark
    常见参数：sni, type (ws/grpc), host, path, security (tls), alpn, fp
    """
    try:
        # URL parse 需要标准的 scheme，trojan 是合法 scheme
        parsed = urlparse(uri)

        password = unquote(parsed.username or "")
        server = parsed.hostname or ""
        port = parsed.port or 443
        params = parse_qs(parsed.query)

        if not server or not password:
            return None

        # 查询参数（每个可能是列表，取第一个值）
        def _param(key: str, default: str = ""):
            vals = params.get(key, [])
            return vals[0] if vals else default

        sni = _param("sni", server)
        transport = _param("type", "tcp")
        tls_enabled = _param("security", "") == "tls" or sni != server or True
        allow_insecure = _param("allowInsecure", "0") == "1"

        proxy = StandardProxy(
            protocol="trojan",
            server=server,
            port=port,
            uuid=password,
            transport=transport,
            tls=tls_enabled,
            sni=sni,
            allow_insecure=allow_insecure,
            remark=unquote(parsed.fragment or ""),
            raw_link=uri,
            extra={}
        )

        # 收集其他参数
        other_keys = {"sni", "type", "security", "allowInsecure", "host", "path", "alpn", "fp"}
        proxy.extra = {k: v[0] if isinstance(v, list) else v
                       for k, v in params.items() if k not in other_keys}

        return proxy
    except Exception:
        return None


# ==================== vless ====================

@register(["vless"])
def parse_vless(uri: str) -> Optional[StandardProxy]:
    """解析 vless://uuid@host:port?params#remark 格式。

    格式: vless://uuid@server:port?encryption=none&security=tls&sni=xxx&type=ws&...&fp=xxx#remark

    VLESS 特有参数：
      encryption — 通常为 "none"（VLESS 无内部加密）
      flow       — XTLS 流控（xtls-rprx-vision 等）
      security   — 传输层安全（tls / reality）
      pbk        — Reality 公钥
      sid        — Reality shortId
    """
    try:
        parsed = urlparse(uri)

        uuid = unquote(parsed.username or "")
        server = parsed.hostname or ""
        port = parsed.port or 443
        params = parse_qs(parsed.query)

        if not server or not uuid:
            return None

        def _param(key: str, default: str = ""):
            vals = params.get(key, [])
            return vals[0] if vals else default

        sni = _param("sni", server)
        transport = _param("type", "tcp")
        security = _param("security", "none")
        tls_enabled = security in ("tls", "reality")
        allow_insecure = _param("allowInsecure", "0") == "1"

        proxy = StandardProxy(
            protocol="vless",
            server=server,
            port=port,
            uuid=uuid,
            transport=transport,
            tls=tls_enabled,
            sni=sni,
            allow_insecure=allow_insecure,
            remark=unquote(parsed.fragment or ""),
            raw_link=uri,
        )

        # VLESS 特有参数
        flow = _param("flow", "")
        if flow:
            proxy.extra["flow"] = flow
        if security == "reality":
            proxy.extra["security"] = "reality"
            proxy.extra["pbk"] = _param("pbk", "")
            proxy.extra["sid"] = _param("sid", "")

        return proxy
    except Exception:
        return None


# ==================== ss (Shadowsocks) ====================

@register(["ss"])
def parse_ss(uri: str) -> Optional[StandardProxy]:
    """解析 ss:// 格式。

    Shadowsocks 有两种常见格式：
      1. ss://base64(method:password)@server:port#remark
      2. ss://base64(method:password@server:port)#remark
      3. ss://base64(server:port:method:password)  (旧版 SIP002)
    新版 (SIP022) 支持查询参数：plugin, plugin-opts 等。
    """
    try:
        payload = uri.removeprefix("ss://").strip()

        # 分离 fragment
        fragment = ""
        if "#" in payload:
            idx = payload.rindex("#")
            fragment = unquote(payload[idx + 1:])
            payload = payload[:idx]

        # 分离查询参数
        params = {}
        if "?" in payload:
            idx = payload.index("?")
            params = parse_qs(payload[idx + 1:])
            payload = payload[:idx]

        # 解码 Base64 部分（可能包含 @ 也可能不包含）
        if "@" in payload:
            # 格式: base64(method:password)@server:port
            b64_part, server_part = payload.split("@", 1)
            b64_decoded = _safe_b64_decode(b64_part)
            if not b64_decoded:
                return None
            if ":" in b64_decoded:
                method, password = b64_decoded.split(":", 1)
            else:
                method, password = "aes-256-gcm", b64_decoded

            if ":" in server_part:
                server, port_str = server_part.rsplit(":", 1)
                port = int(port_str)
            else:
                server = server_part
                port = 8388  # SS 默认端口
        else:
            # 整个 payload 是 Base64（旧版格式）
            decoded = _safe_b64_decode(payload)
            if not decoded:
                return None
            # 可能是 method:password@server:port
            if "@" in decoded:
                creds, server_part = decoded.split("@", 1)
                if ":" in creds:
                    method, password = creds.split(":", 1)
                else:
                    method, password = "aes-256-gcm", creds

                if ":" in server_part:
                    server, port_str = server_part.rsplit(":", 1)
                    port = int(port_str)
                else:
                    server = server_part
                    port = 8388
            elif ":" in decoded:
                # 可能是 server:port:method:password
                parts = decoded.split(":")
                if len(parts) >= 4:
                    server = parts[0]
                    port = int(parts[1])
                    method = parts[2]
                    password = ":".join(parts[3:])
                else:
                    return None
            else:
                return None

        if not server or not password:
            return None

        return StandardProxy(
            protocol="ss",
            server=server,
            port=port,
            uuid=password,
            security=method,
            transport="tcp",
            remark=fragment,
            raw_link=uri,
        )
    except Exception:
        return None


# ==================== ssr (ShadowsocksR) ====================

@register(["ssr"])
def parse_ssr(uri: str) -> Optional[StandardProxy]:
    """解析 ssr:// 格式。

    格式: ssr://base64(server:port:protocol:method:obfs:base64_password/?obfsparam=...)

    SSR 是 Base64 编码的复合格式，结构复杂：
      - 前半部分（/ 之前）：server:port:protocol:method:obfs:password_base64
      - 后半部分（? 之后）：参数列表（本身也是 key=value 对）

    注意：password 是 Base64 编码的！
    """
    try:
        payload = uri.removeprefix("ssr://").strip()

        # 提取 Base64 主体
        b64_match = re.match(r'[A-Za-z0-9+/=_-]+', payload)
        if not b64_match:
            return None

        decoded = _safe_b64_decode(b64_match.group(0))
        if not decoded:
            return None

        # 分离主体和参数部分
        if "?" in decoded:
            body, query_str = decoded.split("?", 1)
        elif "/?" in decoded:
            body, query_str = decoded.split("/?", 1)
        else:
            body = decoded
            query_str = ""

        # 主体格式: server:port:protocol:method:obfs:base64_password
        parts = body.split(":")
        if len(parts) < 6:
            return None

        server = parts[0]
        port = int(parts[1])
        protocol = parts[2]      # origin / auth_chain_a 等
        method = parts[3]         # aes-256-cfb 等
        obfs = parts[4]           # plain / http_simple / tls1.2_ticket_auth
        password_b64 = parts[5]
        password = _safe_b64_decode(password_b64) or password_b64

        # 解析查询参数
        params = parse_qs(query_str)
        def _param(key: str, default: str = ""):
            vals = params.get(key, [])
            val = vals[0] if vals else default
            # SSR 参数值可能也是 Base64 编码
            if key in ("obfsparam", "protoparam", "remarks", "group"):
                return _safe_b64_decode(val) or val
            return val

        remark = _param("remarks", "")

        return StandardProxy(
            protocol="ssr",
            server=server,
            port=port,
            uuid=password,
            security=method,
            transport="tcp",
            remark=remark,
            raw_link=uri,
            extra={
                "ssr_protocol": protocol,
                "ssr_obfs": obfs,
                "obfsparam": _param("obfsparam", ""),
                "protoparam": _param("protoparam", ""),
            }
        )
    except Exception:
        return None


# ==================== hysteria2 / hy2 ====================

@register(["hysteria2", "hy2", "hysteria"])
def parse_hysteria2(uri: str) -> Optional[StandardProxy]:
    """解析 hysteria2:// 或 hy2:// 格式。

    格式: hysteria2://password@host:port?sni=xxx&insecure=0&obfs=xxx&obfs-password=xxx#remark
    简写: hy2://password@host:port?...

    常见参数：
      sni      — SNI 主机名
      insecure — 是否跳过证书验证（0/1）
      obfs     — 混淆类型（salamander）
      obfs-password — 混淆密码
      pinSHA256 — 证书 SHA256 指纹
    """
    try:
        parsed = urlparse(uri)

        password = unquote(parsed.username or "")
        server = parsed.hostname or ""
        port = parsed.port or 443
        params = parse_qs(parsed.query)

        if not server or not password:
            return None

        def _param(key: str, default: str = ""):
            vals = params.get(key, [])
            return vals[0] if vals else default

        sni = _param("sni", server)
        allow_insecure = _param("insecure", "0") == "1"

        proxy = StandardProxy(
            protocol="hysteria2",
            server=server,
            port=port,
            uuid=password,
            transport="quic",
            tls=True,  # hysteria2 始终使用 TLS
            sni=sni,
            allow_insecure=allow_insecure,
            remark=unquote(parsed.fragment or ""),
            raw_link=uri,
        )

        # 混淆参数
        obfs = _param("obfs", "")
        if obfs:
            proxy.extra["obfs"] = obfs
            proxy.extra["obfs_password"] = _param("obfs-password", "")
        pinsha = _param("pinSHA256", "")
        if pinsha:
            proxy.extra["pinSHA256"] = pinsha

        return proxy
    except Exception:
        return None


# ==================== tuic ====================

@register(["tuic"])
def parse_tuic(uri: str) -> Optional[StandardProxy]:
    """解析 tuic:// 格式。

    格式: tuic://uuid:password@host:port?sni=xxx&congestion_control=bbr&alpn=h3&...&allow_insecure=0#remark

    常见参数：
      sni                 — SNI
      congestion_control  — 拥塞控制（bbr / cubic / new_reno）
      alpn                — ALPN（h3 / h2 / http/1.1）
      allow_insecure      — 跳过证书验证（0/1）
      disable_sni         — 禁用 SNI（0/1）
      udp_relay_mode      — UDP 中继模式（native / quic）
    """
    try:
        parsed = urlparse(uri)

        userinfo = unquote(parsed.username or "")
        server = parsed.hostname or ""
        port = parsed.port or 443
        params = parse_qs(parsed.query)

        if not server:
            return None

        # tuic 的 userinfo 格式: uuid:password
        uuid = ""
        password = ""
        if userinfo and ":" in userinfo:
            uuid, password = userinfo.split(":", 1)
        elif userinfo:
            uuid = userinfo
            password = ""

        def _param(key: str, default: str = ""):
            vals = params.get(key, [])
            return vals[0] if vals else default

        sni = _param("sni", server)
        allow_insecure = _param("allow_insecure", "0") == "1"
        alpn = _param("alpn", "h3")
        cc = _param("congestion_control", "bbr")

        proxy = StandardProxy(
            protocol="tuic",
            server=server,
            port=port,
            uuid=uuid,
            security=password,  # tuic 密码
            transport="quic",
            tls=True,
            sni=sni,
            allow_insecure=allow_insecure,
            remark=unquote(parsed.fragment or ""),
            raw_link=uri,
            extra={
                "alpn": alpn,
                "congestion_control": cc,
            }
        )

        udp_mode = _param("udp_relay_mode", "")
        if udp_mode:
            proxy.extra["udp_relay_mode"] = udp_mode

        return proxy
    except Exception:
        return None


# ==================== 验证与提取入口 ====================

def validate_candidate(candidate: str) -> Optional[StandardProxy]:
    """尝试验证并解析一个候选 URI。

    根据 URI 的 scheme 选择对应的解析器。解析成功返回 StandardProxy，
    解析失败返回 None（调用者丢弃即可）。

    Args:
        candidate: 原始 URI 字符串（如 "vmess://eyJh...", "trojan://pass@host:443"）

    Returns:
        StandardProxy 实例，无法解析时返回 None
    """
    candidate = candidate.strip()
    if not candidate or "://" not in candidate:
        return None

    scheme = candidate.split("://", 1)[0].lower()
    parser = PARSER_REGISTRY.get(scheme)
    if not parser:
        return None

    return parser(candidate)


def extract_proxies_from_text(text: str, max_depth: int = 3) -> List[StandardProxy]:
    """从任意文本中提取并验证所有代理节点。

    管道：
      discover_candidates() → 验证 schema → 协议解析 → 去重 → 返回

    注意：此函数的去重范围仅限于当前文本。
    全局去重应在调用者处基于 StandardProxy.dedup_key() 进行。

    Args:
        text: 任意文本内容（文件内容、配置等）
        max_depth: 递归解码最大深度（默认 3）

    Returns:
        解析成功的 StandardProxy 列表（已去重当前文本内的重复项）
    """
    candidates = discover_candidates(text, max_depth)
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
