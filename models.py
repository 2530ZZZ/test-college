"""
统一节点数据模型与去重逻辑。

所有 URI 解析器将原始链接转换为 StandardProxy 对象，
后续的测速、过滤、输出等模块都基于 StandardProxy 工作。

去重 key 生成逻辑集中在此模块，确保全局一致。
"""

from dataclasses import dataclass, field
from typing import Optional, Tuple
import json


@dataclass
class StandardProxy:
    """标准化的代理节点数据结构。

    字段说明按协议类型可能有不同映射：
    - vmess/vless/trojan: uuid 映射到 id/password
    - ss/ssr: uuid 映射到 password，security 映射到 method
    - hysteria2/tuic: uuid 映射到 password/auth

    Attributes:
        protocol: 协议类型 (vmess/vless/trojan/ss/ssr/hysteria2/tuic/hy2)
        server: 服务器地址（IP 或域名）
        port: 端口号（0 表示未知）
        uuid: 用户 ID / UUID / 密码
        security: 加密方式，vmess 默认为 "auto"，ss 为加密方法
        transport: 传输协议（tcp/ws/grpc/h2/quic）
        tls: 是否启用 TLS
        sni: TLS SNI 主机名，空字符串表示使用 server 作为 SNI
        allow_insecure: 是否跳过 TLS 证书验证
        remark: 节点备注/名称
        raw_link: 原始分享链接 (vmess://... / trojan://...)
        source_url: 来源文件的 raw URL
        extra: 其他非标准字段
    """

    # 核心连接字段
    protocol: str = ""
    server: str = ""
    port: int = 0
    uuid: str = ""

    # TLS / 传输
    security: str = ""
    transport: str = "tcp"
    tls: bool = False
    sni: str = ""
    allow_insecure: bool = False

    # 元数据
    remark: str = ""
    raw_link: str = ""
    source_url: str = ""

    # 扩展字段（协议特有参数如 flow, reality-opts, alpn 等）
    extra: dict = field(default_factory=dict)

    def dedup_key(self, strategy: str = "server_port_protocol") -> Tuple:
        """生成去重 key。

        Args:
            strategy: 去重策略
                "server_port"          — (server, port)
                "server_port_protocol" — (server, port, protocol)（推荐）

        Returns:
            去重元组，用于存入 set 做唯一性检查。

        Raises:
            ValueError: 未知的去重策略
        """
        if strategy == "server_port":
            return (self.server, self.port)
        elif strategy == "server_port_protocol":
            return (self.server, self.port, self.protocol)
        else:
            raise ValueError(f"未知的去重策略: {strategy}")

    def identity_key(self) -> Tuple:
        """生成完全一致性的标识 key。

        server + port + protocol + uuid → 用于判断两个节点是否完全相同。
        """
        return (self.server, self.port, self.protocol, self.uuid)

    def is_valid(self) -> bool:
        """检查节点最基础的字段是否完整。

        Returns:
            True 如果 server 非空且 port 在合法范围内 (1-65535)。
        """
        return bool(self.server) and 1 <= self.port <= 65535

    def to_dict(self) -> dict:
        """转为字典，用于 JSON 序列化。"""
        return {
            "protocol": self.protocol,
            "server": self.server,
            "port": self.port,
            "uuid": self.uuid,
            "security": self.security,
            "transport": self.transport,
            "tls": self.tls,
            "sni": self.sni,
            "allow_insecure": self.allow_insecure,
            "remark": self.remark,
            "raw_link": self.raw_link,
            "source_url": self.source_url,
            "extra": self.extra,
        }

    def to_uri(self) -> str:
        """还原为标准 URI 链接，subs-check / v2rayN 等工具可直接识别。

        优先返回 raw_link（原始格式），否则按协议构建标准 URI。
        """
        import base64
        from urllib.parse import quote
        if self.raw_link:
            return self.raw_link

        p = self.protocol.lower()
        host = f"{self.server}:{self.port}"
        userinfo = quote(self.uuid, safe='')
        remark = quote(self.remark, safe='') if self.remark else ""
        fragment = f"#{remark}" if remark else ""

        if p == "vmess":
            # vmess://base64(json)
            obj = {"v": "2", "ps": self.remark, "add": self.server,
                   "port": self.port, "id": self.uuid, "aid": 0,
                   "scy": self.security or "auto", "net": self.transport,
                   "tls": "tls" if self.tls else ""}
            if self.sni:
                obj["sni"] = self.sni
            payload = base64.b64encode(json.dumps(obj).encode()).decode()
            return f"vmess://{payload}"

        if p == "ss":
            # ss://base64(method:password)@server:port#remark
            creds = base64.b64encode(
                f"{self.security or 'aes-256-gcm'}:{self.uuid}".encode()
            ).decode().rstrip("=")
            return f"ss://{creds}@{host}{fragment}"

        if p == "ssr":
            # ssr://base64(...)
            from urllib.parse import quote as q
            body = f"{self.server}:{self.port}:{self.extra.get('ssr_protocol','origin')}:{self.security or 'aes-256-cfb'}:{self.extra.get('ssr_obfs','plain')}:{base64.b64encode(self.uuid.encode()).decode()}"
            params = f"?obfsparam={q(self.extra.get('obfsparam',''),safe='')}&remarks={q(self.remark,safe='')}"
            payload = base64.b64encode(f"{body}/{params}".encode()).decode()
            return f"ssr://{payload}"

        # trojan, vless, hysteria2, tuic: URL 格式
        params = []
        if self.sni and self.sni != self.server:
            params.append(f"sni={quote(self.sni, safe='')}")
        if self.transport != "tcp":
            params.append(f"type={quote(self.transport, safe='')}")
        if self.security and p in ("ss",):
            pass  # handled above
        query = "&".join(params)
        query_str = f"?{query}" if query else ""

        if p == "trojan":
            return f"trojan://{userinfo}@{host}{query_str}{fragment}"
        if p == "vless":
            return f"vless://{userinfo}@{host}{query_str}{fragment}"
        if p in ("hysteria2", "hy2", "hysteria"):
            return f"hysteria2://{userinfo}@{host}{query_str}{fragment}"
        if p == "tuic":
            return f"tuic://{userinfo}@{host}{query_str}{fragment}"

        # 无法识别的协议 → JSON 兜底
        return json.dumps(self.to_dict(), ensure_ascii=False)

    def __hash__(self) -> int:
        """哈希值基于 identity_key，用于 set/dict 去重。"""
        return hash(self.identity_key())

    def __eq__(self, other) -> bool:
        """相等性基于 identity_key。"""
        if not isinstance(other, StandardProxy):
            return False
        return self.identity_key() == other.identity_key()

    def __repr__(self) -> str:
        return (f"StandardProxy({self.protocol}://{self.server}:{self.port}"
                f"{'#' + self.remark if self.remark else ''})")


# ==================== Clash / Sing-box 格式字段映射 ====================

# Clash proxy 字段 → StandardProxy 属性映射
# 用于从 YAML/JSON 配置中提取节点信息
CLASH_FIELD_MAP = {
    "name": "remark",
    "type": "protocol",
    "server": "server",
    "port": "port",
    "uuid": "uuid",
    "password": "uuid",
    "cipher": "security",
    "network": "transport",
    "tls": "tls",
    "sni": "sni",
    "servername": "sni",
    "skip-cert-verify": "allow_insecure",
    "alterId": ("extra", "alterId"),
}

# Sing-box outbound 字段 → StandardProxy 属性映射
SINGBOX_FIELD_MAP = {
    "tag": "remark",
    "type": "protocol",
    "server": "server",
    "server_port": "port",
    "uuid": "uuid",
    "password": "uuid",
    "method": "security",
    "transport": "transport",
    "tls": ("extra", "tls"),
    "sni": "sni",
    "server_name": "sni",
}


def dict_to_standard_proxy(d: dict, field_map: dict = None) -> Optional[StandardProxy]:
    """从字典（Clash/Sing-box 格式）创建 StandardProxy。

    自动识别并处理 Clash 和 Sing-box 两种字段命名差异。

    Args:
        d: 原始配置字典
        field_map: 字段映射表，默认自动选择 CLASH_FIELD_MAP + SINGBOX_FIELD_MAP

    Returns:
        StandardProxy 实例，或 None（如果缺少核心字段）
    """
    if field_map is None:
        # 自动检测格式：Sing-box 使用 server_port，Clash 使用 port
        if "server_port" in d:
            field_map = SINGBOX_FIELD_MAP
        else:
            field_map = CLASH_FIELD_MAP

    try:
        protocol = d.get("type", "")
        if not protocol:
            return None

        # 识别支持的协议
        supported = {"vmess", "vless", "trojan", "ss", "ssr", "hysteria2", "tuic", "hy2"}
        if protocol.lower() not in supported:
            return None

        server = d.get("server", "")
        port = d.get("port") or d.get("server_port") or 0
        uuid = d.get("uuid") or d.get("password", "")

        if not server or not port:
            return None

        proxy = StandardProxy(
            protocol=protocol.lower(),
            server=server,
            port=int(port),
            uuid=str(uuid),
            remark=d.get("name") or d.get("tag", ""),
            security=d.get("cipher") or d.get("method", ""),
            transport=d.get("network") or d.get("transport", "tcp"),
            tls=bool(d.get("tls", False)),
            sni=d.get("sni") or d.get("servername") or d.get("server_name", ""),
            allow_insecure=bool(d.get("skip-cert-verify", False)),
        )
        return proxy
    except Exception:
        return None
