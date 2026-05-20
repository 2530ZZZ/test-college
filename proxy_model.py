"""
统一的节点数据模型。
所有解析器最终都将节点转换为 StandardProxy 对象，
后续的测速、过滤、输出等模块都基于这个模型工作。
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class StandardProxy:
    """标准化的代理节点数据结构"""
    # 核心字段
    protocol: str                  # vmess, vless, trojan, ss, ssr, hysteria2, tuic, hy2 等
    server: str                    # 服务器地址
    port: int                      # 端口号
    uuid: str = ""                 # 用户ID / 密码 / UUID
    security: str = ""             # 加密方式（如 auto, chacha20-ietf-poly1305）
    transport: str = "tcp"         # 传输协议 tcp/ws/grpc/h2
    tls: bool = False              # 是否启用 TLS
    sni: str = ""                  # TLS SNI
    allow_insecure: bool = False   # 是否跳过证书验证
    remark: str = ""               # 节点备注/名称
    # 额外信息
    raw_link: str = ""             # 原始分享链接 (vmess://... / trojan://...)
    source_url: str = ""           # 来源文件的 raw URL

    def to_node_line(self) -> str:
        """
        生成 no.txt 中存储的标准格式行。
        默认直接使用 raw_link，如果不存在则返回序列化后的 JSON。
        """
        if self.raw_link:
            return self.raw_link
        # 后备：JSON 格式（实际项目中建议统一用 raw_link）
        import json
        return json.dumps(self.__dict__, ensure_ascii=False)
