"""
输出模块 — 保存存活节点到 alive.txt。
"""

from config import ALIVE_NODE_FILE
from utils import now_str


def save_alive_nodes(node_uris: list, output_path: str = None):
    """保存存活节点到 alive.txt，每行一个 URI。

    Args:
        node_uris: 节点 URI 列表
        output_path: 输出文件路径，默认 config.ALIVE_NODE_FILE
    """
    path = output_path or ALIVE_NODE_FILE
    # 先清除非法字符（防止 surrogate 导致写入失败）
    text = "\n".join(node_uris).encode("utf-8", errors="replace").decode("utf-8")
    with open(path, "w", encoding="utf-8") as f:
        if node_uris:
            f.write(text)
    print(f"[{now_str()}] 保存 {path} ({len(node_uris)} 个节点)", flush=True)
