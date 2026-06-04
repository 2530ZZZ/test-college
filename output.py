"""
输出模块。

负责将测速结果格式化输出为 alive.txt 和 mihomo.yaml。
与搜集和测速完全解耦，可独立调用。

支持：
  - alive.txt: 每行一个存活节点 URI
  - mihomo.yaml: mihomo 兼容的代理配置文件
  - 结果统计摘要
"""

import os
import json
from typing import List, Dict
from datetime import datetime, timezone, timedelta

from config import (
    ALIVE_NODE_FILE, MIHOMO_OUTPUT_FILE, MIHOMO_TEMPLATE_FILE,
    MIHOMO_MIXED_PORT, MIHOMO_API_PORT,
    SUBS_CHECK_LATENCY_URL,
)
from utils import now_str


def save_alive_nodes(node_uris: List[str], output_path: str = None):
    """保存存活节点到 alive.txt。

    每行一个节点 URI，文件末尾保留空行。

    Args:
        node_uris: 存活节点 URI 列表
        output_path: 输出文件路径，默认 config.ALIVE_NODE_FILE
    """
    path = output_path or ALIVE_NODE_FILE
    with open(path, "w", encoding="utf-8") as f:
        if node_uris:
            f.write("\n".join(node_uris))
    print(f"[{now_str()}] 保存 {path} ({len(node_uris)} 个节点)", flush=True)


def merge_batch_results(results: Dict[int, List[str]],
                        errors: Dict[int, str] = None) -> List[str]:
    """合并各批次的测速结果为统一的存活节点列表。

    按 batch_id 排序以保证顺序一致性。去重（不同批次可能返回相同节点）。

    Args:
        results: batch_id → [uri, uri, ...]
        errors: batch_id → 错误信息（用于日志）

    Returns:
        去重后的存活节点 URI 列表
    """
    if errors:
        for batch_id, err in errors.items():
            print(f"[{now_str()}] ⚠️ 批次 {batch_id} 测速失败: {err}", flush=True)

    all_uris = []
    seen = set()
    for batch_id in sorted(results.keys()):
        for uri in results[batch_id]:
            if uri not in seen:
                seen.add(uri)
                all_uris.append(uri)

    print(f"[{now_str()}] 合并 {len(results)} 个批次结果: "
          f"去重后 {len(all_uris)} 个存活节点 "
          f"({len(errors or {})} 个批次失败)", flush=True)
    return all_uris


def generate_mihomo_yaml(node_uris: List[str],
                         template_path: str = None,
                         output_path: str = None):
    """生成 mihomo 兼容的代理配置文件。

    注意：本函数生成的是供终端用户订阅的 mihomo 配置，
    不再作为测速引擎使用（测速已由 subs-check 完成）。

    配置结构：
      - proxies: 每个节点一个条目（使用 raw link 格式）
      - proxy-groups: auto 组包含所有节点（url-test 自动选优）

    Args:
        node_uris: 存活节点 URI 列表
        template_path: 模板文件路径，默认 config.MIHOMO_TEMPLATE_FILE
        output_path: 输出文件路径，默认 config.MIHOMO_OUTPUT_FILE
    """
    tpl_path = template_path or MIHOMO_TEMPLATE_FILE
    out_path = output_path or MIHOMO_OUTPUT_FILE

    # 生成节点列表
    proxies = []
    proxy_names = []
    for idx, raw in enumerate(node_uris):
        name = f"alive_{idx}"
        # 注意：正确的 mihomo 代理配置应包含完整字段而非仅 link
        # 这里使用 link 格式是因为原始 URI (vmess://...) 对 mihomo 是合法格式
        proxies.append({"name": name, "link": raw})
        proxy_names.append(name)

    # 使用模板或默认配置
    if os.path.exists(tpl_path):
        try:
            with open(tpl_path, "r", encoding="utf-8") as f:
                content = f.read()

            try:
                import yaml
                config = yaml.safe_load(content)
            except ImportError:
                config = json.loads(content)

            if isinstance(config, dict):
                config["proxies"] = proxies
                if "proxy-groups" in config and isinstance(config["proxy-groups"], list):
                    for group in config["proxy-groups"]:
                        if group.get("name") in ("auto", "🌐 PROXY"):
                            group["proxies"] = proxy_names

                _write_yaml_or_json(out_path, config)
                print(f"[{now_str()}] 已生成 {out_path} "
                      f"(基于模板 {tpl_path}, {len(node_uris)} 个节点)", flush=True)
                return
        except Exception as e:
            print(f"[{now_str()}] 模板处理失败: {e}，使用默认配置", flush=True)

    # 默认配置（无模板）
    default_config = {
        "mixed-port": MIHOMO_MIXED_PORT,
        "external-controller": f"127.0.0.1:{MIHOMO_API_PORT}",
        "allow-lan": False,
        "mode": "rule",
        "log-level": "error",
        "proxies": proxies,
        "proxy-groups": [
            {
                "name": "auto",
                "type": "url-test",
                "proxies": proxy_names,
                "url": SUBS_CHECK_LATENCY_URL,
                "interval": 3600,
            }
        ],
    }
    _write_yaml_or_json(out_path, default_config)
    print(f"[{now_str()}] 已生成 {out_path} "
          f"(默认配置, {len(node_uris)} 个节点)", flush=True)


def generate_stats_summary(total_collected: int,
                           total_unique: int,
                           batches_tested: int,
                           total_alive: int,
                           elapsed_seconds: float,
                           errors_count: int = 0) -> str:
    """生成运行统计摘要。

    Args:
        total_collected: 搜索处理的仓库数
        total_unique: 去重后唯一节点数
        batches_tested: 测速批次数
        total_alive: 最终存活节点数
        elapsed_seconds: 总耗时（秒）
        errors_count: 失败的批次数

    Returns:
        格式化的统计摘要字符串
    """
    elapsed_min = elapsed_seconds / 60
    alive_rate = (total_alive / total_unique * 100) if total_unique > 0 else 0

    lines = [
        "=" * 60,
        "  节点收集与测速统计",
        "=" * 60,
        f"  检查仓库:     {total_collected}",
        f"  去重后节点:   {total_unique}",
        f"  测速批次:     {batches_tested} (失败 {errors_count})",
        f"  存活节点:     {total_alive}",
        f"  存活率:       {alive_rate:.1f}%",
        f"  总耗时:       {elapsed_min:.1f} 分钟 ({elapsed_seconds:.0f} 秒)",
        "=" * 60,
    ]
    return "\n".join(lines)


# ==================== 内部辅助 ====================

def _write_yaml_or_json(path: str, data: dict):
    """写入 YAML 或 JSON 文件。

    优先使用 yaml 模块，不可用时回退到 JSON。

    Args:
        path: 输出文件路径
        data: 配置字典
    """
    try:
        import yaml
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(data, f, allow_unicode=True, default_flow_style=False)
    except ImportError:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


def save_batch_nodes(node_uris: List[str], batch_id: int,
                     base_dir: str = ".") -> str:
    """持久化一个批次的节点到文件。

    独立于 Collector 的批次写入，用于直接从 URI 列表写入。

    Args:
        node_uris: 节点 URI 列表
        batch_id: 批次序号
        base_dir: 基础目录

    Returns:
        写入的文件路径
    """
    filename = os.path.join(base_dir, f"no_batch_{batch_id:04d}.txt")
    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(node_uris))
    return filename
