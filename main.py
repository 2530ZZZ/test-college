"""
主入口 — 动态生成搜索关键词，启动收集与测速。

架构：
  搜集线程 (Collector)                测速线程 (TestOrchestrator)
  ──────────────                      ──────────────────────────
  搜索仓库 → 提取节点 → 去重          等待批次文件
  → 每凑够 10000 节点刷盘              → subprocess 启动 subs-check
  → on_batch_flush 回调                → 并发管理（默认 3 个）
  → 投喂 TestOrchestrator             → 回收结果
                                       → 合并 alive.txt + mihomo.yaml
  搜集完成 → signal_done()

所有日志同时输出到控制台和 log/ 文件夹，保留最近 N 个日志文件。
"""

import os
import sys
import time
import glob
import threading
from datetime import datetime, timezone, timedelta

from collector import Collector
from test_orchestrator import TestOrchestrator
from output import (
    save_alive_nodes, merge_batch_results,
    generate_mihomo_yaml, generate_stats_summary,
)
from utils import now_str
from config import (
    GITHUB_TOKEN, BASE_QUERIES, SEARCH_SUFFIX, SEARCH_IN,
    LOG_DIR, MAX_LOG_FILES,
    SUBS_CHECK_BATCH_SIZE, SUBS_CHECK_MAX_CONCURRENT,
    SUBS_CHECK_BIN,
)


# ==================== 日志持久化 ====================

os.makedirs(LOG_DIR, exist_ok=True)

log_filename = f"collect_{datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')}.log"
log_path = os.path.join(LOG_DIR, log_filename)


class Tee:
    """同时输出到原始 stdout/stderr 和日志文件。"""
    def __init__(self, file, stream):
        self.file = file
        self.stream = stream

    def write(self, data):
        self.file.write(data)
        self.file.flush()
        self.stream.write(data)
        self.stream.flush()

    def flush(self):
        self.file.flush()
        self.stream.flush()


log_file = open(log_path, "a", encoding="utf-8")
sys.stdout = Tee(log_file, sys.__stdout__)
sys.stderr = Tee(log_file, sys.__stderr__)

# 清理旧日志，保留最近 N 个
existing_logs = sorted(
    glob.glob(os.path.join(LOG_DIR, "collect_*.log")),
    key=os.path.getctime
)
while len(existing_logs) > MAX_LOG_FILES:
    os.remove(existing_logs[0])
    existing_logs.pop(0)


# ==================== 搜索关键词构建 ====================

def build_queries():
    """动态构建搜索关键词列表。

    为每个基础关键词附加：
      - pushed:>24h（只搜索 24 小时内推送的仓库）
      - 语言排除、fork 设置（来自 SEARCH_SUFFIX）
      - 搜索范围限定（可选）

    Returns:
        搜索关键词字符串列表
    """
    utc_now = datetime.now(timezone.utc)
    time_limit = (utc_now - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')
    time_suffix = f"pushed:>{time_limit}"

    queries = []
    for q in BASE_QUERIES:
        if SEARCH_IN:
            query_body = f"{q} in:{SEARCH_IN} {time_suffix} {SEARCH_SUFFIX}"
        else:
            query_body = f"{q} {time_suffix} {SEARCH_SUFFIX}"
        queries.append(query_body)

    return queries


# ==================== 主流程 ====================

def main():
    """主流程：搜集 + 测速双线程编排。

    流程：
      1. 构建搜索关键词
      2. 启动 TestOrchestrator（后台线程）
      3. 运行 Collector（主线程），边搜集边通过回调投喂批次
      4. 搜集完成后通知编排器
      5. 等待测速完成
      6. 合并结果 → alive.txt + mihomo.yaml
    """
    start_time = time.time()

    # ---- 阶段 0: 构建搜索关键词 ----
    queries = build_queries()
    print(f"[{now_str()}] 🚀 程序启动")
    print(f"[{now_str()}] 关键词: {len(queries)} 个, "
          f"时间基准: {(datetime.now(timezone.utc) - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')} UTC", flush=True)

    # ---- 阶段 1: 启动测速编排器 ----
    orch = TestOrchestrator()
    orch.start()

    # ---- 阶段 2: 运行搜集（边搜集边通过回调投喂） ----
    # 回调函数：将持久化的批次文件投喂给测速编排器
    def on_batch_flush(batch_id, file_path, node_count):
        orch.enqueue(file_path)

    # 检查 subs-check 是否可用
    subs_check_available = _check_subs_check()
    if not subs_check_available:
        print(f"[{now_str()}] ⚠️ subs-check 不可用，将跳过测速阶段", flush=True)

    collector = Collector(
        token=GITHUB_TOKEN,
        queries=queries,
        on_batch_flush=on_batch_flush if subs_check_available else None,
    )

    # 搜集运行（主线程）
    collector_start = time.time()
    print(f"[{now_str()}] 🔍 开始搜集节点...", flush=True)

    try:
        collector.run()
    except Exception as e:
        print(f"[{now_str()}] ❌ 搜集过程崩溃: {e}", flush=True)
        import traceback
        traceback.print_exc()
        # 已持久化的批次文件不受影响

    collect_elapsed = time.time() - collector_start
    print(f"[{now_str()}] 搜集完成，耗时 {collect_elapsed:.0f}s, "
          f"唯一节点: {len(collector.unique_nodes)}, "
          f"批次: {len(collector.batch_file_paths)}", flush=True)

    # ---- 阶段 3: 等待测速完成 ----
    if subs_check_available and collector.batch_file_paths:
        print(f"[{now_str()}] 🧪 等待测速完成...", flush=True)
        orch.signal_done()
        results, errors = orch.wait()
        stats = orch.get_stats()
        print(f"[{now_str()}] 测速统计: 完成 {stats['completed']}, "
              f"失败 {stats['failed']}, 存活 {stats['total_alive']}", flush=True)

        # 合并去重
        alive_uris = merge_batch_results(results, errors)
    else:
        # 无 subs-check 或无线程：所有唯一节点视为"未测速存活"
        print(f"[{now_str()}] ⚠️ 跳过测速，所有 {len(collector.unique_nodes)} 个节点"
              f"输出为 alive.txt (未验证)", flush=True)
        alive_uris = list(collector.unique_nodes)
        results, errors = {}, {}

    # ---- 阶段 4: 输出 ----
    # 4.1 alive.txt
    save_alive_nodes(alive_uris)

    # 4.2 mihomo.yaml
    generate_mihomo_yaml(alive_uris)

    # 4.3 统计
    stats_text = generate_stats_summary(
        total_collected=collector.checked_count,
        total_unique=len(collector.unique_nodes),
        batches_tested=len(results),
        total_alive=len(alive_uris),
        elapsed_seconds=time.time() - start_time,
        errors_count=len(errors),
    )
    print(stats_text, flush=True)
    log_file.write(stats_text + "\n")
    log_file.flush()

    # 4.4 最终日志
    total_elapsed = time.time() - start_time
    print(f"[{now_str()}] 🎉 全部完成，总耗时 {total_elapsed:.1f} 秒", flush=True)
    log_file.close()


# ==================== 辅助 ====================

def _check_subs_check() -> bool:
    """检查 subs-check 是否可用。

    尝试在 PATH 中查找，或使用 shutil.which。

    Returns:
        True 如果 subs-check 可执行
    """
    import shutil
    found = shutil.which(SUBS_CHECK_BIN)
    if found:
        print(f"[{now_str()}] ✅ subs-check 已就绪: {found}", flush=True)
        return True

    # 尝试当前目录
    local_path = os.path.join(os.getcwd(), SUBS_CHECK_BIN)
    if os.path.isfile(local_path) and os.access(local_path, os.X_OK):
        print(f"[{now_str()}] ✅ subs-check 已就绪: {local_path}", flush=True)
        return True

    print(f"[{now_str()}] ⚠️ subs-check ({SUBS_CHECK_BIN}) 未找到", flush=True)
    return False


# ==================== 入口 ====================

if __name__ == "__main__":
    main()
