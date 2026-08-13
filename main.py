"""
主入口 — 多源搜集节点，输出 alive.txt。

架构：
  搜集线程 (Collector) — 并行多通道（GitHub / Web）
  → 边搜集边持久化 → 输出 alive.txt

所有日志同时输出到控制台和 log/ 文件夹。

运行环境：GitHub Actions（Linux，2 核，单次 6 小时上限），也可本机运行。
本入口只负责：日志 Tee、搜索关键词构建、信号处理、启动 Collector。
队列调度 / 下载 / 解析 / 收尾去重等全部逻辑在 collector.py 中。
文件头 docstring 中"输出 alive.txt"为早期版本描述，当前实际输出
由 collector 收尾阶段写入 no.txt / no/ 分片（详见 docs/DESIGN.md）。
"""

import os
import sys
import time
import glob
import signal
import threading
from datetime import datetime, timezone, timedelta

from collector import Collector
from utils import now_str
from config import (
    GITHUB_TOKEN, BASE_QUERIES, SEARCH_SUFFIX, SEARCH_IN,
    LOG_DIR, MAX_LOG_FILES,
)


# ==================== 日志持久化 ====================

os.makedirs(LOG_DIR, exist_ok=True)
# 文件名用 UTC 时间（与 GA 控制台时间戳同源，便于对账下载日志），
# 而日志内容时间戳用北京时间（now_str()）——两者相差 8 小时，
# 分析时勿混用（曾把 UTC 当北京时间导致运行状态误判，见 docs/DESIGN.md 踩坑 27）
log_filename = f"collect_{datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')}.log"
log_path = os.path.join(LOG_DIR, log_filename)


class Tee:
    """同时输出到原始 stdout/stderr 和日志文件。"""
    def __init__(self, file, stream):
        self.file = file
        self.stream = stream

    def write(self, data):
        # 每次写入都立即 flush：GA 的日志管道有缓冲，进程随时可能被
        # 强杀/取消（SIGKILL 不触发任何收尾），不逐行刷出会丢最后一段日志
        self.file.write(data)
        self.file.flush()
        self.stream.write(data)
        self.stream.flush()

    def flush(self):
        self.file.flush()
        self.stream.flush()

    def close(self):
        self.flush()
        if self.file:
            self.file.close()


log_file = open(log_path, "a", encoding="utf-8")
sys.stdout = Tee(log_file, sys.__stdout__)
sys.stderr = Tee(log_file, sys.__stderr__)

existing_logs = sorted(
    glob.glob(os.path.join(LOG_DIR, "collect_*.log")),
    key=os.path.getctime
)
# 只保留最近 MAX_LOG_FILES 个日志：每轮运行都生成一个 log 文件，
# 长期不清理会膨胀（曾把 66MB 日志误提交进 GA 仓库，见 docs/DESIGN.md）；
# 按创建时间逐个删最老的
while len(existing_logs) > MAX_LOG_FILES:
    os.remove(existing_logs[0])
    existing_logs.pop(0)


# ==================== 搜索关键词构建 ====================

def build_queries():
    """动态构建搜索关键词列表。"""
    utc_now = datetime.now(timezone.utc)
    # pushed:>24h —— 与 SKIP_PROCESSING_AGE_HOURS=24 保持一致的唯一年龄阈值
    # （历史上曾用 168h，是黑名单机制的配套遗留，黑名单删除后 168h 分支
    #  同步废弃，统一为 24h，见 docs/DESIGN.md 决策 33）
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


# ==================== 停止信号处理 ====================

def install_signal_handler(collector):
    """安装停止信号处理（SIGINT/SIGTERM）——取消/终止时优雅退出。

    背景：GA 取消 job 时发送 SIGINT/SIGTERM。默认 KeyboardInterrupt 的
    抛出时机不稳定（主线程可能在 communicate/网络等待中），且退出前
    flush stdout 可能卡在 GA 日志管道上（08085 取消后"运行搜集"转圈 1 小时）。

    处理逻辑：
      1. 重定向 stdout/stderr 到 devnull——退出前 flush 不再碰可能阻塞的管道
      2. 置 limiter.exceeded=True → 所有 _should_stop() 检查点（worker/
         搜索/队列等待循环）优雅退出 → collector.run() 正常收尾保存数据
      3. 120 秒后强制 os._exit(0) 兜底——主线程若卡在不可中断等待
         （communicate 900s / 网络超时）也能保证进程退出，GA 不再干等

    正常运行（无信号）时此处理完全不参与，零影响。
    """
    def _handler(signum, frame):
        try:
            # 防退出 flush 阻塞：stdout/stderr 指向 devnull（后续日志进黑洞）
            sys.stdout = open(os.devnull, 'w')
            sys.stderr = open(os.devnull, 'w')
            # 置停止标志：worker/搜索/队列循环检测到后优雅收尾
            try:
                collector.limiter.exceeded = True
            except Exception:
                pass
            # 兜底：120 秒后强制退出（_finalize 收尾通常 <60s）
            threading.Timer(120, lambda: os._exit(0)).start()
        except Exception:
            os._exit(0)

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


# ==================== 主流程 ====================

def main():
    start_time = time.time()
    queries = build_queries()
    print(f"[{now_str()}] 🚀 程序启动")
    print(f"[{now_str()}] 关键词: {len(queries)} 个", flush=True)

    collector = Collector(token=GITHUB_TOKEN, queries=queries)
    # 安装停止信号处理（GA 取消/终止时优雅退出，防"运行搜集"转圈卡死）
    install_signal_handler(collector)
    collector.run()

    # 节点已由 collector 内部自动保存到 no.txt
    total_elapsed = time.time() - start_time
    print(f"[{now_str()}] 🎉 全部完成，总耗时 {total_elapsed:.1f} 秒", flush=True)

    # 恢复原始 stdout/stderr，避免 Python 退出时 flush 已关闭的 Tee 导致 exit code 120
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    log_file.close()

    # 强制退出：不等待任何残留线程（卡住的解析/下载线程会让 Python 在
    # 收尾后挂 30 分钟，08091 的 pid 2302 被 GA 超时杀的教训）。
    # _finalize 已保存全部分片/SHA 缓存/种子/clone_stats，os._exit 跳过
    # 解释器退出清理，数据安全。不能用线程池 initializer 设 daemon——
    # Python 3.10+ 禁止在线程启动后改 daemon（08101 线程池全废事故）。
    os._exit(0)


if __name__ == "__main__":
    main()
