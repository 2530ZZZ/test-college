"""
subs-check 并发编排器。

负责管理 subs-check 的并发测速流程：
  - 维护测试队列（搜集线程投喂批次）
  - 控制并发实例数（subprocess.Popen 进程隔离）
  - 错误隔离（每个批次独立，崩溃不扩散）
  - 超时处理（单批次超时自动 kill）
  - 结果汇总

设计原则：
  1. 搜集和测速通过队列通信，互相独立
  2. 每个 subs-check 实例运行在独立子进程，端口不冲突
  3. 任何单批次故障不影响其他批次和搜集
  4. 持久化优先 → 批次文件先写磁盘，再入队测速
"""

import os
import shutil
import time
import json
import subprocess
import threading
from queue import Queue, Empty
from typing import List, Dict, Optional, Callable

from config import (
    SUBS_CHECK_BIN, SUBS_CHECK_BATCH_SIZE, SUBS_CHECK_MAX_CONCURRENT,
    SUBS_CHECK_BATCH_TIMEOUT, SUBS_CHECK_BASE_PORT, SUBS_CHECK_CONCURRENT,
    SUBS_CHECK_LATENCY_URL, SUBS_CHECK_SPEED_TEST_URL,
    LATENCY_TIMEOUT, SPEED_TIMEOUT,
)
from utils import now_str


class TestOrchestrator:
    """subs-check 并发测速编排器。

    搜集线程通过 enqueue() 投喂批次文件，编排器自动管理并发。
    使用示例：
        orch = TestOrchestrator()
        orch.start()                          # 启动编排线程
        orch.enqueue("no_batch_0001.txt")     # 投喂批次
        orch.signal_done()                    # 搜集完成
        results, errors = orch.wait()         # 等待并获取结果
    """

    def __init__(
        self,
        batch_size: int = None,
        max_concurrent: int = None,
        batch_timeout: int = None,
        base_port: int = None,
        subs_check_bin: str = None,
    ):
        """
        Args:
            batch_size: 每批节点数（仅用于日志）
            max_concurrent: 最大并发 subs-check 实例数
            batch_timeout: 单批次超时秒数
            base_port: subs-check 实例起始端口
            subs_check_bin: subs-check 二进制路径
        """
        self.batch_size = batch_size or SUBS_CHECK_BATCH_SIZE
        self.max_concurrent = max_concurrent or SUBS_CHECK_MAX_CONCURRENT
        self.batch_timeout = batch_timeout or SUBS_CHECK_BATCH_TIMEOUT
        self.base_port = base_port or SUBS_CHECK_BASE_PORT
        self.subs_check_bin = subs_check_bin or SUBS_CHECK_BIN

        # 解析二进制文件全路径（subprocess.Popen 不搜当前目录）
        resolved = shutil.which(self.subs_check_bin)
        if not resolved:
            local = os.path.join(os.getcwd(), self.subs_check_bin)
            if os.path.isfile(local):
                resolved = local
        if resolved:
            self.subs_check_bin = resolved

        # 队列与状态
        self._queue = Queue()
        self._active: Dict[int, subprocess.Popen] = {}   # batch_id → process
        self._active_files: Dict[int, str] = {}           # batch_id → input_file
        self._results: Dict[int, List[str]] = {}          # batch_id → alive_uris
        self._errors: Dict[int, str] = {}                 # batch_id → error_message
        self._batch_id_counter = 0
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    @property
    def active_count(self) -> int:
        """当前正在运行的 subs-check 实例数。"""
        return len(self._active)

    @property
    def total_batches(self) -> int:
        """已完成（成功+失败）的批次数。"""
        return len(self._results) + len(self._errors)

    def enqueue(self, input_file: str) -> int:
        """投喂一个批次文件到测速队列。

        Args:
            input_file: 批次节点文件路径（如 "no_batch_0001.txt"）

        Returns:
            分配的 batch_id
        """
        self._batch_id_counter += 1
        batch_id = self._batch_id_counter
        self._queue.put((batch_id, input_file))
        print(f"[{now_str()}] 📥 批次 {batch_id} 已入队测速队列 "
              f"({input_file})", flush=True)
        return batch_id

    def signal_done(self):
        """信号：搜集完成，所有批次已入队。

        编排器处理完队列中所有批次后将自动返回。
        """
        self._queue.put(None)  # sentinel
        print(f"[{now_str()}] 🏁 所有批次已入队，等待测速完成", flush=True)

    def start(self):
        """启动编排线程（非阻塞）。

        线程内运行 _run_loop()，循环消费队列并管理并发。
        """
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def wait(self) -> tuple:
        """等待编排线程完成，返回结果。

        Returns:
            (results: dict[batch_id → list[uris]], errors: dict[batch_id → str])
        """
        if self._thread and self._thread.is_alive():
            self._thread.join()
        return dict(self._results), dict(self._errors)

    # ---- 内部循环 ----

    def _run_loop(self):
        """编排器主循环。

        逻辑：
          1. 回收已完成/崩溃的进程
          2. 如果槽位有空闲，从队列取新批次
          3. 启动 subs-check 子进程
          4. 循环直到收到 sentinel 且无活跃进程
        """
        print(f"[{now_str()}] 🧪 测速编排器启动 "
              f"(并发={self.max_concurrent}, 批次大小={self.batch_size})", flush=True)

        sentinel_received = False
        batches_received = 0
        batch_start_time = {}  # batch_id → start_timestamp

        while self._running:
            # 1. 检查是否有超时的批次（超过 SUBS_CHECK_BATCH_TIMEOUT 秒无结果）
            now = time.time()
            for bid, start_ts in list(batch_start_time.items()):
                if bid in self._active:
                    elapsed = now - start_ts
                    if elapsed > self.batch_timeout:
                        proc = self._active.get(bid)
                        if proc:
                            try:
                                proc.kill()
                                proc.wait(timeout=3)
                            except Exception:
                                pass
                            print(f"[{now_str()}] ⏰ 批次 {bid} 超时 "
                                  f"({int(elapsed)}s > {self.batch_timeout}s)，强制终止",
                                  flush=True)
                            self._errors[bid] = f"运行超时 ({int(elapsed)}s)"
                            del self._active[bid]
                        batch_start_time.pop(bid, None)
                        # 减少并发槽位计数，允许启动新批次

            # 2. 回收已完成的进程
            self._reap_finished()

            # 3. 尝试启动新批次
            while len(self._active) < self.max_concurrent:
                try:
                    item = self._queue.get(timeout=2)
                except Empty:
                    if sentinel_received and not self._active:
                        if batches_received == 0:
                            print(f"[{now_str()}] ⚠️ 编排器未收到任何批次文件，"
                                  f"跳过测速", flush=True)
                        else:
                            completed = len(self._results)
                            failed = len(self._errors)
                            print(f"[{now_str()}] ✅ 测速完成: "
                                  f"{completed} 批成功, {failed} 批失败",
                                  flush=True)
                        return
                    break

                if item is None:  # sentinel
                    sentinel_received = True
                    if not self._active:
                        completed = len(self._results)
                        failed = len(self._errors)
                        print(f"[{now_str()}] ✅ 测速完成: "
                              f"{completed} 批成功, {failed} 批失败", flush=True)
                        return
                    break

                batch_id, input_file = item
                batches_received += 1
                batch_start_time[batch_id] = time.time()
                self._spawn(batch_id, input_file)

            # 3. 如果收到 sentinel 且无活跃进程 → 退出
            if sentinel_received and not self._active:
                if batches_received == 0:
                    print(f"[{now_str()}] ⚠️ 编排器未收到任何批次文件，"
                          f"跳过测速", flush=True)
                return

            # 4. 短暂休眠（只需在无活跃进程时等队列）
            if not self._active:
                time.sleep(1)
            else:
                time.sleep(0.5)

        # 清理：等待所有剩余进程
        if batches_received > 0:
            self._wait_all()
        else:
            print(f"[{now_str()}] ⚠️ 编排器未收到任何批次文件，跳过测速", flush=True)

    def _spawn(self, batch_id: int, input_file: str):
        """启动一个 subs-check 子进程。

        Args:
            batch_id: 批次 ID
            input_file: 节点文件路径
        """
        port = self.base_port + batch_id * 10

        # 生成专属配置文件
        config_path = self._generate_config(batch_id, input_file, port)

        try:
            proc = subprocess.Popen(
                [self.subs_check_bin, "-f", config_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            self._active[batch_id] = proc
            self._active_files[batch_id] = input_file
            print(f"[{now_str()}] 🚀 批次 {batch_id} subs-check 启动 "
                  f"(端口={port}, 文件={input_file})", flush=True)

        except FileNotFoundError:
            err = f"subs-check 二进制文件未找到: {self.subs_check_bin}"
            print(f"[{now_str()}] ❌ 批次 {batch_id}: {err}", flush=True)
            self._errors[batch_id] = err
        except Exception as e:
            err = f"启动 subs-check 失败: {e}"
            print(f"[{now_str()}] ❌ 批次 {batch_id}: {err}", flush=True)
            self._errors[batch_id] = err

    def _reap_finished(self):
        """回收所有已完成的子进程。

        检查进程返回码：
          - 0: 成功 → 解析输出
          - 非 0: 失败 → 记录错误
        """
        done_ids = []
        for batch_id, proc in self._active.items():
            retcode = proc.poll()
            if retcode is not None:
                done_ids.append(batch_id)
                if retcode == 0:
                    alive = self._parse_output(batch_id)
                    self._results[batch_id] = alive
                    print(f"[{now_str()}] ✅ 批次 {batch_id} 完成: "
                          f"{len(alive)} 个存活节点", flush=True)
                else:
                    stderr = ""
                    try:
                        stderr = proc.stderr.read()[:500]
                    except Exception:
                        pass
                    err = f"subs-check 异常退出 (code={retcode}): {stderr}"
                    print(f"[{now_str()}] ❌ 批次 {batch_id}: {err}", flush=True)
                    self._errors[batch_id] = err

        for batch_id in done_ids:
            del self._active[batch_id]
            # 清理临时配置文件
            config_path = f"subs_check_batch_{batch_id}.yaml"
            if os.path.exists(config_path):
                try:
                    os.remove(config_path)
                except Exception:
                    pass

    def _wait_all(self):
        """等待所有活跃进程，带超时保护。

        超时后 kill 进程并标记为失败。
        """
        deadline = time.time() + self.batch_timeout
        while self._active and time.time() < deadline:
            self._reap_finished()
            if self._active:
                time.sleep(2)

        # 超时处理：kill 仍未完成的进程
        for batch_id, proc in list(self._active.items()):
            if proc.poll() is None:
                try:
                    proc.kill()
                    proc.wait(timeout=5)
                except Exception:
                    pass
                err = f"批次超时 ({self.batch_timeout}s)"
                print(f"[{now_str()}] ⏰ 批次 {batch_id}: {err}，强制终止", flush=True)
                self._errors[batch_id] = err

    def _generate_config(self, batch_id: int, input_file: str, port: int) -> str:
        """为 subs-check 实例生成临时配置文件。

        每个实例使用独立的端口，避免冲突。

        Args:
            batch_id: 批次 ID
            input_file: 节点文件路径
            port: 分配的基础端口

        Returns:
            配置文件路径
        """
        config_path = f"subs_check_batch_{batch_id}.yaml"
        # concurrent 控制并发 goroutine 数，默认很低（实测约1-2）
        # 不设的话 subs-check 默认值会很慢。
        config = {
            "mixed-port": port,
            "external-controller": f"127.0.0.1:{port + 1}",
            "allow-lan": False,
            "mode": "rule",
            "log-level": "error",
            "proxies-file": input_file,
            "concurrent": SUBS_CHECK_CONCURRENT,
            "url": SUBS_CHECK_LATENCY_URL,
            "timeout": LATENCY_TIMEOUT,
            "speed-test-url": SUBS_CHECK_SPEED_TEST_URL,
            "speed-test-timeout": SPEED_TIMEOUT,
        }

        try:
            import yaml
            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
        except ImportError:
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=2, ensure_ascii=False)

        return config_path

    def _parse_output(self, batch_id: int) -> List[str]:
        """解析 subs-check 的输出。

        预期输出格式（根据 subs-check 实际格式调整）：
          - 如果输出为 alive.txt 格式（每行一个 URI）
          - 或 YAML 配置格式

        Returns:
            存活节点 URI 列表
        """
        # 尝试读取 subs-check 输出的 alive 结果文件
        # 注意：subs-check 的实际输出格式需要根据其文档确认
        output_candidates = [
            f"alive_batch_{batch_id:04d}.txt",
            f"batch_{batch_id:04d}_alive.txt",
            "alive.txt",  # subs-check 可能输出到固定文件名
            "result.txt",
        ]

        for filename in output_candidates:
            if os.path.exists(filename):
                try:
                    with open(filename, "r", encoding="utf-8") as f:
                        lines = [line.strip() for line in f if line.strip()]
                    print(f"[{now_str()}] 批次 {batch_id} 从 {filename} "
                          f"读取 {len(lines)} 个存活节点", flush=True)
                    return lines
                except Exception as e:
                    print(f"[{now_str()}] 解析输出文件 {filename} 失败: {e}", flush=True)

        print(f"[{now_str()}] ⚠️ 批次 {batch_id} 未找到输出文件，"
              f"返回空结果", flush=True)
        return []

    def get_stats(self) -> dict:
        """返回当前测速统计信息。"""
        total_alive = sum(len(v) for v in self._results.values())
        return {
            "total_batches": self.total_batches,
            "completed": len(self._results),
            "failed": len(self._errors),
            "active": len(self._active),
            "total_alive": total_alive,
        }
