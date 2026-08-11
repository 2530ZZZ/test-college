"""
日志汇聚器（模块级单例）— 所有模块统一输出出口。

解决：多线程直接 print 挤行（无锁）vs 全局锁阻塞生产（有锁）的矛盾。
- 无挤行：单消费者线程打印，天然有序
- 不阻塞：队列满时丢弃（日志不拖慢生产）
- 全模块统一：collector / http_client / quota_manager / 监控线程 都用它

双队列（08111 监控盲区根因）：普通日志满时被丢弃没问题，但监控块
也被挤丢 → 21:02 后 30 分钟零监控。现在监控块走高优先级队列，
消费者先打印高优先级，高优先级满时清空普通队列保底——刷屏再严重
监控块也保证输出。

用法：
    from log_sink import log_sink
    log_sink.emit(f"[{now_str()}] {msg}")            # 普通日志
    log_sink.emit_priority(f"[{now_str()}] {block}") # 高优先级（监控块）
"""

import threading
from queue import Queue, Full as QueueFull, Empty as QueueEmpty


class LogSink:
    """日志汇聚器：Worker 只做 put_nowait（不阻塞），单消费者线程唯一打印。

    双队列：_q 普通日志（满丢弃）；_hi_q 高优先级（监控块，优先打印）。
    """

    def __init__(self, maxsize: int = 20000, hi_maxsize: int = 1000):
        self._q = Queue(maxsize=maxsize)
        self._hi_q = Queue(maxsize=hi_maxsize)
        threading.Thread(target=self._run, name="LogSink", daemon=True).start()

    def emit(self, msg: str):
        try:
            self._q.put_nowait(msg)
        except QueueFull:
            pass  # 日志队列满 → 丢弃（高水位保护）

    def emit_priority(self, msg: str):
        """高优先级日志（监控块）：优先打印，不被刷屏挤丢。

        hi 队列满时：先清空普通队列（丢低优先级日志保底），再入队。
        """
        try:
            self._hi_q.put_nowait(msg)
        except QueueFull:
            try:
                while True:
                    self._q.get_nowait()
            except QueueEmpty:
                pass
            try:
                self._hi_q.put_nowait(msg)
            except QueueFull:
                pass  # 极端情况仍满 → 丢弃（不影响主流程）

    def flush(self):
        """同步等待队列清空（收尾时用，确保日志完整输出）。"""
        self._q.join()

    def qsize(self) -> int:
        """当前队列待打印日志数（监控健康检查）。"""
        return self._q.qsize()

    def consumer_alive(self) -> bool:
        """LogSink 消费者线程是否存活（监控健康检查）。"""
        for t in threading.enumerate():
            if t.name == "LogSink" and t.is_alive():
                return True
        return False

    def _run(self):
        while True:
            # 高优先级优先：监控块不被普通日志挤掉。
            # _q.get(timeout) 而非阻塞 get：普通队列空时消费者不长时间
            # 挂起，最多 0.5s 就重新检查 hi_q（否则 hi_q 的监控块会被
            # 空普通队列"优先级反转"延迟）。
            try:
                msg = self._hi_q.get_nowait()
            except QueueEmpty:
                try:
                    msg = self._q.get(timeout=0.5)
                except Exception:
                    continue
                self._q.task_done()
            print(msg, flush=True)


# 模块级单例（所有模块共享）
log_sink = LogSink()
