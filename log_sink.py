"""
日志汇聚器（模块级单例）— 所有模块统一输出出口。

解决：多线程直接 print 挤行（无锁）vs 全局锁阻塞生产（有锁）的矛盾。
- 无挤行：单消费者线程打印，天然有序
- 不阻塞：队列满时丢弃（日志不拖慢生产）
- 全模块统一：collector / http_client / quota_manager / 监控线程 都用它

用法：
    from log_sink import log_sink
    log_sink.emit(f"[{now_str()}] {msg}")
"""

import threading
from queue import Queue, Full as QueueFull


class LogSink:
    """日志汇聚器：Worker 只做 put_nowait（不阻塞），单消费者线程唯一打印。"""

    def __init__(self, maxsize: int = 20000):
        self._q = Queue(maxsize=maxsize)
        threading.Thread(target=self._run, name="LogSink", daemon=True).start()

    def emit(self, msg: str):
        try:
            self._q.put_nowait(msg)
        except QueueFull:
            pass  # 日志队列满 → 丢弃（高水位保护）

    def flush(self):
        """同步等待队列清空（收尾时用，确保日志完整输出）。"""
        self._q.join()

    def _run(self):
        while True:
            msg = self._q.get()
            print(msg, flush=True)
            self._q.task_done()


# 模块级单例（所有模块共享）
log_sink = LogSink()
