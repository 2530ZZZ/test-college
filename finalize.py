"""收尾去重兜底脚本：从 batches 分片生成 no/。

用途（08111）：GA job 超时/取消时，主流程 _finalize 的收尾日志进
devnull、git 步骤抢跑，no/ 产出可能丢失。workflow 的"提交结果"步骤
（if: always()）在 git add 前运行本脚本兜底——此时 runner 工作区
仍有本轮 batches/ 分片，与主流程共用同一去重函数
（collector.dedup_batches_write_no），产出完全一致。
（08141：7 天窗口 no_his 已回退，只做单次去重写 no/。）

用法：python finalize.py
"""
from collector import dedup_batches_write_no


def main():
    count = dedup_batches_write_no()
    if count:
        print(f"[finalize] 收尾去重完成: {count} 节点 -> no/ 已生成")
    else:
        print("[finalize] 无批次，保留旧 no/ 不动")


if __name__ == "__main__":
    main()
