"""任务完成数据回执登记中心（入湖管道与回填任务共用）。

把原先散落在入湖管道、回填任务两条链路里的“已处理记录数登记 + 完成回执
构造”收拢为统一的 CompletionReceipt，供质量监控与治理报告按目标湖表统一
核对、入账。

设计要点：
- 登记幂等：同一 (task_module, task_id) 的完工请求只产生一份回执，
  重复投递只累加 delivery_count，不重复登记、不重复入账；
- 状态与确认分离：任务完成状态先持久化，回执 confirmed 由下游消费方
  （质量监控 / 治理报告）入账后置位。状态已更新但回执尚未确认期间处理被
  打断时，重发完工请求不会重复投递事件；下游读取 pending_confirmation()
  即可知道哪些完成数“已经可以入账”；
- 湖表隔离：全部计量按 目标湖表 + 任务类型 归集，入湖与回填的行数不会
  因为共用登记逻辑串到另一张表。
"""
from __future__ import annotations

import datetime
from typing import Any

# 任务类型 → 默认下游消费模块（入湖交质量监控，回填交治理报告）
RECEIPT_KINDS: dict[str, str] = {
    "ingest_pipeline": "quality_monitor",
    "backfill_job": "govern_report",
}


class CompletionReceiptRegistry:
    """进程内统一回执登记表（与内存 store 同级的单例）。"""

    def __init__(self) -> None:
        self._receipts: list[dict[str, Any]] = []
        self._seq = 0

    def reset(self) -> None:
        """清空登记记录（仅用于测试与演示脚本的场景隔离）。"""
        self._receipts.clear()
        self._seq = 0

    # ---- 登记 / 重发 ----

    def register(
        self,
        *,
        task_module: str,
        task_id: int,
        task_no: str,
        task_name: str,
        target_table: str,
        record_count: int,
        status: str,
    ) -> tuple[dict[str, Any], bool]:
        """登记任务完成数据回执；同一任务已登记时原样返回，不重复登记。

        返回 ``(回执, 是否本次新建)``。
        """
        existing = self.find_task(task_module, task_id)
        if existing is not None:
            return existing, False

        self._seq += 1
        receipt = {
            "receipt_no": f"CR-{task_module.upper()}-{self._seq:04d}",
            "task_module": task_module,
            "task_id": task_id,
            "task_no": task_no,
            "task_name": task_name,
            "target_table": target_table,
            "record_count": int(record_count),
            "status": status,
            "consumer_module": RECEIPT_KINDS.get(task_module, ""),
            "registered_at": datetime.datetime.now().isoformat(),
            "delivered": False,
            "delivered_at": None,
            "confirmed": False,
            "confirmed_at": None,
            "confirmed_by": None,
            "confirmations": 0,
            "delivery_count": 1,
        }
        self._receipts.append(receipt)
        return receipt, True

    def mark_redelivery(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """记录一次完工请求重发：不产生新回执、不重复入账。"""
        receipt["delivery_count"] = int(receipt.get("delivery_count", 1)) + 1
        return receipt

    def mark_delivered(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """标记回执已向下游投递；投递只发生一次，重发不重复产生事件。"""
        if not receipt.get("delivered"):
            receipt["delivered"] = True
            receipt["delivered_at"] = datetime.datetime.now().isoformat()
        return receipt

    # ---- 查询 ----

    def all(
        self,
        *,
        task_module: str | None = None,
        target_table: str | None = None,
        confirmed: bool | None = None,
    ) -> list[dict[str, Any]]:
        """回执明细分页前的全量筛选入口。"""
        result = list(self._receipts)
        if task_module is not None:
            result = [r for r in result if r["task_module"] == task_module]
        if target_table is not None:
            result = [r for r in result if r["target_table"] == target_table]
        if confirmed is not None:
            result = [r for r in result if bool(r["confirmed"]) is confirmed]
        return result

    def find_task(self, task_module: str, task_id: int) -> dict[str, Any] | None:
        """按原任务号（模块 + 任务 id）查既有完成明细。"""
        for receipt in self._receipts:
            if receipt["task_module"] == task_module and receipt["task_id"] == task_id:
                return receipt
        return None

    def find_by_task_no(
        self, task_no: str, task_module: str | None = None
    ) -> list[dict[str, Any]]:
        """按业务任务编号查回执（外部只持有原任务号时的核对入口）。

        不同任务类型的原任务号可能重号（如入湖任务 4 与回填任务 4），
        传入 task_module 可限定到具体任务类型；唯一键始终是
        (task_module, task_id)，用 find_task 精确查询。
        """
        return [
            r for r in self._receipts
            if r["task_no"] == str(task_no)
            and (task_module is None or r["task_module"] == task_module)
        ]

    def find_by_task_name(self, task_name: str) -> list[dict[str, Any]]:
        """按任务名称查回执，便于与界面上的任务记录对照。"""
        return [r for r in self._receipts if r["task_name"] == task_name]

    def pending_confirmation(self, consumer_module: str | None = None) -> list[dict[str, Any]]:
        """返回完成状态已持久化、但下游尚未确认入账的回执。

        下游（质量监控 / 治理报告）据此知道哪些完成数已经可以入账。
        """
        return [
            r for r in self._receipts
            if not r["confirmed"]
            and (consumer_module is None or r["consumer_module"] == consumer_module)
        ]

    # ---- 下游确认 ----

    def confirm(
        self, task_module: str, task_id: int, consumer_module: str
    ) -> dict[str, Any] | None:
        """下游消费方入账成功后置确认位；重复确认只计数、不重复入账。"""
        receipt = self.find_task(task_module, task_id)
        if receipt is None:
            return None
        receipt["confirmations"] += 1
        if not receipt["confirmed"]:
            receipt["confirmed"] = True
            receipt["confirmed_at"] = datetime.datetime.now().isoformat()
            receipt["confirmed_by"] = consumer_module
        return receipt

    # ---- 计量（按湖表隔离） ----

    def table_metrics(self, target_table: str, task_module: str) -> dict[str, Any]:
        """返回某张湖表上某类任务的完成计量。

        已确认（可入账）行数与待确认行数分开统计，且只覆盖本表本类任务。
        """
        receipts = [
            r for r in self._receipts
            if r["target_table"] == target_table and r["task_module"] == task_module
        ]
        return {
            "target_table": target_table,
            "task_module": task_module,
            "receipt_count": len(receipts),
            "confirmed_receipt_count": sum(1 for r in receipts if r["confirmed"]),
            "record_count": sum(int(r["record_count"]) for r in receipts if r["confirmed"]),
            "unconfirmed_record_count": sum(
                int(r["record_count"]) for r in receipts if not r["confirmed"]
            ),
        }

    def metrics_by_table(self, task_module: str) -> dict[str, dict[str, Any]]:
        """按湖表汇总某类任务的计量，表与表之间互不串数。"""
        tables = sorted({
            r["target_table"]
            for r in self._receipts
            if r["task_module"] == task_module
        })
        return {table: self.table_metrics(table, task_module) for table in tables}


# 统一任务完成数据回执登记单例
completion_registry = CompletionReceiptRegistry()
