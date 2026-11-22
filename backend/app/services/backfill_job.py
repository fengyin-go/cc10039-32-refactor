"""数据回填任务领域服务：管理历史数据回填、补数和断点续做。

支持回填检查点（checkpoint）：中断后可从上次位置继续，不会重复处理已完成的记录。
上游接收 Schema 演进的不兼容变更事件，下游联动治理报告记录回填结果。

完工处理与入湖管道共用“任务完成数据回执”登记（completion_receipt）：
完成状态先持久化、回执交治理报告确认；重发完工复用同一回执，不重复投递
事件、不重复入账，计量按目标湖表隔离。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.completion_receipt import completion_registry
from app.store import store


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "回填任务"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["任务名称", "目标湖表", "回填时间范围", "处理记录数", "执行状态"]
    REQUIRED_FIELDS = ["任务名称", "目标湖表", "回填时间范围"]
    STATUS_ORDER = ["待调度", "回填中", "已完成", "回填失败"]
    ACTION_RULES = {"启动回填": "回填中", "取消回填": "回填失败"}
    NEGATIVE_ACTIONS = []

    # 回填检查点：任务 id → 已处理记录偏移
    _checkpoints: dict[int, int] = {}

    def start_backfill(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启动回填任务：检查 Schema 变更状态后启动。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "待调度":
            return None, f"回填任务状态为{entry.get('status')}，不可启动"

        table_name = entry.get("目标湖表", "")
        time_range = entry.get("回填时间范围", "")

        # 检查同湖表是否有执行中的回填
        active_backfills = [
            bj for bj in store.rows(self.MODULE)
            if bj.get("目标湖表") == table_name
            and bj.get("status") == "回填中"
            and bj.get("id") != entry_id
        ]
        if active_backfills:
            return None, f"湖表 {table_name} 已有回填任务在执行中"

        # 初始化检查点
        checkpoint = self._checkpoints.get(entry_id, 0)

        entry["status"] = "回填中"
        entry["pending"] = True
        entry["abnormal"] = False
        entry["处理记录数"] = str(checkpoint)

        store.log_event("backfill_started", "backfill_job", "backfill_job", {
            "task_id": entry_id,
            "table_name": table_name,
            "time_range": time_range,
            "checkpoint": checkpoint,
        })

        return entry, f"回填任务 {entry.get('任务名称')} 已启动，时间范围={time_range}，检查点从 {checkpoint} 开始"

    def checkpoint_backfill(self, entry_id: int, processed_count: int) -> tuple[dict[str, Any] | None, str]:
        """更新回填检查点，支持断点续做。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "回填中":
            return None, f"回填任务状态为{entry.get('status')}，不可保存检查点"

        self._checkpoints[entry_id] = processed_count
        entry["处理记录数"] = str(processed_count)

        store.log_event("backfill_checkpoint", "backfill_job", "backfill_job", {
            "task_id": entry_id,
            "checkpoint": processed_count,
        })

        return entry, f"回填任务检查点已更新：已处理 {processed_count} 条记录"

    def _persist_completion(self, entry: dict[str, Any], total_records: int) -> dict[str, Any]:
        """完成状态持久化 + 统一回执登记（不投递治理报告事件）。"""
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["处理记录数"] = str(total_records)

        # 清理检查点
        self._checkpoints.pop(int(entry["id"]), None)

        receipt, _ = completion_registry.register(
            task_module=self.MODULE,
            task_id=int(entry["id"]),
            task_no=str(entry["id"]),
            task_name=entry.get("任务名称", ""),
            target_table=entry.get("目标湖表", ""),
            record_count=total_records,
            status=entry["status"],
        )
        return receipt

    def _deliver_to_govern(self, entry: dict[str, Any], receipt: dict[str, Any],
                           total_records: int) -> None:
        """把任务完成数据回执交给治理报告（仅首次完工投递一次）。"""
        store.log_event("backfill_completed_to_govern", "backfill_job", "govern_report", {
            "task_id": entry["id"],
            "table_name": receipt["target_table"],
            "total_records": total_records,
            "receipt_no": receipt["receipt_no"],
        })
        completion_registry.mark_delivered(receipt)

    def complete_backfill(self, entry_id: int, total_records: int) -> tuple[dict[str, Any] | None, str]:
        """完成回填任务，登记统一回执并通知治理报告。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "回填中":
            return None, f"回填任务状态为{entry.get('status')}，不可完成"

        receipt = self._persist_completion(entry, total_records)
        self._deliver_to_govern(entry, receipt, total_records)

        return entry, (
            f"回填任务 {entry.get('任务名称')} 已完成，总计 {total_records} 条记录，"
            f"回执 {receipt['receipt_no']} 已通知治理报告"
        )

    def persist_backfill_completion(self, entry_id: int, total_records: int) -> tuple[dict[str, Any] | None, str]:
        """只持久化完成状态并登记回执，不投递治理报告（模拟完工请求被打断）。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "回填中":
            return None, f"回填任务状态为{entry.get('status')}，不可完成"

        receipt = self._persist_completion(entry, total_records)
        return entry, (
            f"回填任务 {entry.get('任务名称')} 完成状态已持久化，回执 {receipt['receipt_no']} "
            "待治理报告确认（投递前停住）"
        )

    def redeliver_completion(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """重发完工请求：复用既有回执；已确认则不再重复投递与入账。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        receipt = completion_registry.find_task(self.MODULE, entry_id)
        if receipt is None:
            return None, "回填任务尚未登记完成回执，无法重发完工"

        completion_registry.mark_redelivery(receipt)
        if receipt.get("delivered"):
            return entry, (
                f"回填任务 {entry.get('任务名称')} 完工请求已重发（第 {receipt['delivery_count']} 次投递），"
                f"回执 {receipt['receipt_no']} 已投递，跳过重复事件"
                + ("（已确认入账）" if receipt["confirmed"] else "（待治理报告确认）")
            )

        self._deliver_to_govern(entry, receipt, receipt["record_count"])
        return entry, (
            f"回填任务 {entry.get('任务名称')} 完工请求已重发（第 {receipt['delivery_count']} 次投递），"
            f"回执 {receipt['receipt_no']} 已补交给治理报告"
        )

    def fail_backfill(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """回填失败：保留检查点以便恢复。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "回填中":
            return None, f"回填任务状态为{entry.get('status')}，不可标记失败"

        checkpoint = self._checkpoints.get(entry_id, 0)

        entry["status"] = "回填失败"
        entry["pending"] = False
        entry["abnormal"] = True

        store.log_event("backfill_failed", "backfill_job", "retention_mgr", {
            "task_id": entry_id,
            "failed_at_checkpoint": checkpoint,
        })

        return entry, f"回填任务失败，检查点保留在 {checkpoint} 条，可从该位置恢复"

    def get_checkpoint(self, entry_id: int) -> int | None:
        """获取回填检查点。"""
        return self._checkpoints.get(entry_id)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启动回填":
            return self.start_backfill(entry_id)
        return super().run_action(entry_id, action)
