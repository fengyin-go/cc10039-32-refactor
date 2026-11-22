"""入湖管道领域服务：管理数据从来源系统加载到数据湖的管道配置与执行。

上游与湖表注册联动，下游为质量监控提供入湖数据用于规则检查。
支持管道状态追踪和失败恢复。

任务完成时不再各自登记处理记录数，而是统一登记“任务完成数据回执”
（completion_receipt），完成状态先持久化、回执交质量监控确认入账；
重发完工请求复用同一回执，不重复投递事件、不重复入账。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.completion_receipt import completion_registry
from app.services.quality_monitor import QualityMonitorService
from app.store import store


class IngestPipelineService(BaseModuleService):
    MODULE = "ingest_pipeline"
    ENTITY = "入湖任务"
    OPERATION_TERM = "入湖"
    LIST_FIELDS = ["任务名称", "来源系统", "目标湖表", "数据格式", "执行状态"]
    REQUIRED_FIELDS = ["任务名称", "来源系统", "目标湖表"]
    STATUS_ORDER = ["待启动", "运行中", "已完成", "运行失败"]
    ACTION_RULES = {"启动任务": "运行中", "取消任务": "运行失败"}
    NEGATIVE_ACTIONS = []

    def start_pipeline(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启动入湖管道：验证目标湖表已注册后启动。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "入湖任务不存在"
        if entry.get("status") != "待启动":
            return None, f"入湖任务状态为{entry.get('status')}，不可启动"

        table_name = entry.get("目标湖表", "")
        source = entry.get("来源系统", "")
        data_format = entry.get("数据格式", "")

        # 验证目标湖表已注册
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        if not lake_tables or not any(lt.get("status") == "已注册" for lt in lake_tables):
            return None, f"目标湖表 {table_name} 尚未注册或已下线，不可启动管道"

        # 自身状态流转
        entry["status"] = "运行中"
        entry["pending"] = True
        entry["abnormal"] = False

        store.log_event("pipeline_started", "ingest_pipeline", "lake_table", {
            "pipeline_id": entry_id,
            "table_name": table_name,
            "source": source,
            "data_format": data_format,
        })

        return entry, f"入湖管道 {entry.get('任务名称')} 已启动，来源={source}，目标={table_name}"

    def _persist_completion(self, entry: dict[str, Any], record_count: int) -> dict[str, Any]:
        """完成状态持久化 + 统一回执登记（不触发任何下游事件）。"""
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = False

        receipt, _ = completion_registry.register(
            task_module=self.MODULE,
            task_id=int(entry["id"]),
            task_no=str(entry["id"]),
            task_name=entry.get("任务名称", ""),
            target_table=entry.get("目标湖表", ""),
            record_count=record_count,
            status=entry["status"],
        )
        return receipt

    def _deliver_to_quality(self, entry: dict[str, Any], receipt: dict[str, Any],
                            record_count: int) -> None:
        """把任务完成数据回执交给质量监控（仅首次完工投递一次）。"""
        table_name = receipt["target_table"]

        # 通知质量监控模块检查入湖数据
        quality_rules = store.find_by_field("quality_monitor", "目标湖表", table_name)
        if quality_rules:
            for qr in quality_rules:
                if qr.get("status") == "已启用":
                    qr["pending"] = True
            store.log_event("pipeline_to_quality", "ingest_pipeline", "quality_monitor", {
                "pipeline_id": entry["id"],
                "table_name": table_name,
                "record_count": record_count,
                "quality_rules_triggered": len(quality_rules),
                "receipt_no": receipt["receipt_no"],
            })

        # 质量监控按回执登记可入账的入湖行数（幂等，重复投递只入账一次）
        QualityMonitorService().accept_ingest_receipt(receipt)

        # 更新湖表状态
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        for lt in lake_tables:
            lt["pending"] = True

        completion_registry.mark_delivered(receipt)

    def complete_pipeline(self, entry_id: int, record_count: int) -> tuple[dict[str, Any] | None, str]:
        """完成入湖管道：登记统一回执并联动质量监控进行数据检查。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "入湖任务不存在"
        if entry.get("status") != "运行中":
            return None, f"入湖任务状态为{entry.get('status')}，不可完成"

        receipt = self._persist_completion(entry, record_count)
        self._deliver_to_quality(entry, receipt, record_count)

        return entry, (
            f"入湖管道 {entry.get('任务名称')} 已完成，入湖 {record_count} 条记录，"
            f"回执 {receipt['receipt_no']} 已通知质量监控"
        )

    def persist_pipeline_completion(self, entry_id: int, record_count: int) -> tuple[dict[str, Any] | None, str]:
        """只持久化完成状态并登记回执，不投递下游（模拟完工请求被打断）。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "入湖任务不存在"
        if entry.get("status") != "运行中":
            return None, f"入湖任务状态为{entry.get('status')}，不可完成"

        receipt = self._persist_completion(entry, record_count)
        return entry, (
            f"入湖管道 {entry.get('任务名称')} 完成状态已持久化，回执 {receipt['receipt_no']} "
            "待质量监控确认（投递前停住）"
        )

    def redeliver_completion(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """重发完工请求：复用既有回执；已确认则不再重复投递与入账。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "入湖任务不存在"
        receipt = completion_registry.find_task(self.MODULE, entry_id)
        if receipt is None:
            return None, "入湖任务尚未登记完成回执，无法重发完工"

        completion_registry.mark_redelivery(receipt)
        if receipt.get("delivered"):
            return entry, (
                f"入湖管道 {entry.get('任务名称')} 完工请求已重发（第 {receipt['delivery_count']} 次投递），"
                f"回执 {receipt['receipt_no']} 已投递，跳过重复事件"
                + ("（已确认入账）" if receipt["confirmed"] else "（待质量监控确认）")
            )

        self._deliver_to_quality(entry, receipt, receipt["record_count"])
        return entry, (
            f"入湖管道 {entry.get('任务名称')} 完工请求已重发（第 {receipt['delivery_count']} 次投递），"
            f"回执 {receipt['receipt_no']} 已补交给质量监控"
        )

    def fail_pipeline(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """管道失败：保留状态用于重试恢复。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "入湖任务不存在"
        if entry.get("status") != "运行中":
            return None, f"入湖任务状态为{entry.get('status')}，不可标记失败"

        entry["status"] = "运行失败"
        entry["pending"] = False
        entry["abnormal"] = True

        store.log_event("pipeline_failed", "ingest_pipeline", "task_scheduler", {
            "pipeline_id": entry_id,
            "table_name": entry.get("目标湖表", ""),
        })

        return entry, f"入湖管道 {entry.get('任务名称')} 运行失败"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启动任务":
            return self.start_pipeline(entry_id)
        return super().run_action(entry_id, action)
