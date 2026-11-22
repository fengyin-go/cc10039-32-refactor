"""湖内质量监控领域服务：配置数据湖内数据的完整性、时效性和准确性监控规则。

上游接收入湖管道完成事件，对入湖数据执行质量检查。
下游联动血缘图谱记录质量问题，再汇总到治理报告中。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.completion_receipt import completion_registry
from app.store import store


class QualityMonitorService(BaseModuleService):
    MODULE = "quality_monitor"
    ENTITY = "监控规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "监控指标", "告警阈值", "目标湖表", "检查频率"]
    REQUIRED_FIELDS = ["规则名称", "监控指标", "告警阈值"]
    STATUS_ORDER = ["待启用", "已启用", "已停用"]
    ACTION_RULES = {"启用监控": "已启用", "停用监控": "已停用"}
    NEGATIVE_ACTIONS = ["停用监控"]

    # 质量检查结果存储
    _check_results: list[dict[str, Any]] = []
    # 入湖完成回执入账结果：key=(任务模块, 任务id)，按目标湖表隔离
    _intake: dict[tuple[str, int], dict[str, Any]] = {}

    def accept_ingest_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """入湖管道完成回执入账（质量监控侧）。

        同一份回执重复投递只入账一次：已入账时原样返回既有结果，
        并把统一回执置为已确认，使入湖行数可在质量结果中核对。
        """
        key = (receipt["task_module"], receipt["task_id"])
        existing = self._intake.get(key)
        if existing is not None:
            return existing

        result = {
            "receipt_no": receipt["receipt_no"],
            "task_no": receipt["task_no"],
            "task_name": receipt["task_name"],
            "table_name": receipt["target_table"],
            "record_count": int(receipt["record_count"]),
            "booked_at": __import__("datetime").datetime.now().isoformat(),
        }
        self._intake[key] = result
        completion_registry.confirm(
            receipt["task_module"], receipt["task_id"], self.MODULE
        )
        return result

    def ingest_quality_for_table(self, table_name: str) -> dict[str, Any]:
        """按目标湖表汇总已入账的入湖完成数，质量结果按表隔离。"""
        booked = [
            item for item in self._intake.values()
            if item["table_name"] == table_name
        ]
        return {
            "table_name": table_name,
            "booked_receipt_count": len(booked),
            "booked_record_count": sum(int(item["record_count"]) for item in booked),
            "receipts": booked,
        }

    def enable_monitor(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启用监控规则：验证目标湖表存在。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "监控规则不存在"
        if entry.get("status") != "待启用":
            return None, f"监控规则状态为{entry.get('status')}，不可启用"

        table_name = entry.get("目标湖表", "")

        # 验证目标湖表已注册
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        if not lake_tables:
            return None, f"目标湖表 {table_name} 不存在"

        entry["status"] = "已启用"
        entry["pending"] = False
        entry["abnormal"] = False

        store.log_event("quality_rule_enabled", "quality_monitor", "lake_table", {
            "rule_id": entry_id,
            "table_name": table_name,
            "metric": entry.get("监控指标", ""),
        })

        return entry, f"监控规则 {entry.get('规则名称')} 已启用，指标={entry.get('监控指标')}"

    def check_quality(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """执行质量检查：对目标湖表数据执行指标检查，命中阈值则触发告警。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "监控规则不存在"
        if entry.get("status") != "已启用":
            return None, f"监控规则状态为{entry.get('status')}，不可执行检查"

        table_name = entry.get("目标湖表", "")
        metric = entry.get("监控指标", "")
        threshold = entry.get("告警阈值", "")

        # 模拟检查：统计目标湖表的数据行数
        lake_table_rows = store.find_by_field("lake_table", "表名", table_name)
        row_count = len(lake_table_rows)

        # 阈值比较
        try:
            threshold_value = int(threshold) if threshold.isdigit() else 0
        except (ValueError, AttributeError):
            threshold_value = 0

        passed = row_count >= threshold_value
        result = {
            "rule_id": entry_id,
            "rule_name": entry.get("规则名称"),
            "table_name": table_name,
            "metric": metric,
            "threshold": threshold,
            "actual_value": row_count,
            "passed": passed,
            "checked_at": __import__("datetime").datetime.now().isoformat(),
        }
        self._check_results.append(result)

        # 质量不通过时联动血缘和治理报告
        if not passed:
            store.log_event("quality_failed_to_lineage", "quality_monitor", "lineage_graph", {
                "rule_id": entry_id,
                "table_name": table_name,
                "metric": metric,
                "threshold": threshold,
                "actual": row_count,
            })

            # 更新血缘链路为待验证
            lineages = store.find_by_field("lineage_graph", "中间转换", table_name)
            if not lineages:
                lineages = store.find_by_field("lineage_graph", "下游消费", table_name)
            for lin in lineages:
                lin["status"] = "待验证"
                lin["abnormal"] = True
        else:
            store.log_event("quality_passed_to_govern", "quality_monitor", "govern_report", {
                "rule_id": entry_id,
                "table_name": table_name,
                "metric": metric,
                "actual": row_count,
            })

        return entry, (
            f"质量检查完成：{metric}={row_count}，阈值={threshold}，"
            + ("通过" if passed else "不通过")
        )

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启用监控":
            return self.enable_monitor(entry_id)
        return super().run_action(entry_id, action)