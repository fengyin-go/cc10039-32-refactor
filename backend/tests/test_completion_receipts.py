"""任务完成数据回执统一登记的专项测试。

覆盖：
- 入湖 120 行 / 回填 76 行的统一回执登记与下游确认；
- 完成状态已持久化、回执未确认期间打断，重发完工不重复投递、不重复入账；
- 任务记录条数与消费者收到数量一致，重复投递只入账一次；
- 按原任务号可查既有完成明细；
- 入湖与回填计量按目标湖表隔离，不串表。
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.store import store
from app.services.completion_receipt import completion_registry
from app.services.ingest_pipeline import IngestPipelineService
from app.services.backfill_job import BackfillJobService
from app.services.quality_monitor import QualityMonitorService
from app.services.govern_report import GovernReportService
from app.services.lake_table import LakeTableService


@pytest.fixture(autouse=True)
def _isolated_registry():
    """每个用例前清空统一回执与质量入账，避免单例跨用例污染。"""
    completion_registry.reset()
    QualityMonitorService._intake.clear()
    yield


def _register_lake_table(name: str) -> int:
    entry, missing = LakeTableService().create_entry({
        "表名": name,
        "所属业务域": "销售域",
        "存储格式": "parquet",
    })
    assert entry is not None and not missing
    LakeTableService().register_table(entry["id"])
    return entry["id"]


def _create_enabled_quality_rule(table: str, name: str) -> int:
    svc = QualityMonitorService()
    entry, missing = svc.create_entry({
        "规则名称": name,
        "监控指标": "入湖行数",
        "告警阈值": "1",
        "目标湖表": table,
        "检查频率": "每次入湖",
    })
    assert entry is not None and not missing
    svc.enable_monitor(entry["id"])
    return entry["id"]


def _create_pipeline(table: str, name: str) -> int:
    entry, missing = IngestPipelineService().create_entry({
        "任务名称": name,
        "来源系统": "销售系统",
        "目标湖表": table,
        "数据格式": "json",
    })
    assert entry is not None and not missing
    return entry["id"]


def _create_backfill(table: str, name: str, time_range: str = "2026-10-01~2026-10-31") -> int:
    entry, missing = BackfillJobService().create_entry({
        "任务名称": name,
        "目标湖表": table,
        "回填时间范围": time_range,
    })
    assert entry is not None and not missing
    return entry["id"]


def _create_govern_report(period: str) -> int:
    entry, missing = GovernReportService().create_entry({
        "报告周期": period,
        "治理指标": "回填完成量",
        "优化建议": "无",
    })
    assert entry is not None and not missing
    return entry["id"]


class TestIngestReceipt:
    """入湖管道 → 质量监控。"""

    def test_ingest_120_rows_registered_and_confirmed(self):
        svc = IngestPipelineService()
        quality = QualityMonitorService()
        table = "销量湖表-入湖120"
        _register_lake_table(table)
        _create_enabled_quality_rule(table, "销量入湖质量规则")
        pid = _create_pipeline(table, "销量入湖任务")

        svc.start_pipeline(pid)
        entry, msg = svc.complete_pipeline(pid, 120)

        assert entry is not None and entry["status"] == "已完成"
        receipt = completion_registry.find_task("ingest_pipeline", pid)
        assert receipt is not None
        assert receipt["record_count"] == 120
        assert receipt["target_table"] == table
        assert receipt["consumer_module"] == "quality_monitor"
        assert receipt["confirmed"] is True
        assert receipt["confirmed_by"] == "quality_monitor"

        # 质量监控按表收到的数量与任务记录一致
        booked = quality.ingest_quality_for_table(table)
        assert booked["booked_receipt_count"] == 1
        assert booked["booked_record_count"] == 120

        # 对外事件输入输出不变，且只投递一次
        events = store.events_between("ingest_pipeline", "quality_monitor")
        assert events and events[-1]["payload"]["record_count"] == 120
        assert "120" in msg

    def test_redeliver_completed_ingest_books_once(self):
        svc = IngestPipelineService()
        quality = QualityMonitorService()
        table = "销量湖表-重投入湖"
        _register_lake_table(table)
        _create_enabled_quality_rule(table, "重投质量规则")
        pid = _create_pipeline(table, "重投入湖任务")
        svc.start_pipeline(pid)
        svc.complete_pipeline(pid, 120)

        events_before = len(store.events_between("ingest_pipeline", "quality_monitor"))
        entry, msg = svc.redeliver_completion(pid)

        assert entry is not None
        # 已确认回执：重发不再产生新事件
        assert len(store.events_between("ingest_pipeline", "quality_monitor")) == events_before
        booked = quality.ingest_quality_for_table(table)
        assert booked["booked_receipt_count"] == 1
        assert booked["booked_record_count"] == 120
        receipt = completion_registry.find_task("ingest_pipeline", pid)
        assert receipt["delivery_count"] == 2
        assert receipt["confirmations"] == 1
        assert "跳过重复事件" in msg

    def test_interrupted_ingest_redelivered_to_quality(self):
        """完成状态持久化后、回执确认前打断，重发后质量监控才能入账。"""
        svc = IngestPipelineService()
        quality = QualityMonitorService()
        table = "销量湖表-打断入湖"
        _register_lake_table(table)
        _create_enabled_quality_rule(table, "打断质量规则")
        pid = _create_pipeline(table, "打断入湖任务")
        svc.start_pipeline(pid)

        # 阶段一：只持久化完成状态，投递前停住
        entry, _ = svc.persist_pipeline_completion(pid, 120)
        assert entry["status"] == "已完成"
        receipt = completion_registry.find_task("ingest_pipeline", pid)
        assert receipt is not None and receipt["confirmed"] is False
        pending = completion_registry.pending_confirmation("quality_monitor")
        assert any(r["task_id"] == pid for r in pending)
        assert quality.ingest_quality_for_table(table)["booked_record_count"] == 0

        # 阶段二：重发完工，质量监控补入账
        svc.redeliver_completion(pid)
        receipt = completion_registry.find_task("ingest_pipeline", pid)
        assert receipt["confirmed"] is True
        assert quality.ingest_quality_for_table(table)["booked_record_count"] == 120
        events = store.events_between("ingest_pipeline", "quality_monitor")
        assert len([e for e in events if e["payload"].get("pipeline_id") == pid]) == 1


class TestBackfillReceipt:
    """回填任务 → 治理报告。"""

    def test_backfill_76_rows_flow_and_report(self):
        svc = BackfillJobService()
        table = "库存湖表-回填76"
        _register_lake_table(table)
        bid = _create_backfill(table, "库存回填任务")
        svc.start_backfill(bid)
        entry, msg = svc.complete_backfill(bid, 76)

        assert entry is not None and entry["status"] == "已完成"
        # 原有数量字段保持兼容，且与回执、消费者数量一致
        assert entry["处理记录数"] == "76"
        receipt = completion_registry.find_task("backfill_job", bid)
        assert receipt is not None and receipt["record_count"] == 76
        assert receipt["consumer_module"] == "govern_report"
        # 报告尚未生成，回执处于“可入账但未确认”
        assert receipt["confirmed"] is False
        assert any(
            r["task_id"] == bid
            for r in completion_registry.pending_confirmation("govern_report")
        )

        # 治理报告生成时入账并确认
        rid = _create_govern_report("2026-W47-回填76")
        report, _ = GovernReportService().generate_report(rid)
        summary = report["report_data"]["backfill_summary"]
        assert summary["total_backfill_records"] == 76
        assert summary["confirmed_receipts"] >= 1
        assert report["report_data"]["backfill_summary"]["by_table"][table]["record_count"] == 76
        assert completion_registry.find_task("backfill_job", bid)["confirmed"] is True
        assert "76" in msg

    def test_redeliver_confirmed_backfill_books_once(self):
        svc = BackfillJobService()
        table = "库存湖表-重投回填"
        _register_lake_table(table)
        bid = _create_backfill(table, "重投回填任务")
        svc.start_backfill(bid)
        svc.complete_backfill(bid, 76)
        rid = _create_govern_report("2026-W47-重投回填")
        GovernReportService().generate_report(rid)

        events_before = len(
            [e for e in store.events_between("backfill_job", "govern_report")
             if e["payload"].get("task_id") == bid]
        )
        entry, msg = svc.redeliver_completion(bid)
        assert entry is not None and "跳过重复事件" in msg
        events_after = [
            e for e in store.events_between("backfill_job", "govern_report")
            if e["payload"].get("task_id") == bid
        ]
        assert len(events_after) == events_before
        receipt = completion_registry.find_task("backfill_job", bid)
        assert receipt["delivery_count"] == 2
        assert receipt["confirmations"] == 1
        # 报告合计不变
        metrics = completion_registry.table_metrics(table, "backfill_job")
        assert metrics["record_count"] == 76 and metrics["receipt_count"] == 1

    def test_interrupted_backfill_report_totals_before_after_recovery(self):
        """下一笔回填在完成状态持久化后停住，对照恢复前后报告合计。"""
        svc = BackfillJobService()
        govern = GovernReportService()
        table = "库存湖表-打断回填"
        _register_lake_table(table)

        # 第一笔回填 76 行并出报告（恢复前基线）
        bid1 = _create_backfill(table, "库存回填任务-第一笔")
        svc.start_backfill(bid1)
        svc.complete_backfill(bid1, 76)
        rid1 = _create_govern_report("2026-W47-基线")
        report_before, _ = govern.generate_report(rid1)
        total_before = report_before["report_data"]["backfill_summary"]["total_backfill_records"]
        assert total_before == 76

        # 下一笔回填：完成状态持久化后停住（未投递治理报告）
        bid2 = _create_backfill(table, "库存回填任务-第二笔", "2026-09-01~2026-09-15")
        svc.start_backfill(bid2)
        entry, _ = svc.persist_backfill_completion(bid2, 40)
        assert entry["status"] == "已完成"
        receipt2 = completion_registry.find_task("backfill_job", bid2)
        assert receipt2 is not None and receipt2["confirmed"] is False
        # 打断期间没有投递治理事件
        assert not [
            e for e in store.events_between("backfill_job", "govern_report")
            if e["payload"].get("task_id") == bid2
        ]
        # 治理读取能看到这笔“已可入账”的完成数
        pending = completion_registry.pending_confirmation("govern_report")
        assert any(r["task_id"] == bid2 and r["record_count"] == 40 for r in pending)

        # 重发完工
        _, msg = svc.redeliver_completion(bid2)
        assert "补交给治理报告" in msg
        assert len([
            e for e in store.events_between("backfill_job", "govern_report")
            if e["payload"].get("task_id") == bid2
        ]) == 1

        # 恢复后重新出报告：合计 = 76 + 40，且第二笔只入账一次
        rid2 = _create_govern_report("2026-W47-恢复后")
        report_after, _ = govern.generate_report(rid2)
        summary = report_after["report_data"]["backfill_summary"]
        assert summary["total_backfill_records"] == 116
        assert summary["by_table"][table]["record_count"] == 116
        # 再重发一次也不会改变合计
        svc.redeliver_completion(bid2)
        rid3 = _create_govern_report("2026-W47-复核")
        report_check, _ = govern.generate_report(rid3)
        assert report_check["report_data"]["backfill_summary"]["total_backfill_records"] == 116


class TestReceiptLookupAndIsolation:
    """原任务号可查与湖表计量隔离。"""

    def test_find_receipt_by_original_task_no(self):
        table = "湖表-任务号查询"
        _register_lake_table(table)
        bid = _create_backfill(table, "按任务号查询的回填")
        BackfillJobService().start_backfill(bid)
        BackfillJobService().complete_backfill(bid, 30)

        # 原任务号 + 任务类型唯一定位
        found = completion_registry.find_by_task_no(str(bid), "backfill_job")
        assert len(found) == 1 and found[0]["task_id"] == bid
        assert found[0]["record_count"] == 30

        # 精确唯一键（模块, 任务id）始终可用
        assert completion_registry.find_task("backfill_job", bid)["record_count"] == 30

        # 不同任务类型可能重号：不限定类型时按类型区分，不会互相覆盖
        pid = _create_pipeline(table, "按任务号查询的入湖")
        IngestPipelineService().start_pipeline(pid)
        IngestPipelineService().complete_pipeline(pid, 20)
        same_id = str(bid) if bid == pid else None
        if same_id is not None:
            cross_kind = completion_registry.find_by_task_no(same_id)
            assert {r["task_module"] for r in cross_kind} == {
                "backfill_job", "ingest_pipeline"}

    def test_ingest_and_backfill_metrics_do_not_cross_tables(self):
        ingest_svc = IngestPipelineService()
        backfill_svc = BackfillJobService()
        quality = QualityMonitorService()

        sales_table = "销量湖表-隔离"
        stock_table = "库存湖表-隔离"
        _register_lake_table(sales_table)
        _register_lake_table(stock_table)
        _create_enabled_quality_rule(sales_table, "销量隔离规则")

        # 销量表：入湖 120 行
        pid = _create_pipeline(sales_table, "隔离-销量入湖")
        ingest_svc.start_pipeline(pid)
        ingest_svc.complete_pipeline(pid, 120)

        # 库存表：回填 76 行
        bid = _create_backfill(stock_table, "隔离-库存回填")
        backfill_svc.start_backfill(bid)
        backfill_svc.complete_backfill(bid, 76)
        rid = _create_govern_report("2026-W47-隔离")
        GovernReportService().generate_report(rid)

        # 销量表只认入湖 120，回填计量为 0
        sales_ingest = completion_registry.table_metrics(sales_table, "ingest_pipeline")
        sales_backfill = completion_registry.table_metrics(sales_table, "backfill_job")
        assert sales_ingest["record_count"] == 120
        assert sales_backfill["record_count"] == 0 and sales_backfill["receipt_count"] == 0

        # 库存表只认回填 76，入湖计量为 0；质量侧也读不到销量的行数
        stock_backfill = completion_registry.table_metrics(stock_table, "backfill_job")
        stock_ingest = completion_registry.table_metrics(stock_table, "ingest_pipeline")
        assert stock_backfill["record_count"] == 76
        assert stock_ingest["record_count"] == 0 and stock_ingest["receipt_count"] == 0
        assert quality.ingest_quality_for_table(stock_table)["booked_record_count"] == 0
        assert quality.ingest_quality_for_table(sales_table)["booked_record_count"] == 120

        # 汇总视图同样按表分列
        ingest_by_table = completion_registry.metrics_by_table("ingest_pipeline")
        backfill_by_table = completion_registry.metrics_by_table("backfill_job")
        assert ingest_by_table[sales_table]["record_count"] == 120
        assert stock_table not in ingest_by_table
        assert backfill_by_table[stock_table]["record_count"] == 76
        assert sales_table not in backfill_by_table

    def test_record_count_consistent_across_entry_receipt_consumer(self):
        """任务记录条数 = 回执条数 = 消费者收到数量。"""
        table = "湖表-三方一致"
        _register_lake_table(table)
        _create_enabled_quality_rule(table, "三方一致规则")
        pid = _create_pipeline(table, "三方一致入湖")
        svc = IngestPipelineService()
        svc.start_pipeline(pid)
        entry, _ = svc.complete_pipeline(pid, 120)

        receipt = completion_registry.find_task("ingest_pipeline", pid)
        booked = QualityMonitorService().ingest_quality_for_table(table)
        assert receipt["record_count"] == booked["booked_record_count"] == 120
        assert booked["booked_receipt_count"] == 1
