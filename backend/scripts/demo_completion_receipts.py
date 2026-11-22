"""端到端演示：统一任务完成数据回执。

场景（与任务说明一一对应）：
1. 销量入湖 120 行 → 统一回执交质量监控，保存响应与事件样本；
2. 库存回填 76 行 → 统一回执交治理报告，保存响应与事件样本；
3. 下一笔回填在“完成状态持久化后、回执确认前”停住，随后重发完工；
4. 对照恢复前后治理报告合计，核对入湖质量结果与两张表的任务计量，
   验证入湖/回填行数不串表、重复投递只入账一次、原任务号可查。

运行：python scripts/demo_completion_receipts.py
样本输出：/workspace/backend/samples/completion_receipts/
"""
from __future__ import annotations

import json
import os
import sys

# 保证从任意目录运行都能 import app
BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)

from app.store import store  # noqa: E402
from app.services.completion_receipt import completion_registry  # noqa: E402
from app.services.ingest_pipeline import IngestPipelineService  # noqa: E402
from app.services.backfill_job import BackfillJobService  # noqa: E402
from app.services.quality_monitor import QualityMonitorService  # noqa: E402
from app.services.govern_report import GovernReportService  # noqa: E402
from app.services.lake_table import LakeTableService  # noqa: E402

SAMPLE_DIR = os.path.join(BACKEND_DIR, "samples", "completion_receipts")

SALES_TABLE = "demo_销量湖表"
STOCK_TABLE = "demo_库存湖表"
INGEST_ROWS = 120
BACKFILL_ROWS = 76
NEXT_BACKFILL_ROWS = 40


def _dump(name: str, payload: object) -> str:
    path = os.path.join(SAMPLE_DIR, name)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    return path


def _events(source: str, target: str) -> list[dict]:
    return store.events_between(source, target)


def main() -> None:
    os.makedirs(SAMPLE_DIR, exist_ok=True)
    # 场景隔离：清空回执登记与质量入账，保留 store 种子数据
    completion_registry.reset()
    QualityMonitorService._intake.clear()

    lake_svc = LakeTableService()
    ingest_svc = IngestPipelineService()
    backfill_svc = BackfillJobService()
    quality_svc = QualityMonitorService()
    govern_svc = GovernReportService()

    # ---- 准备：注册两张目标湖表 + 启用销量表质量规则 ----
    sales_lake, _ = lake_svc.create_entry({
        "表名": SALES_TABLE, "所属业务域": "销售域", "存储格式": "parquet",
    })
    lake_svc.register_table(sales_lake["id"])
    stock_lake, _ = lake_svc.create_entry({
        "表名": STOCK_TABLE, "所属业务域": "库存域", "存储格式": "orc",
    })
    lake_svc.register_table(stock_lake["id"])

    q_rule, _ = quality_svc.create_entry({
        "规则名称": "demo_销量入湖完整性规则",
        "监控指标": "入湖记录数",
        "告警阈值": "1",
        "目标湖表": SALES_TABLE,
        "检查频率": "每次入湖",
    })
    quality_svc.enable_monitor(q_rule["id"])

    # ---- 场景 1：销量入湖 120 行 ----
    pipeline, _ = ingest_svc.create_entry({
        "任务名称": "demo_销量入湖任务",
        "来源系统": "销售中台",
        "目标湖表": SALES_TABLE,
        "数据格式": "json",
    })
    ingest_svc.start_pipeline(pipeline["id"])
    ingest_entry, ingest_msg = ingest_svc.complete_pipeline(pipeline["id"], INGEST_ROWS)
    ingest_receipt = completion_registry.find_task("ingest_pipeline", pipeline["id"])
    ingest_events = [
        e for e in _events("ingest_pipeline", "quality_monitor")
        if e["payload"].get("pipeline_id") == pipeline["id"]
    ]
    quality_result = quality_svc.ingest_quality_for_table(SALES_TABLE)

    _dump("01_ingest_response.json", {
        "ok": True,
        "message": ingest_msg,
        "entry": ingest_entry,
    })
    _dump("02_ingest_receipt.json", ingest_receipt)
    _dump("03_ingest_event_to_quality.json", ingest_events)
    _dump("04_quality_ingest_result.json", quality_result)

    # ---- 场景 2：库存回填 76 行 ----
    backfill_create, _ = backfill_svc.create_entry({
        "任务名称": "demo_库存回填任务",
        "目标湖表": STOCK_TABLE,
        "回填时间范围": "2026-10-01~2026-10-31",
    })
    backfill_svc.start_backfill(backfill_create["id"])
    backfill_entry, backfill_msg = backfill_svc.complete_backfill(backfill_create["id"], BACKFILL_ROWS)
    backfill_receipt = completion_registry.find_task("backfill_job", backfill_create["id"])
    backfill_events = [
        e for e in _events("backfill_job", "govern_report")
        if e["payload"].get("task_id") == backfill_create["id"]
    ]

    # 治理报告（恢复前基线）：回填 76 行在此入账确认
    report1, _ = govern_svc.create_entry({
        "报告周期": "demo_2026-W47_基线",
        "治理指标": "回填完成量",
        "优化建议": "无",
    })
    report_before, report_before_msg = govern_svc.generate_report(report1["id"])

    _dump("05_backfill_response.json", {
        "ok": True,
        "message": backfill_msg,
        "entry": backfill_entry,
    })
    _dump("06_backfill_receipt.json", backfill_receipt)
    _dump("07_backfill_event_to_govern.json", backfill_events)
    _dump("08_govern_report_before_recovery.json", {
        "ok": True,
        "message": report_before_msg,
        "entry": report_before,
    })

    # ---- 场景 3：下一笔回填，完成状态持久化后停住，再重发完工 ----
    next_bf, _ = backfill_svc.create_entry({
        "任务名称": "demo_库存回填任务_下一笔",
        "目标湖表": STOCK_TABLE,
        "回填时间范围": "2026-09-01~2026-09-15",
    })
    backfill_svc.start_backfill(next_bf["id"])
    paused_entry, paused_msg = backfill_svc.persist_backfill_completion(
        next_bf["id"], NEXT_BACKFILL_ROWS
    )
    paused_receipt = completion_registry.find_task("backfill_job", next_bf["id"])
    # 固化打断时刻的回执状态（后续报告生成会把该回执确认掉）
    paused_confirmed = bool(paused_receipt["confirmed"])
    pending_for_govern = completion_registry.pending_confirmation("govern_report")
    # 固化打断时刻的可入账快照（后续报告生成会把这些回执确认掉）
    paused_pending_ids = [
        (r["task_module"], r["task_id"], r["record_count"]) for r in pending_for_govern
    ]
    interrupted_events = [
        e for e in _events("backfill_job", "govern_report")
        if e["payload"].get("task_id") == next_bf["id"]
    ]

    _dump("09_backfill_paused_after_persist.json", {
        "ok": True,
        "message": paused_msg,
        "entry": paused_entry,
        "receipt": paused_receipt,
        "pending_confirmation_for_govern": pending_for_govern,
        "govern_events_before_redeliver": interrupted_events,
    })

    # 重发完工请求（首次重发：补投递事件）
    redeliver_entry, redeliver_msg = backfill_svc.redeliver_completion(next_bf["id"])
    # 再重发两次：回执已投递，不能再产生事件；确认由治理报告生成时完成
    backfill_svc.redeliver_completion(next_bf["id"])
    second_redeliver_entry, second_redeliver_msg = backfill_svc.redeliver_completion(next_bf["id"])
    events_after_repeats = [
        e for e in _events("backfill_job", "govern_report")
        if e["payload"].get("task_id") == next_bf["id"]
    ]
    next_receipt = completion_registry.find_task("backfill_job", next_bf["id"])

    _dump("10_backfill_redeliver.json", {
        "first_redeliver": {"ok": True, "message": redeliver_msg, "entry": redeliver_entry},
        "repeat_redeliver": {"ok": True, "message": second_redeliver_msg, "entry": second_redeliver_entry},
        "govern_event_count": len(events_after_repeats),
        "govern_events": events_after_repeats,
        "receipt": next_receipt,
    })

    # ---- 场景 4：恢复后报告 + 两表计量核对 ----
    report2, _ = govern_svc.create_entry({
        "报告周期": "demo_2026-W47_恢复后",
        "治理指标": "回填完成量",
        "优化建议": "无",
    })
    report_after, report_after_msg = govern_svc.generate_report(report2["id"])

    sales_ingest_metrics = completion_registry.table_metrics(SALES_TABLE, "ingest_pipeline")
    sales_backfill_metrics = completion_registry.table_metrics(SALES_TABLE, "backfill_job")
    stock_backfill_metrics = completion_registry.table_metrics(STOCK_TABLE, "backfill_job")
    stock_ingest_metrics = completion_registry.table_metrics(STOCK_TABLE, "ingest_pipeline")
    quality_sales = quality_svc.ingest_quality_for_table(SALES_TABLE)
    quality_stock = quality_svc.ingest_quality_for_table(STOCK_TABLE)

    _dump("11_govern_report_after_recovery.json", {
        "ok": True,
        "message": report_after_msg,
        "entry": report_after,
    })
    _dump("12_table_task_metrics.json", {
        SALES_TABLE: {
            "ingest_pipeline": sales_ingest_metrics,
            "backfill_job": sales_backfill_metrics,
            "quality_ingest_booked": quality_sales,
        },
        STOCK_TABLE: {
            "ingest_pipeline": stock_ingest_metrics,
            "backfill_job": stock_backfill_metrics,
            "quality_ingest_booked": quality_stock,
        },
    })
    _dump("13_all_completion_receipts.json", {
        "total": len(completion_registry.all()),
        "items": completion_registry.all(),
    })
    _dump("14_lookup_by_original_task_no.json", {
        "ingest_task_no": str(pipeline["id"]),
        "backfill_task_no": str(backfill_create["id"]),
        "next_backfill_task_no": str(next_bf["id"]),
        "ingest": completion_registry.find_by_task_no(str(pipeline["id"]), "ingest_pipeline"),
        "backfill": completion_registry.find_by_task_no(str(backfill_create["id"]), "backfill_job"),
        "next_backfill": completion_registry.find_by_task_no(str(next_bf["id"]), "backfill_job"),
    })

    # ---- 断言：把验收口径固化在脚本里 ----
    checks: list[dict[str, object]] = []

    def check(name: str, condition: bool, detail: object) -> None:
        checks.append({"check": name, "passed": bool(condition), "detail": detail})

    check("入湖回执 120 行且质量已确认",
          ingest_receipt["record_count"] == 120 and ingest_receipt["confirmed"] is True,
          ingest_receipt)
    check("回填回执 76 行", backfill_receipt["record_count"] == 76, backfill_receipt)
    check("回填任务记录条数=76 与回执一致",
          backfill_entry["处理记录数"] == "76" and backfill_receipt["record_count"] == 76,
          {"entry": backfill_entry["处理记录数"], "receipt": backfill_receipt["record_count"]})
    check("打断期间无治理事件", len(interrupted_events) == 0, interrupted_events)
    check("打断时回执未确认且可入账",
          paused_confirmed is False
          and ("backfill_job", next_bf["id"], NEXT_BACKFILL_ROWS) in paused_pending_ids,
          {"paused_confirmed": paused_confirmed, "paused_pending_ids": paused_pending_ids})
    before_total = report_before["report_data"]["backfill_summary"]["total_backfill_records"]
    after_total = report_after["report_data"]["backfill_summary"]["total_backfill_records"]
    check("恢复前后报告合计 76 → 116",
          before_total == 76 and after_total == 116,
          {"before": before_total, "after": after_total})
    check("下一笔回填重发只投递一次事件",
          len(events_after_repeats) == 1 and next_receipt["delivery_count"] == 4,
          {"events": len(events_after_repeats), "delivery_count": next_receipt["delivery_count"]})
    check("下一笔回填恢复后已确认且只入账一次",
          next_receipt["confirmed"] is True and next_receipt["confirmations"] == 1,
          next_receipt)
    check("销量表入湖质量入账 120",
          quality_sales["booked_record_count"] == 120
          and sales_ingest_metrics["record_count"] == 120,
          {"quality": quality_sales, "metrics": sales_ingest_metrics})
    check("销量表回填计量为 0（不串表）",
          sales_backfill_metrics["record_count"] == 0
          and sales_backfill_metrics["receipt_count"] == 0,
          sales_backfill_metrics)
    check("库存表回填计量 76+40=116（不串表）",
          stock_backfill_metrics["record_count"] == 116
          and stock_backfill_metrics["receipt_count"] == 2,
          stock_backfill_metrics)
    check("库存表入湖计量为 0、质量读不到销量行数",
          stock_ingest_metrics["record_count"] == 0
          and quality_stock["booked_record_count"] == 0,
          {"ingest_metrics": stock_ingest_metrics, "quality": quality_stock})
    lookup_detail = {
        "ingest": completion_registry.find_by_task_no(str(pipeline["id"]), "ingest_pipeline"),
        "backfill": completion_registry.find_by_task_no(str(backfill_create["id"]), "backfill_job"),
        "next_backfill": completion_registry.find_by_task_no(str(next_bf["id"]), "backfill_job"),
        # 跨类型重号时，不限定任务类型会同时命中两种任务（原任务号+任务类型唯一定位）
        "bare_task_no_4_cross_kind": completion_registry.find_by_task_no("4"),
    }
    check("原任务号可查三份完成明细",
          all(len(rows) == 1 for rows in lookup_detail.values()
              if rows != lookup_detail["bare_task_no_4_cross_kind"])
          and len(lookup_detail["bare_task_no_4_cross_kind"]) == 2,
          lookup_detail)

    _dump("15_verification_checks.json", {
        "all_passed": all(item["passed"] for item in checks),
        "checks": checks,
    })

    print("=" * 72)
    print("统一任务完成数据回执演示完成")
    print(f"样本目录：{SAMPLE_DIR}")
    for item in checks:
        flag = "PASS" if item["passed"] else "FAIL"
        print(f"[{flag}] {item['check']}")
    print("=" * 72)
    if not all(item["passed"] for item in checks):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
