"""跨模块编排端点：暴露跨模块数据流链路、事件日志和快照信息。

提供 /api/cross/events, /api/cross/snapshots, /api/cross/pipeline-status 等端点，
让前端和治理报告可以消费跨模块链路数据。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services import service_for, SERVICES
from app.store import store
from app.services.completion_receipt import completion_registry
from app.services.partition_policy import PartitionPolicyService
from app.services.lake_table import LakeTableService
from app.services.storage_tier import StorageTierService
from app.services.file_compaction import FileCompactionService
from app.services.retention_mgr import RetentionMgrService
from app.services.backfill_job import BackfillJobService
from app.services.govern_report import GovernReportService
from app.services.quality_monitor import QualityMonitorService

router = APIRouter(prefix="/api/cross", tags=["跨模块编排"])


@router.get("/events")
def list_cross_events(
    source: str | None = Query(default=None),
    target: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    """查询跨模块事件日志，支持按 source/target 筛选。"""
    if source and target:
        events = store.events_between(source, target)
    elif source or target:
        events = store.events_for_module(source or target or "")
    else:
        events = list(store._events)
    return {
        "total": len(events),
        "items": events[-limit:],
    }


@router.get("/snapshots")
def list_snapshots() -> dict[str, Any]:
    """查询所有模块快照记录。"""
    return {
        "total": len(store._snapshots),
        "keys": store.snapshot_keys(),
    }


@router.get("/completion-receipts")
def list_completion_receipts(
    task_module: str | None = Query(default=None),
    target_table: str | None = Query(default=None),
    confirmed: bool | None = Query(default=None),
    task_no: str | None = Query(default=None),
) -> dict[str, Any]:
    """查询统一任务完成数据回执，支持按任务类型/湖表/确认状态/原任务号核对。"""
    if task_no:
        receipts = completion_registry.find_by_task_no(task_no, task_module)
    else:
        receipts = completion_registry.all(
            task_module=task_module, target_table=target_table, confirmed=confirmed
        )
    return {"total": len(receipts), "items": receipts}


@router.get("/completion-receipts/pending")
def pending_completion_receipts(
    consumer: str | None = Query(default=None),
) -> dict[str, Any]:
    """完成状态已持久化但回执尚未确认、下游可以入账的完成数。"""
    receipts = completion_registry.pending_confirmation(consumer)
    return {"total": len(receipts), "items": receipts}


@router.get("/completion-receipts/table-metrics")
def completion_table_metrics(
    task_module: str = Query(...),
    target_table: str | None = Query(default=None),
) -> dict[str, Any]:
    """按湖表查询入湖/回填任务计量；不同湖表、不同任务类型互不串数。"""
    if target_table:
        return completion_registry.table_metrics(target_table, task_module)
    return {"task_module": task_module, "by_table": completion_registry.metrics_by_table(task_module)}


@router.get("/quality-ingest")
def quality_ingest_results(target_table: str = Query(...)) -> dict[str, Any]:
    """质量监控侧按湖表查询已入账的入湖完成数。"""
    return QualityMonitorService().ingest_quality_for_table(target_table)


@router.get("/overview")
def cross_module_overview() -> dict[str, Any]:
    """跨模块统计概览。"""
    return store.cross_module_stats()


@router.get("/pipeline-status")
def pipeline_status() -> dict[str, Any]:
    """汇总数据湖治理全链路状态。
    从分区策略 → 湖表注册 → 冷热分层 → 小文件合并 → 保留策略 → 治理报告。
    """
    # 分区策略概览
    policies = store.rows("partition_policy")
    active_policies = [p for p in policies if p.get("status") == "已生效"]

    # 湖表注册概览
    lake_tables = store.rows("lake_table")
    registered_tables = [lt for lt in lake_tables if lt.get("status") == "已注册"]

    # 冷热分层概览
    tiers = store.rows("storage_tier")
    active_tiers = [t for t in tiers if t.get("status") == "已生效"]

    # 小文件合并概览
    compactions = store.rows("file_compaction")
    pending_compactions = [c for c in compactions if c.get("status") == "待调度"]
    running_compactions = [c for c in compactions if c.get("status") == "执行中"]
    completed_compactions = [c for c in compactions if c.get("status") == "已完成"]

    # 保留策略概览
    retentions = store.rows("retention_mgr")
    active_retentions = [r for r in retentions if r.get("status") == "已生效"]

    # 回填任务概览
    backfills = store.rows("backfill_job")
    active_backfills = [b for b in backfills if b.get("status") in ("待调度", "回填中")]

    # 血缘链路概览
    lineages = store.rows("lineage_graph")
    broken_lineages = [l for l in lineages if l.get("status") == "链路断裂"]

    # 治理报告概览
    reports = store.rows("govern_report")

    # 跨模块事件
    cross_stats = store.cross_module_stats()

    return {
        "pipeline": {
            "partition_policies": {"total": len(policies), "active": len(active_policies)},
            "lake_tables": {"total": len(lake_tables), "registered": len(registered_tables)},
            "storage_tiers": {"total": len(tiers), "active": len(active_tiers)},
            "file_compactions": {
                "total": len(compactions),
                "pending": len(pending_compactions),
                "running": len(running_compactions),
                "completed": len(completed_compactions),
            },
            "retention_mgrs": {"total": len(retentions), "active": len(active_retentions)},
            "backfill_jobs": {"total": len(backfills), "active": len(active_backfills)},
            "lineage_graphs": {"total": len(lineages), "broken": len(broken_lineages)},
            "govern_reports": {"total": len(reports)},
        },
        "cross_module": cross_stats,
        "health": {
            "has_broken_lineages": len(broken_lineages) > 0,
            "has_active_backfills": len(active_backfills) > 0,
            "all_lake_tables_registered": len(registered_tables) == len(lake_tables),
        },
    }