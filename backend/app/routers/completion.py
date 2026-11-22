"""任务完工与恢复端点：统一收口入湖管道、回填任务的完工/打断/重发请求。

- POST /api/completion/ingest/{entry_id}            完工（持久化 + 投递质量监控）
- POST /api/completion/backfill/{entry_id}          完工（持久化 + 投递治理报告）
- POST /api/completion/{kind}/{entry_id}/persist    只持久化完成状态（投递前停住）
- POST /api/completion/{kind}/{entry_id}/redeliver  重发完工请求（复用同一回执）

通用动作接口的输入输出契约保持不变，这里只是把带记录数参数的完工与
恢复流程暴露成 HTTP，便于在同一套内存数据上演练打断与重发。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.schemas import ActionResult
from app.services.backfill_job import BackfillJobService
from app.services.ingest_pipeline import IngestPipelineService

router = APIRouter(prefix="/api/completion", tags=["任务完工回执"])


class RecordCountPayload(BaseModel):
    record_count: int = Field(ge=0)


def _service(kind: str):
    if kind == "ingest":
        return IngestPipelineService(), "ingest_pipeline"
    if kind == "backfill":
        return BackfillJobService(), "backfill_job"
    return None, ""


@router.post("/ingest/{entry_id}", response_model=ActionResult)
def complete_ingest(entry_id: int, payload: RecordCountPayload) -> ActionResult:
    entry, message = IngestPipelineService().complete_pipeline(entry_id, payload.record_count)
    return ActionResult(ok=entry is not None, message=message, entry=entry)


@router.post("/backfill/{entry_id}", response_model=ActionResult)
def complete_backfill(entry_id: int, payload: RecordCountPayload) -> ActionResult:
    entry, message = BackfillJobService().complete_backfill(entry_id, payload.record_count)
    return ActionResult(ok=entry is not None, message=message, entry=entry)


@router.post("/{kind}/{entry_id}/persist", response_model=ActionResult)
def persist_completion(kind: str, entry_id: int, payload: RecordCountPayload) -> ActionResult:
    service, module = _service(kind)
    if service is None:
        return ActionResult(ok=False, message=f"未知任务类型：{kind}")
    if module == "ingest_pipeline":
        entry, message = service.persist_pipeline_completion(entry_id, payload.record_count)
    else:
        entry, message = service.persist_backfill_completion(entry_id, payload.record_count)
    return ActionResult(ok=entry is not None, message=message, entry=entry)


@router.post("/{kind}/{entry_id}/redeliver", response_model=ActionResult)
def redeliver_completion(kind: str, entry_id: int) -> ActionResult:
    service, _ = _service(kind)
    if service is None:
        return ActionResult(ok=False, message=f"未知任务类型：{kind}")
    entry, message = service.redeliver_completion(entry_id)
    return ActionResult(ok=entry is not None, message=message, entry=entry)
