"""HTTP 层冒烟测试：全部通过真实 HTTP 接口验证统一回执与恢复流程。

覆盖：
- 入湖 120 行完工 → 质量监控入账（/api/completion/ingest）；
- 回填 76 行完工 → 治理报告合计（/api/completion/backfill）；
- 下一笔回填 persist 后停住 → redeliver 重发，报告合计 76 → 76+40；
- 待确认端点、湖表计量隔离、原任务号查询、重发只投递一次。
"""
from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request

BASE = "http://127.0.0.1:8014/api"


def call(method: str, path: str, payload: object = None) -> dict:
    # 对查询串中的非 ASCII 字符（中文湖表名等）做百分号编码
    if "?" in path:
        route, query = path.split("?", 1)
        path = route + "?" + urllib.parse.urlencode(
            urllib.parse.parse_qsl(query, keep_blank_values=True)
        )
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise AssertionError(f"HTTP {exc.code} on {method} {path}: {exc.read().decode()}") from exc


def main() -> None:
    # ---- 准备：两张湖表 + 质量规则 ----
    sales_lake_id = call("POST", "/lake_table", {"values": {
        "表名": "http_销量湖表", "所属业务域": "销售域", "存储格式": "parquet"}})["entry"]["id"]
    call("POST", f"/lake_table/{sales_lake_id}/actions", {"action": "注册入湖"})
    stock_lake_id = call("POST", "/lake_table", {"values": {
        "表名": "http_库存湖表", "所属业务域": "库存域", "存储格式": "orc"}})["entry"]["id"]
    call("POST", f"/lake_table/{stock_lake_id}/actions", {"action": "注册入湖"})
    rule_id = call("POST", "/quality_monitor", {"values": {
        "规则名称": "http_销量质量规则", "监控指标": "入湖记录数", "告警阈值": "1",
        "目标湖表": "http_销量湖表", "检查频率": "每次入湖"}})["entry"]["id"]
    call("POST", f"/quality_monitor/{rule_id}/actions", {"action": "启用监控"})

    # ---- 入湖 120 行 ----
    pid = call("POST", "/ingest_pipeline", {"values": {
        "任务名称": "http_销量入湖", "来源系统": "销售中台",
        "目标湖表": "http_销量湖表", "数据格式": "json"}})["entry"]["id"]
    call("POST", f"/ingest_pipeline/{pid}/actions", {"action": "启动任务"})
    ingest_resp = call("POST", f"/completion/ingest/{pid}", {"record_count": 120})
    assert ingest_resp["ok"] and ingest_resp["entry"]["status"] == "已完成", ingest_resp

    quality = call("GET", "/cross/quality-ingest?target_table=http_销量湖表")
    assert quality["booked_record_count"] == 120, quality
    ingest_receipts = call("GET", "/cross/completion-receipts"
                                  "?task_module=ingest_pipeline&target_table=http_销量湖表")
    assert ingest_receipts["total"] == 1 and ingest_receipts["items"][0]["confirmed"] is True

    # ---- 回填 76 行 + 恢复前报告 ----
    bid = call("POST", "/backfill_job", {"values": {
        "任务名称": "http_库存回填", "目标湖表": "http_库存湖表",
        "回填时间范围": "2026-10-01~2026-10-31"}})["entry"]["id"]
    call("POST", f"/backfill_job/{bid}/actions", {"action": "启动回填"})
    bf_resp = call("POST", f"/completion/backfill/{bid}", {"record_count": 76})
    assert bf_resp["ok"] and bf_resp["entry"]["处理记录数"] == "76", bf_resp

    rid1 = call("POST", "/govern_report", {"values": {
        "报告周期": "http_W47_基线", "治理指标": "回填量", "优化建议": "无"}})["entry"]["id"]
    call("POST", f"/govern_report/{rid1}/actions", {"action": "生成报告"})
    report_before = call("GET", f"/govern_report/{rid1}")
    assert report_before["report_data"]["backfill_summary"]["total_backfill_records"] == 76

    # ---- 下一笔回填：persist 后停住 ----
    bid2 = call("POST", "/backfill_job", {"values": {
        "任务名称": "http_库存回填_下一笔", "目标湖表": "http_库存湖表",
        "回填时间范围": "2026-09-01~2026-09-15"}})["entry"]["id"]
    call("POST", f"/backfill_job/{bid2}/actions", {"action": "启动回填"})
    paused = call("POST", f"/completion/backfill/{bid2}/persist", {"record_count": 40})
    assert paused["ok"] and paused["entry"]["status"] == "已完成", paused

    pending = call("GET", "/cross/completion-receipts/pending?consumer=govern_report")
    assert any(r["task_id"] == bid2 and r["record_count"] == 40 for r in pending["items"])
    events_paused = call("GET", "/cross/events?source=backfill_job&target=govern_report")
    assert not any(e["payload"].get("task_id") == bid2 for e in events_paused["items"])

    # ---- 重发完工（含重复重发）----
    red1 = call("POST", f"/completion/backfill/{bid2}/redeliver")
    assert red1["ok"] and "补交给治理报告" in red1["message"], red1
    call("POST", f"/completion/backfill/{bid2}/redeliver")
    red3 = call("POST", f"/completion/backfill/{bid2}/redeliver")
    assert "跳过重复事件" in red3["message"], red3
    events_after = call("GET", "/cross/events?source=backfill_job&target=govern_report")
    assert sum(1 for e in events_after["items"] if e["payload"].get("task_id") == bid2) == 1

    # ---- 恢复后报告：合计 116 ----
    rid2 = call("POST", "/govern_report", {"values": {
        "报告周期": "http_W47_恢复后", "治理指标": "回填量", "优化建议": "无"}})["entry"]["id"]
    call("POST", f"/govern_report/{rid2}/actions", {"action": "生成报告"})
    report_after = call("GET", f"/govern_report/{rid2}")
    summary = report_after["report_data"]["backfill_summary"]
    assert summary["total_backfill_records"] == 116, summary
    assert summary["by_table"]["http_库存湖表"]["record_count"] == 116

    # ---- 隔离与原任务号 ----
    sales_bf = call("GET", "/cross/completion-receipts/table-metrics"
                           "?task_module=backfill_job&target_table=http_销量湖表")
    assert sales_bf["record_count"] == 0 and sales_bf["receipt_count"] == 0
    stock_ing = call("GET", "/cross/completion-receipts/table-metrics"
                            "?task_module=ingest_pipeline&target_table=http_库存湖表")
    assert stock_ing["record_count"] == 0 and stock_ing["receipt_count"] == 0
    found = call("GET", f"/cross/completion-receipts?task_no={bid2}&task_module=backfill_job")
    assert found["total"] == 1 and found["items"][0]["record_count"] == 40

    print("HTTP smoke 全部通过：")
    print(json.dumps({
        "ingest_120_quality_booked": quality,
        "report_before_total": 76,
        "report_after_backfill_summary": summary,
        "sales_table_backfill_metrics": sales_bf,
        "stock_table_ingest_metrics": stock_ing,
        "redeliver_message": red3["message"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
