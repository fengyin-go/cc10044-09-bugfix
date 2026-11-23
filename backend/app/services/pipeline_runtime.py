"""流水线执行运行时账本：重跑单、ETL 执行单、目标表写入凭据和完成消息共用。

这些账本是跨模块的内部簿记，不属于任何业务模块，因此放在 Store 的
aux 表里：既能被流水线监控和 ETL 流水线共享，又不会改变页面上的模块清单。

关键约束：
- 一个失败号最多派生一张重跑单（pipeline_rerun_orders 做幂等锚点）；
- 一次执行最多产生一条目标表写入凭据（按 execution_id 唯一）；
- 完成消息按 message_id 去重，重复补送不得把处理行数累加；
- 增量水位线只认「已经落表」的写入凭据，失败摘要和未落表执行不能推进起点。
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta
from typing import Any

from app.store import store

# 内部账本名称
EXECUTIONS = "etl_executions"
WRITE_RECEIPTS = "etl_write_receipts"
RERUN_ORDERS = "pipeline_rerun_orders"
COMPLETION_MESSAGES = "pipeline_completion_messages"

# 同一进程内的同步锁：HTTP 同步端点跑在线程池里，重复请求需要串行去重。
FLOW_LOCK = threading.RLock()

DEFAULT_COLLECT_START = "2026-11-21T00:00:00Z"
# 日常订单入仓的源端在 10:15 之前的数据已经可抽取，首轮重跑应写入到这里。
DEFAULT_LANDED_CUTOFF = "2026-11-21T10:15:00Z"
# 失败摘要里声明的采集边界，只代表「尝试过」，不代表已经落表。
DEFAULT_FAILURE_SUMMARY_CUTOFF = "2026-11-21T10:30:00Z"
DEFAULT_PIPELINE_NAME = "ods_order_inc_daily"

# 执行状态机：运行中 -> 待确认 -> 已完成；运行中 -> 执行失败/超时终止。
EXEC_RUNNING = "运行中"
EXEC_PENDING_CONFIRM = "待确认"
EXEC_COMPLETED = "已完成"
EXEC_FAILED = "执行失败"
EXEC_TIMEOUT = "超时终止"

_TIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
)


def next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in _TIME_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def format_time(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def latest_time(values: list[Any]) -> str:
    parsed = [item for item in (parse_time(value) for value in values) if item is not None]
    return format_time(max(parsed)) if parsed else ""


# ---------------------------------------------------------------------------
# 账本查询
# ---------------------------------------------------------------------------


def pipeline_executions(
    pipeline_id: int | None = None, pipeline_name: str | None = None
) -> list[dict[str, Any]]:
    rows = list(store.aux_rows(EXECUTIONS))
    if pipeline_id is not None:
        rows = [row for row in rows if str(row.get("pipeline_id")) == str(pipeline_id)]
    if pipeline_name is not None:
        rows = [row for row in rows if row.get("pipeline_name") == pipeline_name]
    return rows


def write_receipts(
    pipeline_id: int | None = None, pipeline_name: str | None = None
) -> list[dict[str, Any]]:
    rows = list(store.aux_rows(WRITE_RECEIPTS))
    if pipeline_id is not None:
        rows = [row for row in rows if str(row.get("pipeline_id")) == str(pipeline_id)]
    if pipeline_name is not None:
        rows = [row for row in rows if row.get("pipeline_name") == pipeline_name]
    return rows


def landed_receipts(
    pipeline_id: int | None = None, pipeline_name: str | None = None
) -> list[dict[str, Any]]:
    """只有 landed=True 的凭据才算真正写入目标表，才能推进采集起点。"""
    return [
        row
        for row in write_receipts(pipeline_id=pipeline_id, pipeline_name=pipeline_name)
        if row.get("landed")
    ]


def latest_landed_cutoff(
    pipeline_id: int | None = None, pipeline_name: str | None = None
) -> str:
    return latest_time(
        [
            row.get("collect_cutoff")
            for row in landed_receipts(pipeline_id=pipeline_id, pipeline_name=pipeline_name)
        ]
    )


def latest_confirmed_cutoff(
    pipeline_id: int | None = None, pipeline_name: str | None = None
) -> str:
    return latest_time(
        [
            row.get("collect_cutoff")
            for row in landed_receipts(pipeline_id=pipeline_id, pipeline_name=pipeline_name)
            if row.get("confirmed")
        ]
    )


def next_run_time(floor: str | None = None) -> str:
    """给新运行分配一个单调递增的运行时间，避免历史列表排序错乱。"""
    candidates = [row.get("运行时间") for row in store.rows("pipeline_monitor")]
    if floor:
        candidates.append(floor)
    latest = parse_time(latest_time(candidates))
    base = latest or datetime(2026, 11, 21, 8, 30, 0)
    return format_time(base + timedelta(minutes=5))


def _next_collect_cutoff(start: str) -> str:
    """未显式指定截止点时，在已落表起点之后推进一段，保证窗口不为空。"""
    start_dt = parse_time(start)
    default_dt = parse_time(DEFAULT_LANDED_CUTOFF)
    if start_dt is None:
        return DEFAULT_LANDED_CUTOFF
    base = max(start_dt + timedelta(minutes=15), default_dt or start_dt)
    return format_time(base)


def _failure_summary_cutoff(landed: str) -> str:
    """失败摘要声明的采集边界：总是晚于已落表边界，但绝不参与水位线。"""
    landed_dt = parse_time(landed)
    default_dt = parse_time(DEFAULT_FAILURE_SUMMARY_CUTOFF)
    if landed_dt is None:
        return DEFAULT_FAILURE_SUMMARY_CUTOFF
    base = max(default_dt or landed_dt, landed_dt + timedelta(minutes=15))
    return format_time(base)


def find_execution_by_monitor_run(run_id: int) -> dict[str, Any] | None:
    for row in store.aux_rows(EXECUTIONS):
        if int(row.get("monitor_run_id") or 0) == int(run_id):
            return row
    return None


def find_execution_by_failure(failure_id: int) -> dict[str, Any] | None:
    """原失败号 -> 它派生出的那一条执行（幂等锚点）。"""
    order = find_rerun_order(failure_id)
    if order is not None:
        return store.aux_find(EXECUTIONS, int(order["execution_id"]))
    for row in store.aux_rows(EXECUTIONS):
        if row.get("source_failure_id") is not None \
                and int(row.get("source_failure_id")) == int(failure_id):
            return row
    return None


def find_receipt_for_execution(execution_id: int) -> dict[str, Any] | None:
    for row in store.aux_rows(WRITE_RECEIPTS):
        if int(row.get("execution_id") or 0) == int(execution_id):
            return row
    return None


def find_rerun_order(failure_id: int) -> dict[str, Any] | None:
    for row in store.aux_rows(RERUN_ORDERS):
        if int(row.get("failure_id") or 0) == int(failure_id):
            return row
    return None


def find_execution(execution_id: int | None = None,
                   failure_id: int | None = None,
                   run_id: int | None = None) -> dict[str, Any] | None:
    if execution_id is not None:
        return store.aux_find(EXECUTIONS, int(execution_id))
    if failure_id is not None:
        return find_execution_by_failure(int(failure_id))
    if run_id is not None:
        return find_execution_by_monitor_run(int(run_id))
    return None


# ---------------------------------------------------------------------------
# 流水线解析
# ---------------------------------------------------------------------------


def resolve_pipeline(
    pipeline_id: int | None = None, pipeline_name: str | None = None
) -> dict[str, Any] | None:
    rows = store.rows("etl_pipeline")
    if pipeline_id is not None:
        for row in rows:
            if int(row.get("id", 0)) == int(pipeline_id):
                return row
    if pipeline_name:
        for row in rows:
            if row.get("流水线名称") == pipeline_name:
                return row
    return None


def target_table_name(entry: dict[str, Any] | None, fallback: str = "") -> str:
    if entry is None:
        return fallback
    target = entry.get("目标端类型", "")
    name = entry.get("流水线名称", fallback)
    return f"{target.lower()}_{name}" if target else name


# ---------------------------------------------------------------------------
# 重跑受理：一个失败号只派生一条执行 + 一条监控运行记录
# ---------------------------------------------------------------------------


def _latest_monitor_run(pipeline_name: str) -> dict[str, Any] | None:
    rows = [
        row for row in store.rows("pipeline_monitor")
        if row.get("流水线名称") == pipeline_name
    ]
    if not rows:
        return None
    rows.sort(key=lambda row: (row.get("运行时间", ""), int(row.get("id", 0))), reverse=True)
    return rows[0]


def create_rerun(
    failure_id: int,
    *,
    pipeline_id: int | None = None,
    pipeline_name: str | None = None,
    row_count: int = 10,
    collect_cutoff: str | None = None,
    request_id: str | None = None,
    simulate_failure: bool = False,
) -> dict[str, Any]:
    """受理重跑请求，按原失败号做幂等。

    同一个失败号重复请求（包括超时后重发）只会得到同一个 execution_id /
    rerun_id，不会再新增运行记录，也不会二次写目标表。
    """
    with FLOW_LOCK:
        order = find_rerun_order(failure_id)
        if order is not None:
            execution = store.aux_find(EXECUTIONS, int(order["execution_id"]))
            if execution is not None:
                if request_id and request_id not in order.setdefault("request_ids", []):
                    order["request_ids"].append(request_id)
                return {
                    "ok": True,
                    "duplicate": True,
                    "failure_id": failure_id,
                    "rerun_id": order.get("monitor_run_id"),
                    "monitor_run_id": order.get("monitor_run_id"),
                    "execution_id": execution.get("id"),
                    "order_id": order.get("id"),
                    "message": f"该失败号已受理过重跑，沿用运行ID: {order.get('monitor_run_id')}",
                }
            # 订单在但执行丢了：按订单里的信息补一条，仍然只有一个编号。
            order = None

        failure = store.find("pipeline_monitor", int(failure_id))
        if failure is None:
            return {"ok": False, "message": "监控记录不存在"}
        status = failure.get("status")
        if status not in ("执行失败", "超时终止"):
            # 已经成功或尚无失败实体的记录，不能反复派生重跑。
            return {"ok": False, "message": "只能重新运行失败的流水线"}

        name = pipeline_name or failure.get("流水线名称") or DEFAULT_PIPELINE_NAME
        pid = pipeline_id
        if pid is None and failure.get("pipeline_id") is not None:
            pid = int(failure["pipeline_id"])
        pipeline = resolve_pipeline(pipeline_id=pid, pipeline_name=name)
        if pipeline is not None:
            pid = int(pipeline.get("id"))
            name = pipeline.get("流水线名称", name)
        if pid is None:
            # 监控记录只存了名称时，用名称反查；仍查不到就沿用失败记录自身。
            pid = int(failure.get("pipeline_id") or 0)

        row_count = int(row_count or 10)
        # 采集起点只来自已经落表的凭据，绝不采用失败摘要里的边界。
        start = latest_landed_cutoff(pipeline_id=pid, pipeline_name=name) or DEFAULT_COLLECT_START
        cutoff = collect_cutoff or _next_collect_cutoff(start)
        run_time = next_run_time(failure.get("运行时间"))

        execution = {
            "id": next_id(store.aux_rows(EXECUTIONS)),
            "pipeline_id": pid,
            "pipeline_name": name,
            "target_table": target_table_name(pipeline, name),
            "mode": "rerun",
            "source_failure_id": int(failure_id),
            "status": EXEC_RUNNING,
            "state": EXEC_RUNNING,
            "collect_start": start,
            "collect_cutoff": cutoff,
            "attempted_cutoff": cutoff,
            "row_count": row_count,
            "written_rows": 0,
            "confirmed_rows": 0,
            "confirmed_range": "",
            "written_range": "",
            "retry_of": int(failure_id),
            "error": "",
            "created_at": run_time,
            "updated_at": run_time,
        }
        store.aux_rows(EXECUTIONS).append(execution)

        monitor_run = {
            "id": next_id(store.rows("pipeline_monitor")),
            "status": EXEC_PENDING_CONFIRM,
            "pending": True,
            "abnormal": False,
            "流水线名称": name,
            "运行时间": run_time,
            "执行状态": EXEC_PENDING_CONFIRM,
            "处理行数": "0",
            "错误信息": "",
            "retry_of": int(failure_id),
            "retry_reason": failure.get("错误信息", "自动重试"),
            "pipeline_id": pid,
            "execution_id": execution["id"],
            "execution_status": EXEC_PENDING_CONFIRM,
            "confirm_status": EXEC_PENDING_CONFIRM,
            "collect_start": start,
            "collect_cutoff": cutoff,
            "confirm_rows": 0,
        }
        store.rows("pipeline_monitor").append(monitor_run)

        execution["monitor_run_id"] = monitor_run["id"]
        execution["state"] = EXEC_PENDING_CONFIRM
        execution["status"] = EXEC_PENDING_CONFIRM

        receipt = None
        if simulate_failure:
            fail_execution(
                execution_id=execution["id"],
                reason="重跑执行失败：目标端写入超时",
                summary_cutoff=DEFAULT_FAILURE_SUMMARY_CUTOFF,
            )
        else:
            # 首轮同步写入目标表：只落一条凭据，后续重复请求不会再次写入。
            receipt = {
                "id": next_id(store.aux_rows(WRITE_RECEIPTS)),
                "execution_id": execution["id"],
                "pipeline_id": pid,
                "pipeline_name": name,
                "target_table": execution["target_table"],
                "landed": True,
                "written_at": run_time,
                "collect_start": start,
                "collect_cutoff": cutoff,
                "written_range": f"{start}~{cutoff}",
                "row_count": row_count,
                "written_rows": row_count,
                "confirmed": False,
                "confirmed_row_count": 0,
                "confirm_status": EXEC_PENDING_CONFIRM,
                "confirmed_range": "",
                "message_ids": [],
            }
            store.aux_rows(WRITE_RECEIPTS).append(receipt)
            execution["written_rows"] = row_count
            execution["written_range"] = receipt["written_range"]
            monitor_run["written_rows"] = row_count
            monitor_run["written_range"] = receipt["written_range"]

        rerun_order = {
            "id": next_id(store.aux_rows(RERUN_ORDERS)),
            "failure_id": int(failure_id),
            "pipeline_id": pid,
            "pipeline_name": name,
            "execution_id": execution["id"],
            "monitor_run_id": monitor_run["id"],
            "request_ids": [request_id] if request_id else [],
            "status": "已受理",
            "created_at": run_time,
        }
        store.aux_rows(RERUN_ORDERS).append(rerun_order)

        if simulate_failure:
            return {
                "ok": True,
                "duplicate": False,
                "failure_id": int(failure_id),
                "rerun_id": monitor_run["id"],
                "monitor_run_id": monitor_run["id"],
                "execution_id": execution["id"],
                "order_id": rerun_order["id"],
                "status": execution["status"],
                "message": f"重跑执行失败,新失败号: {monitor_run['id']}",
            }
        return {
            "ok": True,
            "duplicate": False,
            "failure_id": int(failure_id),
            "rerun_id": monitor_run["id"],
            "monitor_run_id": monitor_run["id"],
            "execution_id": execution["id"],
            "order_id": rerun_order["id"],
            "status": execution["status"],
            "collect_start": start,
            "collect_cutoff": cutoff,
            "row_count": row_count,
            "message": f"已提交重跑任务,新运行ID: {monitor_run['id']}",
        }


# ---------------------------------------------------------------------------
# 目标表写入与确认
# ---------------------------------------------------------------------------


def _monitor_run_for(execution: dict[str, Any]) -> dict[str, Any] | None:
    run_id = execution.get("monitor_run_id")
    if run_id is None:
        return None
    return store.find("pipeline_monitor", int(run_id))


def _refresh_monitor_run(execution: dict[str, Any]) -> dict[str, Any] | None:
    run = _monitor_run_for(execution)
    if run is None:
        return None
    run["execution_status"] = execution.get("status", run.get("execution_status"))
    run["confirm_status"] = (
        EXEC_COMPLETED if execution.get("status") == EXEC_COMPLETED
        else execution.get("status", run.get("confirm_status"))
    )
    run["execution_id"] = execution.get("id")
    run["pipeline_id"] = execution.get("pipeline_id")
    run["采集截止点"] = execution.get("collect_cutoff", "")
    return run


def apply_completion(
    execution_id: int | None = None,
    *,
    failure_id: int | None = None,
    run_id: int | None = None,
    row_count: int = 10,
    message_id: str | None = None,
    confirmed_cutoff: str | None = None,
    collect_cutoff: str | None = None,
) -> dict[str, Any]:
    """监控收到 ETL 完成消息：待确认 -> 已完成，处理行数按消息绝对值记账。

    同一份消息（同一 message_id，或同一执行已经完成）重复送达时直接返回
    首次结果，不重复累加行数、不重复改变状态。`collect_cutoff` 与
    `confirmed_cutoff` 是同义参数，兼容监控和 ETL 两侧的消息命名。
    """
    confirmed_cutoff = confirmed_cutoff or collect_cutoff
    with FLOW_LOCK:
        execution = find_execution(execution_id, failure_id=failure_id, run_id=run_id)
        if execution is None:
            return {"ok": False, "message": "未找到对应的执行记录"}
        exec_id = int(execution["id"])
        messages = store.aux_rows(COMPLETION_MESSAGES)

        if message_id:
            for message in messages:
                if message.get("message_id") == message_id:
                    return {
                        "ok": True,
                        "duplicate": True,
                        "execution_id": exec_id,
                        "run_id": execution.get("monitor_run_id"),
                        "row_count": message.get("row_count"),
                        "status": execution.get("status"),
                        "message": "完成消息已处理过，忽略重复投递",
                    }

        if execution.get("status") == EXEC_COMPLETED:
            return {
                "ok": True,
                "duplicate": True,
                "execution_id": exec_id,
                "run_id": execution.get("monitor_run_id"),
                "row_count": execution.get("confirmed_rows"),
                "status": execution.get("status"),
                "message": "该执行已完成，忽略重复的完成消息",
            }

        if execution.get("status") == EXEC_FAILED:
            # 已失败的执行可以被值班员用新失败号再跑一轮，但不能用完成消息
            # 把失败改写成完成，否则会出现「未落表却已确认」。
            return {
                "ok": False,
                "message": "该执行已失败，请用新失败号再次发起重跑",
            }

        if execution.get("status") not in (EXEC_PENDING_CONFIRM, EXEC_RUNNING):
            return {
                "ok": False,
                "message": f"执行当前状态为 {execution.get('status')}，不能确认",
            }

        confirmed_rows = int(row_count)
        cutoff = confirmed_cutoff or execution.get("collect_cutoff", "")
        # 完成消息声明的范围不能越过实际写入的采集截止点，避免状态跑在数据前面。
        written_cutoff = execution.get("collect_cutoff", "")
        if cutoff and written_cutoff:
            cutoff_dt, written_dt = parse_time(cutoff), parse_time(written_cutoff)
            if cutoff_dt is not None and written_dt is not None and cutoff_dt > written_dt:
                cutoff = written_cutoff
        execution["status"] = EXEC_COMPLETED
        execution["state"] = EXEC_COMPLETED
        execution["confirm_status"] = EXEC_COMPLETED
        execution["confirmed_rows"] = confirmed_rows
        execution["row_count"] = confirmed_rows
        execution["confirmed_range"] = (
            f"{execution.get('collect_start', '')}~{cutoff}" if cutoff else ""
        )
        execution["confirmed_at"] = next_run_time()
        execution["updated_at"] = execution["confirmed_at"]

        receipt = find_receipt_for_execution(exec_id)
        if receipt is not None:
            # 行数用赋值而不是累加，重复消息不会把 10 变成 20。
            receipt["confirmed"] = True
            receipt["confirm_status"] = EXEC_COMPLETED
            receipt["confirmed_row_count"] = confirmed_rows
            receipt["row_count"] = confirmed_rows
            receipt["confirmed_range"] = execution["confirmed_range"]
            receipt["confirmed_cutoff"] = cutoff
            receipt["confirmed_at"] = execution["confirmed_at"]
            receipt.setdefault("message_ids", [])
            if message_id and message_id not in receipt["message_ids"]:
                receipt["message_ids"].append(message_id)

        run = _refresh_monitor_run(execution)
        if run is not None:
            # 运行记录按题目约定从「待确认」流转到「已完成」。
            run["status"] = EXEC_COMPLETED
            run["pending"] = False
            run["abnormal"] = False
            run["执行状态"] = EXEC_COMPLETED
            run["处理行数"] = str(confirmed_rows)
            run["confirm_rows"] = confirmed_rows
            run["confirm_status"] = EXEC_COMPLETED
            run["execution_status"] = EXEC_COMPLETED
            run["confirmed_range"] = execution["confirmed_range"]
            run["错误信息"] = ""

        messages.append({
            "id": next_id(messages),
            "message_id": message_id or f"confirm-{exec_id}",
            "execution_id": exec_id,
            "monitor_run_id": execution.get("monitor_run_id"),
            "row_count": confirmed_rows,
            "confirmed_cutoff": cutoff,
            "applied": True,
            "received_at": execution["confirmed_at"],
        })

        return {
            "ok": True,
            "duplicate": False,
            "execution_id": exec_id,
            "run_id": execution.get("monitor_run_id"),
            "row_count": confirmed_rows,
            "status": execution["status"],
            "confirmed_range": execution["confirmed_range"],
            "message": f"确认完成: 运行ID {execution.get('monitor_run_id')} 共 {confirmed_rows} 行",
        }


def fail_execution(
    execution_id: int | None = None,
    *,
    failure_id: int | None = None,
    run_id: int | None = None,
    reason: str = "重跑执行失败",
    summary_cutoff: str | None = None,
) -> dict[str, Any]:
    """标记一次执行失败：不产生写入凭据，失败摘要也不得推进采集起点。"""
    with FLOW_LOCK:
        execution = find_execution(execution_id, failure_id=failure_id, run_id=run_id)
        if execution is None:
            return {"ok": False, "message": "未找到对应的执行记录"}
        if execution.get("status") == EXEC_COMPLETED:
            return {"ok": False, "message": "已完成的执行不能标记失败"}

        next_time = next_run_time()
        landed = latest_landed_cutoff(
            pipeline_id=execution.get("pipeline_id"),
            pipeline_name=execution.get("pipeline_name"),
        )
        # 失败摘要是「尝试到的边界」，始终晚于已落表边界，用于诊断和定位
        # 未写入范围；它不会进入水位线计算。
        summary = summary_cutoff or _failure_summary_cutoff(landed)
        execution["status"] = EXEC_FAILED
        execution["state"] = EXEC_FAILED
        execution["confirm_status"] = "未确认"
        execution["error"] = reason
        execution["failure_summary_cutoff"] = summary
        execution["failed_at"] = next_time
        execution["updated_at"] = next_time

        receipt = find_receipt_for_execution(int(execution["id"]))
        if receipt is not None:
            # 已有凭据但仍失败时，凭据标为未落表，水位线不得采用。
            receipt["landed"] = False
            receipt["confirm_status"] = "未确认"
            receipt["failure_reason"] = reason

        run = _refresh_monitor_run(execution)
        if run is not None:
            run["status"] = "执行失败"
            run["pending"] = True
            run["abnormal"] = True
            run["执行状态"] = "失败"
            run["错误信息"] = reason
            # 失败摘要里的采集边界只做诊断展示，estimate_extract_watermark 会忽略它。
            run["max_watermark"] = summary
            run["failure_summary_cutoff"] = summary
            run["execution_status"] = EXEC_FAILED
            run["confirm_status"] = "未确认"

        return {
            "ok": True,
            "execution_id": execution["id"],
            "run_id": execution.get("monitor_run_id"),
            "failure_id": execution.get("monitor_run_id"),
            "status": EXEC_FAILED,
            "failure_summary_cutoff": summary,
            "message": f"执行失败,新失败号: {execution.get('monitor_run_id')}",
        }


def execution_result(execution_id: int) -> dict[str, Any] | None:
    """按执行号返回写入凭据和确认的数据范围，供 ETL/监控/运行历史共用。"""
    execution = store.aux_find(EXECUTIONS, int(execution_id))
    if execution is None:
        return None
    receipt = find_receipt_for_execution(int(execution_id))
    order = None
    for row in store.aux_rows(RERUN_ORDERS):
        if int(row.get("execution_id") or 0) == int(execution_id):
            order = row
            break
    return {
        "execution_id": execution["id"],
        "run_id": execution.get("monitor_run_id"),
        "order_id": order.get("id") if order else None,
        "failure_id": execution.get("source_failure_id"),
        "pipeline_id": execution.get("pipeline_id"),
        "pipeline_name": execution.get("pipeline_name"),
        "target_table": execution.get("target_table"),
        "status": execution.get("status"),
        "execution_status": execution.get("status"),
        "confirm_status": execution.get("confirm_status", execution.get("status")),
        "landed": bool(receipt and receipt.get("landed")),
        "written_range": (receipt or {}).get("written_range", execution.get("written_range", "")),
        "confirmed_range": (receipt or {}).get("confirmed_range", execution.get("confirmed_range", "")),
        "collect_start": execution.get("collect_start"),
        "collect_cutoff": execution.get("collect_cutoff"),
        "row_count": execution.get("confirmed_rows") or execution.get("row_count", 0),
        "written_rows": (receipt or {}).get("written_rows", execution.get("written_rows", 0)),
        "confirmed_rows": execution.get("confirmed_rows", 0),
        "error": execution.get("error", ""),
        "execution_count_for_failure": len([
            row for row in store.aux_rows(EXECUTIONS)
            if execution.get("source_failure_id") is not None
            and int(row.get("source_failure_id") or 0) == int(execution["source_failure_id"])
        ]),
        "write_count": len([
            row for row in store.aux_rows(WRITE_RECEIPTS)
            if int(row.get("execution_id") or 0) == int(execution_id)
        ]),
        "receipt": receipt,
    }


# ---------------------------------------------------------------------------
# 增量水位线
# ---------------------------------------------------------------------------


def estimate_watermark(
    pipeline_id: int, pipeline_name: str | None = None
) -> dict[str, Any]:
    """增量抽取起点只取已落表写入凭据的采集截止点。

    失败摘要在运行记录里可能带着 max_watermark（例如 10:30），但它不代表
    数据已写入目标表，必须排除；未落表的执行同样不能改变起点。
    """
    pipeline = resolve_pipeline(pipeline_id=pipeline_id, pipeline_name=pipeline_name)
    if pipeline is not None:
        pipeline_name = pipeline.get("流水线名称", pipeline_name)
    name = pipeline_name or ""

    landed = landed_receipts(pipeline_id=pipeline_id, pipeline_name=name)
    landed_cutoff = latest_landed_cutoff(pipeline_id=pipeline_id, pipeline_name=name)
    confirmed_cutoff = latest_confirmed_cutoff(pipeline_id=pipeline_id, pipeline_name=name)

    excluded: list[dict[str, Any]] = []
    seen_runs: set[int] = set()
    for execution in pipeline_executions(pipeline_id=pipeline_id, pipeline_name=name):
        run_id = execution.get("monitor_run_id")
        if run_id is not None:
            seen_runs.add(int(run_id))
        if execution.get("status") in (EXEC_COMPLETED, EXEC_PENDING_CONFIRM) \
                and find_receipt_for_execution(int(execution["id"])) is not None:
            continue
        excluded.append({
            "execution_id": execution.get("id"),
            "run_id": run_id,
            "cutoff": execution.get("failure_summary_cutoff")
            or execution.get("attempted_cutoff", ""),
            "reason": "执行未落表" if execution.get("status") != EXEC_COMPLETED else "无写入凭据",
        })

    for run in store.rows("pipeline_monitor"):
        if run.get("流水线名称") != name:
            continue
        # 种子运行记录没有 pipeline_id，按名称归属；有 id 的必须与当前流水线一致。
        if run.get("pipeline_id") is not None \
                and str(run.get("pipeline_id")) != str(pipeline_id):
            continue
        if run.get("id") in seen_runs:
            continue
        if run.get("max_watermark") or run.get("failure_summary_cutoff"):
            excluded.append({
                "execution_id": run.get("execution_id"),
                "run_id": run.get("id"),
                "cutoff": run.get("max_watermark") or run.get("failure_summary_cutoff"),
                "reason": "失败摘要不代表已写入",
            })

    latest_run = _latest_monitor_run(name) if name else None
    return {
        "pipeline_id": pipeline_id,
        "pipeline_name": name,
        "last_watermark": landed_cutoff,
        "next_extract_start": landed_cutoff or DEFAULT_COLLECT_START,
        "confirmed_watermark": confirmed_cutoff,
        "watermark_source": "write_receipt" if landed_cutoff else "none",
        "landed_receipt_count": len(landed),
        "excluded_unlanded": excluded,
        "estimated_lag_seconds": 0 if not landed_cutoff else 300,
        "lag_category": "normal" if not landed_cutoff else "normal",
        "last_run_status": latest_run.get("status", "从未运行") if latest_run else "从未运行",
    }
