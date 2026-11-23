"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    values = dict(payload.payload or {})
    if payload.remark is not None:
        values.setdefault("remark", payload.remark)
    entry, message = service.run_action(entry_id, payload.action.strip(), values)
    return ActionResult(ok=entry is not None, message=message, entry=entry)


@router.post("/{module}/{entry_id}/execute", response_model=dict)
def execute_pipeline(module: str, entry_id: int, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """触发流水线执行：运行记录与内部执行单一一对应。"""
    service = require_service(module)
    executor = getattr(service, "execute_pipeline", None)
    if executor is None:
        raise HTTPException(status_code=404, detail=f"{module} 不支持执行")
    values = payload or {}
    return executor(entry_id, bool(values.get("force_full")))


@router.post("/{module}/{entry_id}/fail", response_model=dict)
def fail_execution(module: str, entry_id: int, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """把某轮执行标记为失败，供值班员用新失败号再次发起重跑。"""
    service = require_service(module)
    failer = getattr(service, "mark_execution_failed", None)
    if failer is None:
        raise HTTPException(status_code=404, detail=f"{module} 不支持失败登记")
    values = dict(payload or {})
    values.setdefault("execution_id", entry_id)
    return failer(values)


@router.get("/{module}/{entry_id}/rerun/{failure_id}", response_model=dict)
def get_rerun_by_failure(module: str, entry_id: int, failure_id: int) -> dict[str, Any]:
    """按原失败号找回该轮重跑的执行与目标表写入凭据。"""
    service = require_service(module)
    resolver = getattr(service, "get_rerun_by_failure", None)
    if resolver is None:
        raise HTTPException(status_code=404, detail=f"{module} 不支持重跑查询")
    result = resolver(failure_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message", "未找到重跑执行"))
    return result


@router.post("/{module}/rerun", response_model=dict)
def accept_rerun(module: str, payload: dict[str, Any]) -> dict[str, Any]:
    """受理重跑单：同一原失败号重复提交只派生一条运行记录。"""
    service = require_service(module)
    acceptor = getattr(service, "accept_rerun_order", None)
    if acceptor is None:
        raise HTTPException(status_code=404, detail=f"{module} 不支持受理重跑单")
    return acceptor(payload)


@router.get("/{module}/{entry_id}/result", response_model=dict)
def get_execution_result(module: str, entry_id: int) -> dict[str, Any]:
    """查询某轮执行的结果数据写入范围与确认的数据范围。"""
    service = require_service(module)
    resolver = getattr(service, "get_execution_result", None)
    if resolver is None:
        raise HTTPException(status_code=404, detail=f"{module} 不支持结果查询")
    result = resolver(execution_id=entry_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message", "未找到执行"))
    return result


@router.post("/{module}/{entry_id}/confirm", response_model=dict)
def confirm_execution(module: str, entry_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    """接收 ETL 完成消息：待确认 -> 已完成，重复投递不重复记行数。"""
    service = require_service(module)
    confirmer = getattr(service, "confirm_write_result", None)
    if confirmer is None:
        raise HTTPException(status_code=404, detail=f"{module} 不支持完成确认")
    values = dict(payload)
    values.setdefault("execution_id", entry_id)
    return confirmer(values)


@router.get("/{module}/{entry_id}/watermark", response_model=dict)
def get_watermark(module: str, entry_id: int) -> dict[str, Any]:
    """查询下一次增量抽取起点：只取已落表写入凭据的采集截止点。"""
    service = require_service(module)
    estimator = getattr(service, "estimate_extract_watermark", None)
    if estimator is None:
        raise HTTPException(status_code=404, detail=f"{module} 不支持水位线估算")
    return estimator(entry_id)
