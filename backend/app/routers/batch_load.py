"""批量导入专用接口：多组原始数据归并、分组可构建行集、局部派发与类型纠正。

通用资源路由只负责导入任务的列表/登记/动作；这里补的是题面要求的批量入口，
路径都挂在 ``/api/batch_load/batches`` 下，避免和 ``/api/{module}/{entry_id}`` 抢匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services import service_for

router = APIRouter(prefix="/api/batch_load", tags=["批量导入 · 多组归并"])


class BatchCreatePayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    groups: list[dict[str, Any]] = Field(default_factory=list)
    target_graph: str | None = None
    remark: str | None = None


class GroupsPayload(BaseModel):
    groups: list[dict[str, Any]] = Field(default_factory=list)


class DispatchPayload(BaseModel):
    group_ids: list[str] = Field(default_factory=list)
    remark: str | None = None


class ResolutionPayload(BaseModel):
    key: str
    type: str
    group_id: str | None = None
    line_no: int | None = None


def _service():
    service = service_for("batch_load")
    if service is None:  # pragma: no cover - 注册表恒定包含 batch_load
        raise HTTPException(status_code=404, detail="批量导入服务未注册")
    return service


def _call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except KeyError as error:
        raise HTTPException(status_code=404, detail=str(error))
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))


@router.get("/batches")
def list_batches() -> dict[str, Any]:
    return {"items": _service().list_batches()}


@router.post("/batches")
def create_batch(payload: BatchCreatePayload) -> dict[str, Any]:
    values = dict(payload.values)
    if payload.target_graph and "目标图谱" not in values:
        values["目标图谱"] = payload.target_graph
    if payload.groups and "groups" not in values:
        values["groups"] = payload.groups
    return _call(_service().create_batch, values)


@router.get("/batches/{batch_id}")
def get_batch(batch_id: int) -> dict[str, Any]:
    return _call(_service().get_batch, batch_id)


@router.post("/batches/{batch_id}/groups")
def submit_groups(batch_id: int, payload: GroupsPayload) -> dict[str, Any]:
    return _call(_service().submit_groups, batch_id, {"groups": payload.groups})


@router.get("/batches/{batch_id}/rowset")
def get_rowset(batch_id: int) -> dict[str, Any]:
    return _call(_service().rowset, batch_id)


@router.post("/batches/{batch_id}/dispatch")
def dispatch(batch_id: int, payload: DispatchPayload) -> dict[str, Any]:
    return _call(
        _service().dispatch, batch_id, payload.group_ids, payload.remark
    )


@router.get("/batches/{batch_id}/results")
def build_results(batch_id: int) -> dict[str, Any]:
    return _call(_service().build_results, batch_id)


@router.get("/batches/{batch_id}/claimable")
def claimable(batch_id: int) -> dict[str, Any]:
    return _call(_service().claimable, batch_id)


@router.post("/batches/{batch_id}/resolutions")
def resolve(batch_id: int, payload: ResolutionPayload) -> dict[str, Any]:
    return _call(
        _service().resolve_candidate,
        batch_id,
        payload.key,
        payload.type,
        payload.group_id,
        payload.line_no,
    )
