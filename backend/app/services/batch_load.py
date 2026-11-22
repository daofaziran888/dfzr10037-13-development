"""批量导入领域服务。

除了通用的导入任务字段外，这个服务承担「多组原始数据 → 同一张图谱」的归并入口：

* 原始输入按组提交，每条来源行用 ``(组编号, 行号)`` 定位，整组重复提交只落一次；
* 同一个实体键在多个组里声明了不同类型时，候选类型全部保留（含原始组与行号），
  此时不把类型相异的同键认作重复，而是标成待定端点；
* 归并产物是「分组可构建行集」：只有两个端点都已确定的边才可建立；
* 局部派发把可建立的边交给图谱构建，写进构建结果，并把接收结果回挂到批量处理清单；
* 端点类型纠正（例如 D18）后，原本待定的边会重新变成可继续领的行。
"""
from __future__ import annotations

from datetime import date
from typing import Any

from app.seed import SEED_BATCH_GROUPS
from app.services.base import BaseModuleService
from app.store import store

SEED_BATCH_ID = 4
DEFAULT_TARGET_GRAPH = "设备关系图谱"


# ---------------------------------------------------------------------------
# 原始行归一化
# ---------------------------------------------------------------------------
def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip() or default


def _first(raw: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in raw:
            value = raw[key]
            if value is not None and str(value).strip() != "":
                return value
    return default


def _to_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _today() -> str:
    return date.today().isoformat()


def _normalize_row(
    raw: dict[str, Any],
    *,
    fallback_line: int,
    group_id: str,
    errors: list[str],
) -> dict[str, Any] | None:
    """把一条来源行归一成实体声明或边，保留行号供后续追溯。"""
    if not isinstance(raw, dict):
        errors.append(f"组 {group_id} 第 {fallback_line} 行不是对象")
        return None

    line_no = _to_int(
        _first(raw, "line_no", "行号", "line", "序号", default=fallback_line),
        fallback_line,
    )
    raw_type = _text(_first(raw, "row_type", "行类型", "kind", default="")).lower()
    source = _text(
        _first(raw, "source", "source_key", "from", "主体", "主体键", "起点")
    )
    target = _text(
        _first(raw, "target", "target_key", "to", "客体", "客体键", "终点")
    )

    if raw_type in {"edge", "关系", "边"}:
        is_edge = True
    elif raw_type in {"entity", "实体", "节点"}:
        is_edge = False
    else:
        is_edge = bool(source or target)

    if is_edge:
        if not source or not target:
            errors.append(f"组 {group_id} 第 {line_no} 行的边缺少起点或终点")
            return None
        edge_key = _text(
            _first(raw, "edge_key", "边编号", "edge_id", "编号"),
            f"{group_id}-E{line_no}",
        )
        return {
            "line_no": line_no,
            "row_type": "edge",
            "edge_key": edge_key,
            "relation": _text(
                _first(raw, "relation", "relation_type", "关系", "关系类型", "predicate"),
                "关联",
            ),
            "source": source,
            "target": target,
        }

    key = _text(
        _first(
            raw,
            "key",
            "entity_key",
            "实体键",
            "实体编号",
            "设备编号",
            "编号",
            "name",
            "名称",
        )
    )
    entity_type = _text(_first(raw, "type", "entity_type", "类型", "实体类型"))
    if not key or not entity_type:
        errors.append(f"组 {group_id} 第 {line_no} 行的实体声明缺少键或类型")
        return None
    return {
        "line_no": line_no,
        "row_type": "entity",
        "key": key,
        "type": entity_type,
    }


def _normalize_group(
    raw: dict[str, Any], *, index: int, errors: list[str]
) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        errors.append(f"第 {index + 1} 组不是对象")
        return None
    group_id = _text(
        _first(raw, "group_id", "组编号", "group", "id", "name", "组名", "名称"),
        f"G{index + 1}",
    )
    name = _text(_first(raw, "name", "组名", "group_name", "名称"), group_id)
    raw_rows = _first(raw, "rows", "行", "source_rows", "来源行", default=[]) or []
    if isinstance(raw_rows, dict):
        raw_rows = [
            {"line_no": line, **row} if isinstance(row, dict) else row
            for line, row in raw_rows.items()
        ]
    if not isinstance(raw_rows, list):
        errors.append(f"组 {group_id} 的 rows 必须是数组")
        return None

    rows: dict[int, dict[str, Any]] = {}
    for offset, raw_row in enumerate(raw_rows):
        row = _normalize_row(
            raw_row,
            fallback_line=offset + 1,
            group_id=group_id,
            errors=errors,
        )
        if row is not None:
            rows[int(row["line_no"])] = row
    return {"group_id": group_id, "name": name, "rows": rows}


# ---------------------------------------------------------------------------
# 服务
# ---------------------------------------------------------------------------
class BatchLoadService(BaseModuleService):
    MODULE = "batch_load"
    ENTITY = "导入任务"
    OPERATION_TERM = "导入"
    LIST_FIELDS = ["任务编号", "数据文件", "目标图谱", "映射规则", "校验模式", "导入行数", "跳过行数", "导入时间"]
    REQUIRED_FIELDS = ["任务编号", "数据文件", "目标图谱"]
    STATUS_ORDER = ["待执行", "导入中", "已完成", "部分跳过"]
    ACTION_RULES = {"启动导入": "导入中", "暂停导入": "已完成", "标记跳过": "部分跳过"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._batches: dict[int, dict[str, Any]] = {}
        self._seed()

    # ===== 批次的内部记录 =====
    def _base_batch(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(row.get("id", 0)),
            "row": row,
            "任务编号": str(row.get("任务编号", "")),
            "数据文件": str(row.get("数据文件", "")),
            "目标图谱": str(row.get("目标图谱", "")),
            "groups": {},
            "resolutions": {},
            "build_edges": [],
            "dispatches": [],
            "next_dispatch": 1,
        }

    def _seed(self) -> None:
        row = store.find(self.MODULE, SEED_BATCH_ID)
        if row is None:
            return
        batch = self._base_batch(row)
        for index, raw_group in enumerate(SEED_BATCH_GROUPS):
            errors: list[str] = []
            group = _normalize_group(raw_group, index=index, errors=errors)
            if group is not None:
                batch["groups"][group["group_id"]] = group
        self._batches[batch["id"]] = batch
        self._refresh(batch)

    def _batch(self, batch_id: int) -> dict[str, Any]:
        batch = self._batches.get(batch_id)
        if batch is not None:
            return batch
        row = store.find(self.MODULE, batch_id)
        if row is None:
            raise KeyError(f"导入任务 {batch_id} 不存在或已归档")
        batch = self._base_batch(row)
        self._batches[batch_id] = batch
        return batch

    # ===== 归并计算 =====
    def _candidate_map(self, batch: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        """实体键 → 候选类型列表，每个候选保留原始组与行号。"""
        candidates: dict[str, list[dict[str, Any]]] = {}
        for group_id in sorted(batch["groups"]):
            group = batch["groups"][group_id]
            for line_no in sorted(group["rows"]):
                row = group["rows"][line_no]
                if row.get("row_type") != "entity":
                    continue
                candidates.setdefault(row["key"], []).append(
                    {
                        "key": row["key"],
                        "type": row["type"],
                        "group_id": group_id,
                        "line_no": line_no,
                    }
                )
        return candidates

    def _determined(
        self,
        batch: dict[str, Any],
        candidates: dict[str, list[dict[str, Any]]],
    ) -> dict[str, dict[str, Any]]:
        determined: dict[str, dict[str, Any]] = {}
        for key in sorted(candidates):
            resolution = batch["resolutions"].get(key)
            if resolution and resolution.get("type"):
                determined[key] = {
                    "key": key,
                    "type": resolution["type"],
                    "source": "人工纠正",
                    "resolved_from": [
                        {
                            "type": item["type"],
                            "group_id": item["group_id"],
                            "line_no": item["line_no"],
                        }
                        for item in candidates[key]
                    ],
                }
                continue
            types: list[str] = []
            for item in candidates[key]:
                if item["type"] not in types:
                    types.append(item["type"])
            if len(types) == 1:
                determined[key] = {"key": key, "type": types[0], "source": "候选唯一"}
        return determined

    @staticmethod
    def _endpoint_state(
        key: str,
        candidates: dict[str, list[dict[str, Any]]],
        determined: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        if key in determined:
            return {
                "key": key,
                "status": "confirmed",
                "type": determined[key]["type"],
                "source": determined[key]["source"],
            }
        items = candidates.get(key, [])
        if not items:
            return {
                "key": key,
                "status": "pending",
                "reason": "缺少端点声明",
                "candidates": [],
            }
        return {
            "key": key,
            "status": "pending",
            "reason": "类型冲突",
            "candidates": items,
        }

    def _edge_records(self, batch: dict[str, Any]) -> list[dict[str, Any]]:
        candidates = self._candidate_map(batch)
        determined = self._determined(batch, candidates)
        built = {
            (edge["来源组"], int(edge["来源行"])) for edge in batch["build_edges"]
        }
        records: list[dict[str, Any]] = []
        for group_id in sorted(batch["groups"]):
            group = batch["groups"][group_id]
            for line_no in sorted(group["rows"]):
                row = group["rows"][line_no]
                if row.get("row_type") != "edge":
                    continue
                source_state = self._endpoint_state(row["source"], candidates, determined)
                target_state = self._endpoint_state(row["target"], candidates, determined)
                pending_endpoints = [
                    state
                    for state in (source_state, target_state)
                    if state["status"] != "confirmed"
                ]
                reasons = []
                for state in pending_endpoints:
                    if state["reason"] not in reasons:
                        reasons.append(state["reason"])
                records.append(
                    {
                        "group_id": group_id,
                        "group_name": group["name"],
                        "line_no": line_no,
                        "edge_key": row["edge_key"],
                        "relation": row["relation"],
                        "source": row["source"],
                        "target": row["target"],
                        "source_state": source_state,
                        "target_state": target_state,
                        "source_type": source_state.get("type"),
                        "target_type": target_state.get("type"),
                        "buildable": not pending_endpoints,
                        "built": (group_id, line_no) in built,
                        "pending_endpoints": pending_endpoints,
                        "reason": "；".join(reasons) if reasons else "端点已确定",
                    }
                )
        return records

    def _snapshot(self, batch: dict[str, Any]) -> dict[str, Any]:
        candidates = self._candidate_map(batch)
        determined = self._determined(batch, candidates)
        records = self._edge_records(batch)
        claimable = [r for r in records if r["buildable"] and not r["built"]]
        pending = [r for r in records if not r["buildable"]]
        built = [r for r in records if r["built"]]
        return {
            "candidates": candidates,
            "determined": determined,
            "records": records,
            "claimable": claimable,
            "pending": pending,
            "built": built,
        }

    @staticmethod
    def _pending_endpoint_list(
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        seen: dict[str, dict[str, Any]] = {}
        for record in records:
            for state in record["pending_endpoints"]:
                key = state["key"]
                if key not in seen:
                    seen[key] = {
                        **state,
                        "翻车组": [],
                        "edges": [],
                    }
                if record["group_id"] not in seen[key]["翻车组"]:
                    seen[key]["翻车组"].append(record["group_id"])
                seen[key]["edges"].append(
                    {
                        "group_id": record["group_id"],
                        "line_no": record["line_no"],
                        "edge_key": record["edge_key"],
                    }
                )
        return [seen[key] for key in sorted(seen)]

    # ===== 刷新汇总行 =====
    def _refresh(self, batch: dict[str, Any]) -> None:
        snapshot = self._snapshot(batch)
        row = batch["row"]
        source_rows = sum(
            len(group["rows"]) for group in batch["groups"].values()
        )
        pending_edges = snapshot["pending"]
        claimable = snapshot["claimable"]
        if source_rows == 0:
            status = "待执行"
        elif pending_edges:
            status = "部分跳过"
        elif claimable:
            status = "导入中"
        else:
            # 没有待定边、也没有待派发边：要么全部已建立，要么只有实体声明。
            status = "已完成"
        row["status"] = status
        row["pending"] = status != "已完成"
        row["abnormal"] = bool(pending_edges)
        row["导入行数"] = source_rows
        row["跳过行数"] = len(pending_edges)
        row["校验模式"] = row.get("校验模式") or "分组归并"
        row["映射规则"] = row.get("映射规则") or "多组映射→同一图谱"
        row["导入时间"] = _today()

    # ===== 对外：批次登记 / 列表 / 详情 =====
    def list_batches(self) -> list[dict[str, Any]]:
        summaries = []
        for batch_id in sorted(self._batches):
            batch = self._batches[batch_id]
            snapshot = self._snapshot(batch)
            summaries.append(
                {
                    "id": batch["id"],
                    "任务编号": batch["任务编号"],
                    "数据文件": batch["数据文件"],
                    "目标图谱": batch["目标图谱"],
                    "status": batch["row"].get("status", "待执行"),
                    "来源行数": sum(
                        len(group["rows"]) for group in batch["groups"].values()
                    ),
                    "已建立边数": len(snapshot["built"]),
                    "可建立边数": len(snapshot["claimable"]),
                    "待定边数": len(snapshot["pending"]),
                }
            )
        return summaries

    def create_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        groups_payload = _first(payload, "groups", "组", "分组", default=[]) or []
        target_graph = _text(
            _first(payload, "目标图谱", "target_graph", "target", "图谱"),
            DEFAULT_TARGET_GRAPH,
        )
        if not groups_payload:
            raise ValueError("至少提供一组原始数据")
        if not target_graph:
            raise ValueError("缺少目标图谱")

        rows = store.rows(self.MODULE)
        batch_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        row: dict[str, Any] = {
            "id": batch_id,
            "status": "待执行",
            "pending": True,
            "abnormal": False,
            "任务编号": _text(
                _first(payload, "任务编号", "batch_no"),
                f"BATC-{batch_id:04d}",
            ),
            "数据文件": _text(
                _first(payload, "数据文件", "source_file"),
                f"多组归并输入-{batch_id:04d}",
            ),
            "目标图谱": target_graph,
            "映射规则": _text(
                _first(payload, "映射规则", "mapping_rule"),
                "多组映射→同一图谱",
            ),
            "校验模式": _text(
                _first(payload, "校验模式", "validate_mode"),
                "分组归并",
            ),
            "导入行数": 0,
            "跳过行数": 0,
            "导入时间": _today(),
        }
        rows.append(row)
        batch = self._base_batch(row)
        self._batches[batch_id] = batch
        self.submit_groups(batch_id, {"groups": groups_payload})
        return self.get_batch(batch_id)

    def get_batch(self, batch_id: int) -> dict[str, Any]:
        batch = self._batch(batch_id)
        snapshot = self._snapshot(batch)
        groups_out: list[dict[str, Any]] = []
        for group_id in sorted(batch["groups"]):
            group = batch["groups"][group_id]
            records = [
                record
                for record in snapshot["records"]
                if record["group_id"] == group_id
            ]
            pending_records = [record for record in records if not record["buildable"]]
            groups_out.append(
                {
                    "group_id": group_id,
                    "name": group["name"],
                    "来源行数": len(group["rows"]),
                    "实体行数": sum(
                        1 for row in group["rows"].values()
                        if row["row_type"] == "entity"
                    ),
                    "边行数": sum(
                        1 for row in group["rows"].values()
                        if row["row_type"] == "edge"
                    ),
                    "已建立边": [r for r in records if r["built"]],
                    "可建立边": [
                        r for r in records if r["buildable"] and not r["built"]
                    ],
                    "待定边": pending_records,
                    "待定端点": self._pending_endpoint_list(pending_records),
                    "source_rows": [
                        group["rows"][line] for line in sorted(group["rows"])
                    ],
                }
            )

        pending_endpoints = self._pending_endpoint_list(snapshot["pending"])
        candidates_flat = [
            item
            for key in sorted(snapshot["candidates"])
            for item in snapshot["candidates"][key]
        ]
        status = batch["row"].get("status", "待执行")
        return {
            "ok": True,
            "id": batch["id"],
            "batch_id": batch["id"],
            "任务编号": batch["任务编号"],
            "batch_no": batch["任务编号"],
            "数据文件": batch["数据文件"],
            "目标图谱": batch["目标图谱"],
            "target_graph": batch["目标图谱"],
            "status": status,
            "清单编号": batch["任务编号"],
            "来源行数": sum(
                len(group["rows"]) for group in batch["groups"].values()
            ),
            "已建立边数": len(snapshot["built"]),
            "可建立边数": len(snapshot["claimable"]),
            "待定边数": len(snapshot["pending"]),
            "待定端点数": len(pending_endpoints),
            "groups": groups_out,
            "候选类型": candidates_flat,
            "candidates": candidates_flat,
            "candidate_map": snapshot["candidates"],
            "resolutions": batch["resolutions"],
            "determined_entities": snapshot["determined"],
            "已建立边": batch["build_edges"],
            "build_edges": batch["build_edges"],
            "可建立边": snapshot["claimable"],
            "claimable_rows": snapshot["claimable"],
            "能继续领的行": snapshot["claimable"],
            "待定边": snapshot["pending"],
            "pending_edges": snapshot["pending"],
            "待定端点": pending_endpoints,
            "pending_endpoints": pending_endpoints,
            "dispatches": [
                {
                    "dispatch_id": item["dispatch_id"],
                    "序号": item["序号"],
                    "构建任务编号": item["构建任务编号"],
                    "选中组": item["选中组"],
                    "接收边数": item["接收边数"],
                    "待定边数": len(item["待定边"]),
                    "处理时间": item["处理时间"],
                    "关联清单": item["关联清单"],
                }
                for item in batch["dispatches"]
            ],
        }

    def rowset(self, batch_id: int) -> dict[str, Any]:
        """分组可构建行集：交给图谱构建前的稳定视图。"""
        detail = self.get_batch(batch_id)
        groups = [
            {
                "group_id": group["group_id"],
                "name": group["name"],
                "可构建行集": group["可建立边"],
                "待定行": group["待定边"],
                "待定端点": group["待定端点"],
            }
            for group in detail["groups"]
        ]
        return {
            "ok": True,
            "batch_id": batch_id,
            "任务编号": detail["任务编号"],
            "目标图谱": detail["目标图谱"],
            "groups": groups,
            "可构建行数": len(detail["可建立边"]),
            "待定行数": len(detail["待定边"]),
            "候选类型": detail["候选类型"],
        }

    # ===== 对外：提交原始组（幂等） =====
    def submit_groups(self, batch_id: int, payload: Any) -> dict[str, Any]:
        batch = self._batch(batch_id)
        if isinstance(payload, list):
            groups_payload = payload
        elif isinstance(payload, dict) and ("groups" in payload or "组" in payload):
            groups_payload = _first(payload, "groups", "组", "分组", default=[]) or []
        elif isinstance(payload, dict):
            groups_payload = [payload]
        else:
            raise ValueError("groups 必须是数组")

        errors: list[str] = []
        normalized: list[dict[str, Any]] = []
        for index, raw_group in enumerate(groups_payload):
            group = _normalize_group(raw_group, index=index, errors=errors)
            if group is not None:
                normalized.append(group)
        if errors:
            raise ValueError("；".join(errors))
        if not normalized:
            raise ValueError("没有可写入的原始行")

        added = 0
        updated = 0
        skipped = 0
        for normalized_group in normalized:
            group_id = normalized_group["group_id"]
            group = batch["groups"].setdefault(
                group_id,
                {"group_id": group_id, "name": normalized_group["name"], "rows": {}},
            )
            group["name"] = normalized_group["name"] or group["name"]
            for line_no, row in normalized_group["rows"].items():
                old = group["rows"].get(line_no)
                if old is None:
                    group["rows"][line_no] = row
                    added += 1
                elif old == row:
                    # 整组重复提交：同一条来源行只落一次。
                    skipped += 1
                else:
                    group["rows"][line_no] = row
                    updated += 1

        self._refresh(batch)
        return {
            "ok": True,
            "message": f"新增 {added} 行，原样跳过 {skipped} 行，覆盖 {updated} 行",
            "batch_id": batch_id,
            "新增行": added,
            "跳过行": skipped,
            "覆盖行": updated,
            "added_rows": added,
            "skipped_rows": skipped,
            "updated_rows": updated,
            "batch": self.get_batch(batch_id),
        }

    # ===== 对外：局部派发到图谱构建 =====
    def dispatch(
        self,
        batch_id: int,
        group_ids: list[str] | None = None,
        remark: str | None = None,
    ) -> dict[str, Any]:
        batch = self._batch(batch_id)
        snapshot = self._snapshot(batch)
        all_groups = sorted(batch["groups"])
        selected = [str(item) for item in (group_ids or []) if str(item).strip()]
        selected = selected or all_groups
        unknown = [group_id for group_id in selected if group_id not in batch["groups"]]
        if unknown:
            raise ValueError(f"未知分组：{'、'.join(unknown)}")

        to_build = [
            record
            for record in snapshot["records"]
            if record["group_id"] in selected
            and record["buildable"]
            and not record["built"]
        ]
        already_built = [
            record
            for record in snapshot["records"]
            if record["group_id"] in selected and record["built"]
        ]
        pending_records = [
            record
            for record in snapshot["records"]
            if record["group_id"] in selected and not record["buildable"]
        ]

        sequence = int(batch["next_dispatch"])
        dispatch_id = f"DISP-{batch['id']:04d}-{sequence:02d}"
        task: dict[str, Any] | None = None
        accepted_edges: list[dict[str, Any]] = []
        if to_build:
            task = self._create_build_task(
                batch, dispatch_id, selected, to_build, pending_records, remark
            )
            for record in to_build:
                accepted = self._build_edge(record, batch, task, dispatch_id)
                batch["build_edges"].append(accepted)
                accepted_edges.append(accepted)
        batch["next_dispatch"] = sequence + 1

        # 派发后的最新快照：用于按组报告已可建立 / 仍待定的端点。
        after = self._snapshot(batch)
        group_results = []
        for group_id in all_groups:
            group = batch["groups"][group_id]
            records = [r for r in after["records"] if r["group_id"] == group_id]
            pending_group = [r for r in records if not r["buildable"]]
            group_results.append(
                {
                    "group_id": group_id,
                    "name": group["name"],
                    "selected": group_id in selected,
                    "已建立边": [r for r in records if r["built"]],
                    "可建立未派发边": [
                        r for r in records if r["buildable"] and not r["built"]
                    ],
                    "待定边": pending_group,
                    "待定端点": self._pending_endpoint_list(pending_group),
                    "本次接收边数": sum(
                        1 for r in to_build if r["group_id"] == group_id
                    ),
                }
            )

        record = {
            "dispatch_id": dispatch_id,
            "序号": sequence,
            "批次ID": batch["id"],
            "批次": batch["任务编号"],
            "关联清单": batch["任务编号"],
            "图谱构建任务": task["id"] if task else None,
            "构建任务编号": task["任务编号"] if task else "",
            "选中组": selected,
            "接收的边": accepted_edges,
            "接收边数": len(accepted_edges),
            "已存在跳过": len(already_built),
            "待定边": [
                {
                    "group_id": r["group_id"],
                    "line_no": r["line_no"],
                    "edge_key": r["edge_key"],
                    "reason": r["reason"],
                    "pending_endpoints": r["pending_endpoints"],
                }
                for r in pending_records
            ],
            "待定端点": self._pending_endpoint_list(pending_records),
            "分组结果": group_results,
            "处理时间": _today(),
            "备注": _text(remark, ""),
        }
        batch["dispatches"].append(record)
        self._refresh(batch)

        message = (
            f"已向图谱构建 {task['任务编号']} 写入 {len(accepted_edges)} 条边"
            if task
            else "当前选中组没有可建立的边，未创建构建任务"
        )
        return {
            "ok": True,
            "message": message,
            "dispatch_id": dispatch_id,
            "batch_id": batch["id"],
            "清单编号": batch["任务编号"],
            "build_task_id": task["id"] if task else None,
            "构建任务编号": task["任务编号"] if task else "",
            "接收边数": len(accepted_edges),
            "接收的边": accepted_edges,
            "accepted_edges": accepted_edges,
            "已存在跳过": len(already_built),
            "待定边": record["待定边"],
            "待定端点": record["待定端点"],
            "pending_endpoints": record["待定端点"],
            "groups": group_results,
            "分组结果": group_results,
            "接收结果": record,
            "dispatch": record,
            "batch": self.get_batch(batch_id),
        }

    def _create_build_task(
        self,
        batch: dict[str, Any],
        dispatch_id: str,
        selected: list[str],
        to_build: list[dict[str, Any]],
        pending_records: list[dict[str, Any]],
        remark: str | None,
    ) -> dict[str, Any]:
        rows = store.rows("graph_build")
        task_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        entity_keys = sorted(
            {
                key
                for record in to_build
                for key in (record["source"], record["target"])
            }
        )
        task: dict[str, Any] = {
            "id": task_id,
            "status": "构建完成",
            "pending": False,
            "abnormal": False,
            "任务编号": f"GRAP-BATC-{batch['id']:04d}-{int(batch['next_dispatch']):02d}",
            "数据范围": f"{batch['数据文件']} / {','.join(selected)}",
            "图类型": batch["目标图谱"],
            "实体数量": len(entity_keys),
            "关系数量": len(to_build),
            "构建模式": "局部派发" if len(selected) < len(batch["groups"]) else "批量归并",
            "增量标记": "增量",
            "构建时间": _today(),
            "关联批次": batch["任务编号"],
            "关联批次ID": batch["id"],
            "关联派发": dispatch_id,
            "关联清单": batch["任务编号"],
            "构建结果": [],
            "待定端点": self._pending_endpoint_list(pending_records),
            "备注": _text(remark, ""),
        }
        rows.append(task)
        return task

    def _build_edge(
        self,
        record: dict[str, Any],
        batch: dict[str, Any],
        task: dict[str, Any],
        dispatch_id: str,
    ) -> dict[str, Any]:
        edge = {
            "边编号": f"EDGE-{batch['id']:04d}-{record['group_id']}-{record['line_no']}",
            "edge_key": record["edge_key"],
            "关系": record["relation"],
            "关系类型": record["relation"],
            "主体键": record["source"],
            "主体类型": record["source_type"],
            "客体键": record["target"],
            "客体类型": record["target_type"],
            "来源组": record["group_id"],
            "来源行": record["line_no"],
            "line_no": record["line_no"],
            "批次": batch["任务编号"],
            "目标图谱": batch["目标图谱"],
            "构建任务": task["id"],
            "构建任务编号": task["任务编号"],
            "派发": dispatch_id,
            "状态": "已建立",
        }
        task["构建结果"].append(edge)
        return edge

    # ===== 对外：候选类型纠正 =====
    def resolve_candidate(
        self,
        batch_id: int,
        key: str,
        entity_type: str,
        group_id: str | None = None,
        line_no: int | None = None,
    ) -> dict[str, Any]:
        batch = self._batch(batch_id)
        key = _text(key)
        entity_type = _text(entity_type)
        if not key:
            raise ValueError("缺少要纠正的实体键")
        if not entity_type:
            raise ValueError("缺少纠正后的类型")

        candidates = self._candidate_map(batch).get(key, [])
        origin_group = _text(group_id)
        origin_line = line_no
        if origin_group and origin_line is None:
            for item in candidates:
                if item["group_id"] == origin_group:
                    origin_line = item["line_no"]
                    break
        batch["resolutions"][key] = {
            "type": entity_type,
            "group_id": origin_group,
            "line_no": int(origin_line) if origin_line is not None else None,
            "resolved_at": _today(),
            "source": "人工纠正",
        }
        self._refresh(batch)
        detail = self.get_batch(batch_id)
        return {
            "ok": True,
            "message": f"已纠正 {key} 的类型为 {entity_type}",
            "batch_id": batch_id,
            "key": key,
            "type": entity_type,
            "候选类型": candidates,
            "可建立边数": detail["可建立边数"],
            "待定端点数": detail["待定端点数"],
            "能继续领的行": detail["claimable_rows"],
            "batch": detail,
        }

    # ===== 对外：构建结果 / 可领行 =====
    def build_results(self, batch_id: int) -> dict[str, Any]:
        batch = self._batch(batch_id)
        return {
            "ok": True,
            "batch_id": batch_id,
            "任务编号": batch["任务编号"],
            "清单编号": batch["任务编号"],
            "已建立边": batch["build_edges"],
            "build_edges": batch["build_edges"],
            "派发记录": batch["dispatches"],
            "dispatches": batch["dispatches"],
        }

    def claimable(self, batch_id: int) -> dict[str, Any]:
        batch = self._batch(batch_id)
        snapshot = self._snapshot(batch)
        groups = []
        for group_id in sorted(batch["groups"]):
            records = [
                r for r in snapshot["records"] if r["group_id"] == group_id
            ]
            groups.append(
                {
                    "group_id": group_id,
                    "name": batch["groups"][group_id]["name"],
                    "能继续领的行": [
                        r for r in records if r["buildable"] and not r["built"]
                    ],
                    "仍待定的行": [r for r in records if not r["buildable"]],
                }
            )
        return {
            "ok": True,
            "batch_id": batch_id,
            "任务编号": batch["任务编号"],
            "能继续领的行": snapshot["claimable"],
            "claimable_rows": snapshot["claimable"],
            "仍待定的行": snapshot["pending"],
            "pending_rows": snapshot["pending"],
            "groups": groups,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["cross_module_consumers"] = ["graph_build"]
        base["merge_batches"] = len(self._batches)
        return base
