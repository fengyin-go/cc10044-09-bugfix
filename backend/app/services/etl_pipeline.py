"""ETL流水线领域服务：管理数据管道的抽取-转换-加载全流程编排，包括阶段分解、水位线估算和增量抽取。

数据链路位置：数据管道层，将源端数据通过抽取、清洗转换和加载写入目标存储。"""

from __future__ import annotations

from typing import Any

from app.services import pipeline_runtime as runtime
from app.services.base import BaseModuleService
from app.store import store


class EtlPipelineService(BaseModuleService):
    MODULE = "etl_pipeline"
    ENTITY = "流水线"
    OPERATION_TERM = "抽取"
    LIST_FIELDS = ["流水线名称", "源端类型", "目标端类型", "执行周期", "最近运行状态"]
    REQUIRED_FIELDS = ["流水线名称", "源端类型", "目标端类型"]
    STATUS_ORDER = ["草稿", "已启用", "运行中", "已停用"]
    ACTION_RULES = {"启用流水线": "已启用", "停用流水线": "已停用"}
    NEGATIVE_ACTIONS = ["停用流水线"]

    SOURCE_TYPES = frozenset({"MySQL", "PostgreSQL", "Kafka", "HDFS", "S3", "Hive", "MongoDB"})
    TARGET_TYPES = frozenset({"Hive", "Iceberg", "Delta", "Hudi", "ClickHouse", "Parquet"})
    EXTRACT_MODES = frozenset({"incremental", "full", "cdc"})

    def __init__(self):
        super().__init__()
        self._watermarks: dict[str, str] = {}

    def plan_pipeline_stages(self, entry_id: int) -> dict[str, Any] | None:
        """生成流水线的阶段分解计划：源端检查→抽取→转换→加载→校验。

        返回带预估行数和依赖的阶段列表，供调度器分配资源。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        source = entry.get("源端类型", "")
        target = entry.get("目标端类型", "")
        stages = [
            {
                "seq": 1, "name": "源端连接检查", "action": "validate_connectivity",
                "source_type": source, "timeout_seconds": 30,
                "depends_on": [], "estimated_rows": 0,
            },
            {
                "seq": 2, "name": "数据抽取", "action": "extract",
                "extract_mode": self._infer_extract_mode(entry),
                "watermark_col": "updated_at" if source in ("MySQL", "PostgreSQL") else "event_time",
                "timeout_seconds": 600, "depends_on": [1],
                "estimated_rows": entry.get("estimated_row_count", 0),
            },
            {
                "seq": 3, "name": "类型转换与清洗", "action": "transform",
                "transform_rules": self._build_transform_rules(source, target),
                "timeout_seconds": 300, "depends_on": [2],
                "estimated_rows": entry.get("estimated_row_count", 0),
            },
            {
                "seq": 4, "name": "目标端写入", "action": "load",
                "write_mode": "append" if target in ("Iceberg", "Delta", "Hudi") else "overwrite",
                "partition_by": entry.get("partition_column", ""),
                "timeout_seconds": 1200, "depends_on": [3],
                "estimated_rows": entry.get("estimated_row_count", 0),
            },
            {
                "seq": 5, "name": "行数校验与快照记录", "action": "validate_and_snapshot",
                "row_checksum": True, "timeout_seconds": 60, "depends_on": [4],
                "estimated_rows": 0,
            },
        ]
        return {
            "pipeline_id": entry_id,
            "pipeline_name": entry.get("流水线名称"),
            "stages": stages,
            "total_stages": len(stages),
            "critical_path": [1, 2, 4, 5],
        }

    def estimate_extract_watermark(self, entry_id: int) -> dict[str, Any]:
        """估算增量抽取的水位线位置和下一次抽取范围。

        起点只能来自目标表写入凭据里的采集截止点：失败摘要里的
        max_watermark 只是诊断信息，未落表的执行也不能改变起点，
        否则下一次抽取会跳过尚未写入的范围。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"error": "流水线不存在"}
        return runtime.estimate_watermark(entry_id, entry.get("流水线名称"))

    def accept_rerun_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        """ETL 侧受理重跑单，并按原失败号幂等。

        ETL 收到重跑单后不再另起一条运行记录：监控的重跑运行记录和执行单
        一一对应，ETL 只在这里补写执行结果与目标表写入凭据。
        """
        failure_id = payload.get("failure_id", payload.get("run_id"))
        if failure_id is None:
            return {"ok": False, "message": "重跑单缺少原失败号 failure_id"}
        return runtime.create_rerun(
            int(failure_id),
            pipeline_id=payload.get("pipeline_id"),
            pipeline_name=payload.get("pipeline_name"),
            row_count=int(payload.get("row_count", 10) or 10),
            collect_cutoff=payload.get("collect_cutoff"),
            request_id=payload.get("request_id"),
            simulate_failure=bool(payload.get("simulate_failure")),
        )

    def get_rerun_by_failure(self, failure_id: int) -> dict[str, Any]:
        """按原失败号找回该轮重跑的执行与目标表写入凭据。"""
        execution = runtime.find_execution_by_failure(failure_id)
        if execution is None:
            return {"ok": False, "message": "该失败号还没有对应的重跑执行"}
        result = runtime.execution_result(int(execution["id"])) or {}
        result["ok"] = True
        return result

    def get_execution_result(
        self,
        execution_id: int | None = None,
        *,
        failure_id: int | None = None,
        run_id: int | None = None,
    ) -> dict[str, Any]:
        """按重跑单查询本轮结果数据写入及确认的数据范围。"""
        execution = runtime.find_execution(execution_id, failure_id=failure_id, run_id=run_id)
        if execution is None:
            return {"ok": False, "message": "未找到对应的重跑执行"}
        result = runtime.execution_result(int(execution["id"])) or {}
        result["ok"] = True
        return result

    def confirm_write_result(self, payload: dict[str, Any]) -> dict[str, Any]:
        """确认本轮写入结果：完成消息只记一次行数，重复投递幂等。"""
        failure_id = payload.get("failure_id")
        run_id = payload.get("run_id")
        execution_id = payload.get("execution_id")
        if failure_id is not None:
            execution = runtime.find_execution_by_failure(int(failure_id))
            execution_id = execution.get("id") if execution else None
        return runtime.apply_completion(
            int(execution_id) if execution_id is not None else None,
            failure_id=int(failure_id) if failure_id is not None else None,
            run_id=int(run_id) if run_id is not None else None,
            row_count=int(payload.get("row_count", 10) or 10),
            message_id=payload.get("message_id"),
            confirmed_cutoff=payload.get("collect_cutoff"),
        )

    def mark_execution_failed(self, payload: dict[str, Any]) -> dict[str, Any]:
        """ETL 把本轮执行标记为失败，后续只能用新失败号再次发起重跑。"""
        failure_id = payload.get("failure_id")
        execution = runtime.find_execution(
            payload.get("execution_id"),
            failure_id=int(failure_id) if failure_id is not None else None,
            run_id=payload.get("run_id"),
        )
        return runtime.fail_execution(
            int(execution["id"]) if execution else None,
            reason=payload.get("reason", "重跑执行失败"),
            summary_cutoff=payload.get("collect_cutoff"),
        )

    def execute_pipeline(self, entry_id: int, force_full: bool = False) -> dict[str, Any]:
        """触发流水线执行，生成运行记录并推进水位线。

        支持强制全量模式和常规增量模式：
        - force_full=True 时忽略水位线，从源端重新全量抽取
        - force_full=False 时根据已记录的水位线做增量抽取
        执行结果写入 pipeline_monitor 模块供监控消费，同时在内部执行账本
        登记唯一一条执行单（monitor 运行记录与执行单一一对应，不再各记一条）。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "流水线不存在"}
        if entry.get("status") not in ("已启用", "运行中"):
            return {"ok": False, "message": "流水线未启用，无法执行"}

        plan = self.plan_pipeline_stages(entry_id)
        if plan is None:
            return {"ok": False, "message": "无法生成执行计划"}

        name = entry.get("流水线名称", "")
        collect_start = runtime.latest_landed_cutoff(
            pipeline_id=entry_id, pipeline_name=name) or runtime.DEFAULT_COLLECT_START
        collect_cutoff = runtime.DEFAULT_LANDED_CUTOFF
        run_time = runtime.next_run_time()
        run_id = self._next_run_id()

        execution = {
            "id": runtime.next_id(store.aux_rows(runtime.EXECUTIONS)),
            "pipeline_id": entry_id,
            "pipeline_name": name,
            "target_table": runtime.target_table_name(entry, name),
            "mode": "full" if force_full else "incremental",
            "source_failure_id": None,
            "status": runtime.EXEC_RUNNING,
            "state": runtime.EXEC_RUNNING,
            "collect_start": runtime.DEFAULT_COLLECT_START if force_full else collect_start,
            "collect_cutoff": collect_cutoff,
            "attempted_cutoff": collect_cutoff,
            "row_count": 0,
            "written_rows": 0,
            "confirmed_rows": 0,
            "written_range": "",
            "confirmed_range": "",
            "error": "",
            "monitor_run_id": run_id,
            "created_at": run_time,
            "updated_at": run_time,
        }
        store.aux_rows(runtime.EXECUTIONS).append(execution)

        run_record = {
            "id": run_id,
            "流水线名称": name,
            "运行时间": run_time,
            "执行状态": "运行中",
            "处理行数": 0,
            "错误信息": "",
            "status": "运行中",
            "pipeline_id": entry_id,
            "execution_id": execution["id"],
            "execution_status": runtime.EXEC_RUNNING,
            "confirm_status": "未确认",
            "collect_start": execution["collect_start"],
            "collect_cutoff": collect_cutoff,
            "extract_mode": execution["mode"],
            "stages_plan": plan,
        }
        store.rows("pipeline_monitor").append(run_record)
        return {
            "ok": True,
            "message": "流水线已提交执行",
            "run_id": run_record["id"],
            "execution_id": execution["id"],
            "stages": len(plan["stages"]),
        }

    def cross_module_snapshot(self, entry_id: int) -> dict[str, Any]:
        """跨模块消费入口：生成当前流水线的数据产物快照。

        下游模块（快照生成器、血缘图谱、质量规则）通过此方法
        获取流水线的最新产出元数据，用于构建数据流转关系。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"error": "流水线不存在"}
        last_run = self._last_pipeline_run(entry_id)
        return {
            "source_module": "etl_pipeline",
            "pipeline_id": entry_id,
            "pipeline_name": entry.get("流水线名称"),
            "target_table": entry.get("目标端类型", ""),
            "last_run_id": last_run.get("id") if last_run else None,
            "last_run_status": last_run.get("状态", "从未运行"),
            "watermark": self._watermarks.get(str(entry_id), ""),
            "produced_tables": self._resolve_target_tables(entry),
            "snapshot_time": "2026-11-21T08:00:00Z",
        }

    def reconcile_pipeline_with_partitions(self, entry_id: int) -> dict[str, Any]:
        """对账流水线产出与分区管理配置，检测不一致。

        比较流水线的目标表和分区管理的配置表，
        发现目标表缺少分区配置或分区粒度不匹配时生成告警。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "流水线不存在"}
        target_table = entry.get("目标端类型", "")
        partition_entries = store.rows("partition_manager")
        matching = [
            p for p in partition_entries
            if p.get("目标表", "") == target_table and p.get("status") == "已启用"
        ]
        issues = []
        if not matching:
            issues.append(f"目标表 {target_table} 没有已启用的分区配置，建议关联分区管理")
        else:
            for p in matching:
                if int(p.get("保留天数", "0")) < 7:
                    issues.append(f"分区 {p.get('配置名称')} 保留天数仅 {p.get('保留天数')} 天，可能不满足回溯需求")
        return {
            "pipeline_id": entry_id,
            "pipeline_name": entry.get("流水线名称"),
            "target_table": target_table,
            "matching_partitions": len(matching),
            "issues": issues,
            "ok": len(issues) == 0,
        }

    def _infer_extract_mode(self, entry: dict[str, Any]) -> str:
        source = entry.get("源端类型", "")
        if source == "Kafka":
            return "cdc"
        return "incremental"

    def _build_transform_rules(self, source: str, target: str) -> list[dict[str, str]]:
        rules = []
        if source == "MySQL":
            rules.append({"column_type": "DATETIME", "target_type": "TIMESTAMP", "action": "convert_timezone"})
        if target in ("Iceberg", "Parquet"):
            rules.append({"null_handling": "replace_empty_string", "action": "normalize_nulls"})
        return rules

    def _next_run_id(self) -> int:
        rows = store.rows("pipeline_monitor")
        return max((int(r.get("id", 0)) for r in rows), default=0) + 1

    def _last_pipeline_run(self, entry_id: int) -> dict[str, Any] | None:
        monitor_rows = store.rows("pipeline_monitor")
        candidates = [r for r in monitor_rows if r.get("pipeline_id") == entry_id]
        if not candidates:
            return None
        candidates.sort(key=lambda r: r.get("运行时间", ""), reverse=True)
        return candidates[0]

    def _resolve_target_tables(self, entry: dict[str, Any]) -> list[str]:
        target = entry.get("目标端类型", "")
        name = entry.get("流水线名称", f"pipeline_{entry.get('id', 'unknown')}")
        return [f"{target.lower()}_{name}"]