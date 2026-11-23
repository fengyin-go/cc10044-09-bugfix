"""重跑故障回归测试：同一失败号只派生一轮执行，水位线只认已落表凭据。

运行：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import unittest

from app.seed import SEED_ROWS
from app.services import pipeline_runtime as runtime
from app.services import service_for
from app.store import store

PIPELINE_ID = 1  # ods_order_inc_daily
PIPELINE_NAME = "ods_order_inc_daily"


class RerunFlowTest(unittest.TestCase):
    def setUp(self) -> None:
        store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
        store._aux_tables = {}
        self.etl = service_for("etl_pipeline")
        self.monitor = service_for("pipeline_monitor")
        self.monitor.get_entry  # 确保服务已注册

    # -- 辅助 ---------------------------------------------------------------
    def _failed_run(self) -> int:
        started = self.etl.execute_pipeline(PIPELINE_ID)
        failed = self.etl.mark_execution_failed({
            "execution_id": started["execution_id"],
            "reason": "连接池耗尽: too many connections",
        })
        self.assertTrue(failed["ok"])
        return failed["failure_id"]

    def _orders_runs(self) -> list[dict]:
        return [r for r in store.rows("pipeline_monitor")
                if r.get("流水线名称") == PIPELINE_NAME]

    def _rerun(self, failure_id: int, **options) -> dict:
        return self.monitor.rerun_pipeline(failure_id, **options)

    # -- 用例 ---------------------------------------------------------------
    def test_duplicate_request_derives_one_execution_and_one_write(self) -> None:
        failure_id = self._failed_run()
        before = len(self._orders_runs())

        first = self._rerun(failure_id, row_count=10, request_id="req-1")
        second = self._rerun(failure_id, row_count=10, request_id="req-2")

        self.assertTrue(first["ok"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["rerun_id"], second["rerun_id"])
        self.assertEqual(first["execution_id"], second["execution_id"])
        self.assertEqual(len(self._orders_runs()) - before, 1)

        result = self.etl.get_execution_result(failure_id=failure_id)
        self.assertEqual(result["execution_count_for_failure"], 1)
        self.assertEqual(result["write_count"], 1)
        self.assertEqual(result["written_range"],
                         "2026-11-21T00:00:00Z~2026-11-21T10:15:00Z")

    def test_watermark_ignores_failure_summary_and_unlanded_runs(self) -> None:
        failure_id = self._failed_run()
        watermark = self.etl.estimate_extract_watermark(PIPELINE_ID)
        self.assertEqual(watermark["watermark_source"], "none")
        self.assertNotEqual(watermark["next_extract_start"], "2026-11-21T10:30:00Z")

        rerun = self._rerun(failure_id, row_count=10)
        watermark = self.etl.estimate_extract_watermark(PIPELINE_ID)
        self.assertEqual(watermark["next_extract_start"], "2026-11-21T10:15:00Z")

        # 再失败一次：摘要声明 10:30，但起点必须停在已落表的 10:15。
        started = self.etl.execute_pipeline(PIPELINE_ID)
        failed = self.etl.mark_execution_failed({"execution_id": started["execution_id"]})
        self.assertEqual(failed["failure_summary_cutoff"], "2026-11-21T10:30:00Z")
        watermark = self.etl.estimate_extract_watermark(PIPELINE_ID)
        self.assertEqual(watermark["next_extract_start"], "2026-11-21T10:15:00Z")
        self.assertTrue(any(item["cutoff"] == "2026-11-21T10:30:00Z"
                            for item in watermark["excluded_unlanded"]))
        self.assertNotEqual(rerun["rerun_id"], failed["failure_id"])

    def test_duplicate_completion_message_counts_rows_once(self) -> None:
        failure_id = self._failed_run()
        rerun = self._rerun(failure_id, row_count=10)
        message = {
            "failure_id": failure_id,
            "row_count": 10,
            "message_id": "etl-msg-001",
            "collect_cutoff": "2026-11-21T10:15:00Z",
        }
        first = self.monitor.receive_completion(**message)
        second = self.monitor.receive_completion(**message)

        self.assertEqual(first["status"], "已完成")
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["row_count"], 10)
        self.assertEqual(second["row_count"], 10)

        detail = self.monitor.get_entry(rerun["monitor_run_id"])
        self.assertEqual(detail["status"], "已完成")
        self.assertEqual(detail["处理行数"], "10")
        self.assertEqual(detail["确认行数"], 10)
        self.assertEqual(detail["执行单号"], first["execution_id"])
        self.assertEqual(detail["确认数据范围"],
                         "2026-11-21T00:00:00Z~2026-11-21T10:15:00Z")

    def test_new_failure_id_can_start_another_round(self) -> None:
        first_failure = self._failed_run()
        self._rerun(first_failure, row_count=10)
        self.monitor.receive_completion(
            failure_id=first_failure, row_count=10, message_id="msg-1",
            collect_cutoff="2026-11-21T10:15:00Z")

        started = self.etl.execute_pipeline(PIPELINE_ID)
        failed = self.etl.mark_execution_failed({
            "execution_id": started["execution_id"],
            "reason": "目标端写入超时",
        })
        self.assertNotEqual(failed["failure_id"], first_failure)

        rerun = self._rerun(failed["failure_id"], row_count=7)
        execution = runtime.store.aux_find(runtime.EXECUTIONS, rerun["execution_id"])
        self.assertEqual(execution["source_failure_id"], failed["failure_id"])
        self.assertEqual(execution["collect_start"], "2026-11-21T10:15:00Z")

        run = store.find("pipeline_monitor", rerun["monitor_run_id"])
        self.assertEqual(run["retry_of"], failed["failure_id"])

    def test_failed_execution_cannot_be_confirmed_as_complete(self) -> None:
        started = self.etl.execute_pipeline(PIPELINE_ID)
        self.etl.mark_execution_failed({"execution_id": started["execution_id"]})
        result = self.etl.confirm_write_result({
            "execution_id": started["execution_id"], "row_count": 999,
        })
        self.assertFalse(result["ok"])
        self.assertIn("新失败号", result["message"])

    def test_successful_run_cannot_be_rerun(self) -> None:
        before = len(self._orders_runs())
        result = self._rerun(1)  # 种子记录 1 为成功
        self.assertFalse(result["ok"])
        self.assertEqual(len(self._orders_runs()), before)


if __name__ == "__main__":
    unittest.main()
