# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import pytest

from airflow.api_fastapi.core_api.datamodels.dags import DAGResponse


def _make_dag_response(**overrides) -> DAGResponse:
    """Create a minimal DAGResponse with sensible defaults."""
    defaults = {
        "dag_id": "test_dag",
        "dag_display_name": "Test DAG",
        "is_paused": False,
        "is_stale": False,
        "last_parsed_time": None,
        "last_parse_duration": None,
        "last_expired": None,
        "bundle_name": "dags-folder",
        "bundle_version": None,
        "relative_fileloc": "test.py",
        "fileloc": "/opt/airflow/dags/test.py",
        "description": None,
        "timetable_summary": "0 * * * *",
        "timetable_description": "At the start of every hour",
        "timetable_partitioned": False,
        "tags": [],
        "max_active_tasks": 16,
        "max_active_runs": 25,
        "max_consecutive_failed_dag_runs": 0,
        "has_task_concurrency_limits": False,
        "has_import_errors": False,
        "next_dagrun_logical_date": None,
        "next_dagrun_data_interval_start": None,
        "next_dagrun_data_interval_end": None,
        "next_dagrun_run_after": None,
        "allowed_run_types": None,
        "owners": "airflow",
    }
    defaults.update(overrides)
    return DAGResponse.model_validate(defaults)


class TestIsBackfillable:
    @pytest.mark.parametrize(
        "timetable_summary",
        [
            pytest.param(None, id="no-schedule"),
            pytest.param("@once", id="once"),
            pytest.param("@continuous", id="continuous"),
            pytest.param("Asset", id="asset-triggered"),
            pytest.param("Partitioned Asset", id="partitioned-asset"),
        ],
    )
    def test_non_backfillable_summaries(self, timetable_summary):
        dag = _make_dag_response(timetable_summary=timetable_summary)
        assert dag.is_backfillable is False

    @pytest.mark.parametrize(
        "timetable_summary",
        [
            pytest.param("0 * * * *", id="cron"),
            pytest.param("@daily", id="daily-preset"),
            pytest.param("timedelta(hours=1)", id="timedelta"),
            pytest.param("Asset or @daily", id="hybrid-asset-or-daily"),
            pytest.param("Asset or 0 * * * *", id="hybrid-asset-or-cron"),
        ],
    )
    def test_backfillable_summaries(self, timetable_summary):
        dag = _make_dag_response(timetable_summary=timetable_summary)
        assert dag.is_backfillable is True
