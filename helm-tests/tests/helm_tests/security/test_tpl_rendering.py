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

import base64

import jmespath
import pytest
from chart_utils.helm_template_generator import render_chart


class TestServiceAccountAnnotationsTplRendering:
    """Tests that ServiceAccount annotations support tpl rendering for all components."""

    @pytest.mark.parametrize(
        "component, template, values_key",
        [
            ("scheduler", "templates/scheduler/scheduler-serviceaccount.yaml", "scheduler"),
            ("pgbouncer", "templates/pgbouncer/pgbouncer-serviceaccount.yaml", "pgbouncer"),
            ("webserver", "templates/webserver/webserver-serviceaccount.yaml", "webserver"),
            ("triggerer", "templates/triggerer/triggerer-serviceaccount.yaml", "triggerer"),
            ("dag-processor", "templates/dag-processor/dag-processor-serviceaccount.yaml", "dagProcessor"),
            ("api-server", "templates/api-server/api-server-serviceaccount.yaml", "apiServer"),
        ],
    )
    def test_tpl_rendered_annotation(self, component, template, values_key):
        airflow_version = "3.0.0" if component in ("dag-processor", "api-server") else "2.11.2"
        component_values = {
            "serviceAccount": {
                "annotations": {
                    "test-annotation": "{{ .Release.Name }}-value",
                },
            },
        }
        if component == "pgbouncer":
            component_values["enabled"] = True
        docs = render_chart(
            values={
                "airflowVersion": airflow_version,
                values_key: component_values,
            },
            show_only=[template],
        )
        annotations = jmespath.search("metadata.annotations", docs[0])
        assert annotations["test-annotation"] == "release-name-value"

    def test_worker_tpl_rendered_annotation(self):
        docs = render_chart(
            values={
                "workers": {
                    "serviceAccount": {
                        "annotations": {
                            "test-annotation": "{{ .Release.Name }}-worker",
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-serviceaccount.yaml"],
        )
        annotations = jmespath.search("metadata.annotations", docs[0])
        assert annotations["test-annotation"] == "release-name-worker"

    @pytest.mark.parametrize(
        "component, template, values_key",
        [
            ("pgbouncer", "templates/pgbouncer/pgbouncer-serviceaccount.yaml", "pgbouncer"),
            ("webserver", "templates/webserver/webserver-serviceaccount.yaml", "webserver"),
        ],
    )
    def test_plain_annotation_still_works(self, component, template, values_key):
        component_values = {
            "serviceAccount": {
                "annotations": {
                    "test-annotation": "plain-value",
                },
            },
        }
        if component == "pgbouncer":
            component_values["enabled"] = True
        docs = render_chart(
            values={
                "airflowVersion": "2.11.2",
                values_key: component_values,
            },
            show_only=[template],
        )
        annotations = jmespath.search("metadata.annotations", docs[0])
        assert annotations["test-annotation"] == "plain-value"


class TestMetadataConnectionTplRendering:
    """Tests that data.metadataConnection.user and .db support tpl rendering."""

    def _get_connection(self, values: dict) -> str:
        docs = render_chart(
            values=values,
            show_only=["templates/secrets/metadata-connection-secret.yaml"],
        )
        encoded = jmespath.search("data.connection", docs[0])
        return base64.b64decode(encoded).decode()

    def test_tpl_rendered_user_and_db(self):
        connection = self._get_connection(
            {
                "data": {
                    "metadataConnection": {
                        "user": "{{ .Release.Name }}-dbuser",
                        "pass": "",
                        "host": "localhost",
                        "port": 5432,
                        "db": "{{ .Release.Name }}-mydb",
                        "protocol": "postgresql",
                        "sslmode": "disable",
                    }
                }
            }
        )
        assert "release-name-dbuser" in connection
        assert "release-name-mydb" in connection

    def test_plain_user_and_db_still_works(self):
        connection = self._get_connection(
            {
                "data": {
                    "metadataConnection": {
                        "user": "plainuser",
                        "pass": "plainpass",
                        "host": "localhost",
                        "port": 5432,
                        "db": "plaindb",
                        "protocol": "postgresql",
                        "sslmode": "disable",
                    }
                }
            }
        )
        assert "plainuser" in connection
        assert "plaindb" in connection
