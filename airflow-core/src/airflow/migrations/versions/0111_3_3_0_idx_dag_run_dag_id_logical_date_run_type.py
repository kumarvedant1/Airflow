#
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

"""
Add index on dag_run for dag_id, logical_date, and run_type.

Revision ID: cecf8e022bd6
Revises: a4c2d171ae18
Create Date: 2026-04-06 16:38:57.936160

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "cecf8e022bd6"
down_revision = "a4c2d171ae18"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add index on dag_run for dag_id, logical_date, and run_type."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.create_index(
            "idx_dag_run_dag_id_logical_date_run_type",
            ["dag_id", "logical_date", "run_type"],
            unique=False,
        )


def downgrade():
    """Remove index on dag_run for dag_id, logical_date, and run_type."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_index("idx_dag_run_dag_id_logical_date_run_type")
