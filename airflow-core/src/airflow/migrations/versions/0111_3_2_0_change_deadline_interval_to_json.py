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
Change type of interval in Deadline Alerts table to JSON.

Revision ID: 82f208dbbad5
Revises: a4c2d171ae18
Create Date: 2026-04-06 16:55:46.517409

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "82f208dbbad5"
down_revision = "a4c2d171ae18"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Apply change deadline interval to text."""
    with op.batch_alter_table("deadline_alert", schema=None) as batch_op:
        batch_op.alter_column(
            "interval",
            existing_type=sa.FLOAT(),
            type_=sa.JSON(),
            existing_nullable=False,
        )

    conn = op.get_bind()

    rows = conn.execute(sa.text("SELECT id, interval FROM deadline_alert")).fetchall()

    for row in rows:
        seconds = float(row.interval)

        serialized = {
            "__type": "timedelta",
            "__var": seconds,
        }

        conn.execute(
            sa.text("UPDATE deadline_alert SET interval = :val WHERE id = :id"),
            {"val": serialized, "id": row.id},
        )


def downgrade():
    """Revert deadline interval back to float."""
    conn = op.get_bind()

    rows = conn.execute(sa.text("SELECT id, interval FROM deadline_alert")).fetchall()

    for row in rows:
        raw = row.interval

        if isinstance(raw, dict) and raw.get("__type") == "timedelta":
            seconds = float(raw.get("__var", 0))
        else:
            seconds = float(raw)

        conn.execute(
            sa.text("UPDATE deadline_alert SET interval = :val WHERE id = :id"),
            {"val": seconds, "id": row.id},
        )

    with op.batch_alter_table("deadline_alert", schema=None) as batch_op:
        batch_op.alter_column(
            "interval",
            existing_type=sa.JSON(),
            type_=sa.FLOAT(),
            existing_nullable=False,
        )
