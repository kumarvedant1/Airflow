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

from airflow.providers.sftp.pools.sftp import SFTPClientPool


@pytest.mark.asyncio
class TestSFTPClientPool:
    @pytest.mark.asyncio
    async def test_acquire_and_release(self, sftp_hook_mocked):
        async with SFTPClientPool("test_conn", pool_size=2) as pool:
            ssh, sftp = await pool.acquire()
            assert ssh is not None
            assert sftp is not None

            await pool.release((ssh, sftp))
            ssh2, sftp2 = await pool.acquire()
            assert ssh2 is not None
            assert sftp2 is not None

    @pytest.mark.asyncio
    async def test_get_sftp_client_context_manager(self, sftp_hook_mocked):
        async with SFTPClientPool("test_conn", pool_size=2) as pool:
            assert pool is not None

    @pytest.mark.asyncio
    async def test_acquire_failure_releases_semaphore(self, sftp_hook_mocked, monkeypatch):
        from airflow.providers.sftp.hooks.sftp import SFTPHookAsync

        orig_get_conn = SFTPHookAsync._get_conn

        async def fail_get_conn(self):
            raise Exception("fail")

        monkeypatch.setattr(SFTPHookAsync, "_get_conn", fail_get_conn)

        async with SFTPClientPool("test_conn", pool_size=2) as pool:
            with pytest.raises(Exception, match="fail"):
                await pool.acquire()

            monkeypatch.setattr(SFTPHookAsync, "_get_conn", orig_get_conn)
            ssh, sftp = await pool.acquire()
            assert ssh is not None
            assert sftp is not None

    @pytest.mark.asyncio
    async def test_close(self, sftp_hook_mocked, mocker):
        pool = SFTPClientPool("test_conn", pool_size=2)
        close_spy = mocker.spy(pool, "close")

        async with pool:
            ssh, sftp = await pool.acquire()
            await pool.release((ssh, sftp))

        assert close_spy.call_count == 1

