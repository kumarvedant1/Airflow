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

import asyncio
from contextlib import asynccontextmanager, suppress
from threading import Lock
from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.providers.sftp.hooks.sftp import SFTPHookAsync
from airflow.sdk.definitions._internal.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    import asyncssh


class SFTPClientPool(LoggingMixin):
    """Lazy Thread-safe and Async-safe Singleton SFTP pool that keeps SSH and SFTP clients alive until exit, and limits concurrent usage to pool_size."""

    _instances: dict[str, SFTPClientPool] = {}
    _lock = Lock()

    def __new__(cls, sftp_conn_id: str, pool_size: int = None):
        with cls._lock:
            if sftp_conn_id not in cls._instances:
                instance = super().__new__(cls)
                instance._pre_init(sftp_conn_id, pool_size)
                cls._instances[sftp_conn_id] = instance
            return cls._instances[sftp_conn_id]

    def __init__(self, sftp_conn_id: str, pool_size: int = None):
        # Prevent parent __init__ argument errors
        pass

    def _pre_init(self, sftp_conn_id: str, pool_size: int):
        """Initialize the Singleton structure synchronously."""
        LoggingMixin.__init__(self)
        self.sftp_conn_id = sftp_conn_id
        self.pool_size = pool_size or conf.getint("core", "parallelism")
        self._idle: asyncio.LifoQueue[tuple[asyncssh.SSHClientConnection, asyncssh.SFTPClient]] = (
            asyncio.LifoQueue()
        )
        self._in_use: set[tuple[asyncssh.SSHClientConnection, asyncssh.SFTPClient]] = set()
        self._semaphore = asyncio.Semaphore(self.pool_size)
        self._init_lock = asyncio.Lock()
        self._initialized = False
        self._closed = False
        self.log.info("SFTPClientPool initialised...")

    async def _ensure_initialized(self):
        """Ensure pool is usable (also handles re-opening after close)."""
        if self._initialized and not self._closed:
            return

        async with self._init_lock:
            if not self._initialized or self._closed:
                self.log.info("Initializing / resetting SFTPClientPool for '%s'", self.sftp_conn_id)
                self._idle = asyncio.LifoQueue()
                self._in_use.clear()
                self._semaphore = asyncio.Semaphore(self.pool_size)
                self._closed = False
                self._initialized = True

    async def _create_connection(
        self,
    ) -> tuple[asyncssh.SSHClientConnection, asyncssh.SFTPClient]:
        ssh_conn = await SFTPHookAsync(sftp_conn_id=self.sftp_conn_id)._get_conn()
        sftp = await ssh_conn.start_sftp_client()
        self.log.info("Created new SFTP connection for sftp_conn_id '%s'", self.sftp_conn_id)
        return ssh_conn, sftp

    async def acquire(self):
        await self._ensure_initialized()

        if self._closed:
            raise RuntimeError("Cannot acquire from a closed SFTPClientPool")

        self.log.debug("Acquiring SFTP connection for '%s'", self.sftp_conn_id)

        await self._semaphore.acquire()

        try:
            try:
                pair = self._idle.get_nowait()
            except asyncio.QueueEmpty:
                pair = await self._create_connection()

            self._in_use.add(pair)
            return pair
        except Exception:
            self._semaphore.release()
            raise

    async def release(self, pair):
        if pair not in self._in_use:
            self.log.warning("Attempted to release unknown or already released connection")
            return

        self._in_use.discard(pair)

        if self._closed:
            ssh, sftp = pair
            with suppress(Exception):
                sftp.exit()
            with suppress(Exception):
                ssh.close()
        else:
            await self._idle.put(pair)

        self.log.debug("Releasing SFTP connection for '%s'", self.sftp_conn_id)
        self._semaphore.release()

    @asynccontextmanager
    async def get_sftp_client(self):
        await self._ensure_initialized()
        pair = None
        try:
            pair = await self.acquire()
            ssh, sftp = pair
            yield sftp
        except BaseException as e:
            self.log.warning("Dropping faulty connection for '%s': %s", self.sftp_conn_id, e)
            if pair:
                ssh, sftp = pair
                self._in_use.discard(pair)
                with suppress(Exception):
                    sftp.exit()
                with suppress(Exception):
                    ssh.close()
                self._semaphore.release()
            raise
        else:
            await self.release(pair)

    async def close(self):
        """Gracefully shutdown all connections in the pool."""
        async with self._init_lock:
            if self._closed:
                return

            self._closed = True

            self.log.info("Closing all SFTP connections for '%s'", self.sftp_conn_id)

            while not self._idle.empty():
                ssh, sftp = await self._idle.get()
                with suppress(Exception):
                    sftp.exit()
                with suppress(Exception):
                    ssh.close()

            for pair in list(self._in_use):
                ssh, sftp = pair
                with suppress(Exception):
                    sftp.exit()
                with suppress(Exception):
                    ssh.close()
                self._in_use.discard(pair)

            if self._in_use:
                self.log.warning("Pool closed with %d active connections", len(self._in_use))

            self._semaphore = asyncio.Semaphore(self.pool_size)

    async def __aenter__(self):
        await self._ensure_initialized()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
