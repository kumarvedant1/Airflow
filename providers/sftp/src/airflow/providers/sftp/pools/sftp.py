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

import asyncssh

from airflow.configuration import conf
from airflow.providers.sftp.hooks.sftp import SFTPHookAsync
from airflow.sdk.definitions._internal.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from paramiko.sftp_client import SFTPClient


class SFTPClientPool(LoggingMixin):
    """Lazy Thread-safe and Async-safe Singleton SFTP pool that keeps SSH and SFTP clients alive until exit, and limits concurrent usage to pool_size."""

    _instances: dict[str, SFTPClientPool] = {}
    _lock = Lock()  # Protects the _instances dict across threads

    def __new__(cls, sftp_conn_id: str, pool_size: int = None):
        # Thread-safe check for existing instance
        with cls._lock:
            if sftp_conn_id not in cls._instances:
                instance = super().__new__(cls)
                # Initialize basic attributes immediately
                instance._pre_init(sftp_conn_id, pool_size)
                cls._instances[sftp_conn_id] = instance
            return cls._instances[sftp_conn_id]

    def __init__(self, sftp_conn_id: str, pool_size: int = None):
        # Prevent parent __init__ argument errors
        pass

    def _pre_init(self, sftp_conn_id: str, pool_size: int):
        """Synchronous initialization for the Singleton structure."""
        LoggingMixin.__init__(self)
        self.sftp_conn_id = sftp_conn_id
        self.pool_size = pool_size or conf.getint("core", "parallelism")
        self._idle: asyncio.LifoQueue[
            tuple[asyncssh.SSHClientConnection, asyncssh.SFTPClient]
        ] = asyncio.LifoQueue()
        self._semaphore = asyncio.Semaphore(self.pool_size)
        self._init_lock = asyncio.Lock()
        self._initialized = False
        self.log.info("SFTPClientPool initialised...")

    async def _ensure_initialized(self):
        """Ensures async-only resources are set up exactly once."""
        if self._initialized:
            return
        async with self._init_lock:
            if not self._initialized:
                # Place any specific async setup here if needed
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
        self.log.debug("Acquiring SFTP connection for '%s'", self.sftp_conn_id)

        # This blocks until a slot in the pool is available
        await self._semaphore.acquire()

        try:
            # Try to get an existing connection
            return self._idle.get_nowait()
        except asyncio.QueueEmpty:
            try:
                # If queue is empty but semaphore allowed us in, create new
                return await self._create_connection()
            except Exception:
                # If creation fails, release semaphore so others can try
                self._semaphore.release()
                raise

    async def release(self, pair):
        """Returns a connection to the pool."""
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
            # Successful use, return to pool
            await self.release(pair)
        except Exception as e:
            self.log.warning("Dropping faulty connection for '%s': %s", self.sftp_conn_id, e)
            if pair:
                ssh, sftp = pair
                with suppress(Exception):
                    sftp.exit()
                with suppress(Exception):
                    ssh.close()
            # We DON'T release the pair back to _idle,
            # but we DO release the semaphore to allow a new connection.
            self._semaphore.release()
            raise

    async def close(self):
        """Gracefully shutdown all connections in the pool."""
        async with self._init_lock:
            self.log.info("Closing all SFTP connections for '%s'", self.sftp_conn_id)
            while not self._idle.empty():
                ssh, sftp = await self._idle.get()
                with suppress(Exception):
                    sftp.exit()
                with suppress(Exception):
                    ssh.close()
            self._initialized = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # Note: In many singleton use-cases, you might NOT want to close
        # the pool on __aexit__ if other tasks are still using it.
        pass
