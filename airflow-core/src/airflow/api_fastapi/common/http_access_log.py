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
"""HTTP access log middleware using structlog."""

from __future__ import annotations

import contextlib
import time
from typing import TYPE_CHECKING

import structlog
from starlette.routing import Match

from airflow._shared.observability.metrics.stats import Stats

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = structlog.get_logger(logger_name="http.access")

_HEALTH_PATHS = frozenset(["/api/v2/monitor/health"])
_API_PATH_PREFIX_TO_SURFACE = (
    ("/api/v2", "public"),
    ("/ui", "ui"),
)


def _get_api_surface(path: str) -> str | None:
    for prefix, surface in _API_PATH_PREFIX_TO_SURFACE:
        if path == prefix or path.startswith(f"{prefix}/"):
            return surface
    return None


def _get_status_family(status_code: int) -> str:
    if status_code < 100:
        return "unknown"
    return f"{status_code // 100}xx"


def _get_route_tag(scope: Scope) -> str:
    router = scope.get("router")
    routes = getattr(router, "routes", None)
    partial_route_path: str | None = None
    if routes:
        for route in routes:
            match, _ = route.matches(scope)
            route_path = getattr(route, "path", None)
            if not isinstance(route_path, str) or not route_path:
                continue
            if match == Match.FULL:
                return route_path
            if match == Match.PARTIAL and partial_route_path is None:
                partial_route_path = route_path

    if partial_route_path:
        return partial_route_path

    endpoint = scope.get("endpoint")
    endpoint_name = getattr(endpoint, "__name__", None)
    if isinstance(endpoint_name, str) and endpoint_name:
        return endpoint_name

    return "unmatched"


def _emit_api_metrics(
    *,
    scope: Scope,
    path: str,
    method: str,
    status_code: int,
    duration_us: int,
) -> None:
    api_surface = _get_api_surface(path)
    if api_surface is None:
        return

    # Keep tags bounded so API metrics remain usable across supported backends.
    tags = {
        "api_surface": api_surface,
        "method": method or "UNKNOWN",
        "route": _get_route_tag(scope),
        "status_code": str(status_code) if status_code else "unknown",
        "status_family": _get_status_family(status_code),
    }
    duration_ms = duration_us / 1000.0

    Stats.incr("api.requests", tags=tags)
    Stats.timing("api.request.duration", duration_ms, tags=tags)
    if status_code >= 400:
        Stats.incr("api.request.errors", tags=tags)


class HttpAccessLogMiddleware:
    """
    Log completed HTTP requests as structured log events.

    This middleware replaces uvicorn's built-in access logger. It measures the
    full round-trip duration, binds any ``x-request-id`` header value to the
    structlog context for the duration of the request, and emits one log event
    per completed request.

    Health-check paths are excluded to avoid log noise.
    """

    def __init__(
        self,
        app: ASGIApp,
        request_id_header: str = "x-request-id",
    ) -> None:
        self.app = app
        self.request_id_header = request_id_header.lower().encode("ascii")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start = time.monotonic_ns()
        response: Message | None = None

        async def capture_send(message: Message) -> None:
            nonlocal response
            if message["type"] == "http.response.start":
                response = message
            await send(message)

        request_id: str | None = None
        for name, value in scope["headers"]:
            if name == self.request_id_header:
                request_id = value.decode("ascii", errors="replace")
                break

        ctx = (
            structlog.contextvars.bound_contextvars(request_id=request_id)
            if request_id is not None
            else contextlib.nullcontext()
        )

        with ctx:
            try:
                await self.app(scope, receive, capture_send)
            except Exception:
                if response is None:
                    response = {"status": 500}
                raise
            finally:
                path = scope["path"]
                if path not in _HEALTH_PATHS:
                    duration_us = (time.monotonic_ns() - start) // 1000
                    status = response["status"] if response is not None else 0
                    method = scope.get("method", "")
                    query = scope["query_string"].decode("ascii", errors="replace")
                    client = scope.get("client")
                    client_addr = f"{client[0]}:{client[1]}" if client else None

                    # Observability failures must never affect serving the request.
                    with contextlib.suppress(Exception):
                        _emit_api_metrics(
                            scope=scope,
                            path=path,
                            method=method,
                            status_code=status,
                            duration_us=duration_us,
                        )

                    logger.info(
                        "request finished",
                        method=method,
                        path=path,
                        query=query,
                        status_code=status,
                        duration_us=duration_us,
                        client_addr=client_addr,
                    )
