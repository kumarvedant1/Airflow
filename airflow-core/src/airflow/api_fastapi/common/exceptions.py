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

import logging
import traceback
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Generic, TypeVar

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError

from airflow.configuration import conf
from airflow.exceptions import DeserializationError
from airflow.utils.strings import get_random_string

T = TypeVar("T", bound=Exception)

log = logging.getLogger(__name__)


class BaseErrorHandler(Generic[T], ABC):
    """Base class for error handlers."""

    def __init__(self, exception_cls: T) -> None:
        self.exception_cls = exception_cls

    @abstractmethod
    def exception_handler(self, request: Request, exc: T):
        """exception_handler method."""
        raise NotImplementedError


class _DatabaseDialect(Enum):
    SQLITE = "sqlite"
    MYSQL = "mysql"
    POSTGRES = "postgres"


class _UniqueConstraintErrorHandler(BaseErrorHandler[IntegrityError]):
    """Exception raised when trying to insert a duplicate value in a unique column."""

    unique_constraint_error_prefix_dict: dict[_DatabaseDialect, str] = {
        _DatabaseDialect.SQLITE: "UNIQUE constraint failed",
        _DatabaseDialect.MYSQL: "Duplicate entry",
        _DatabaseDialect.POSTGRES: "violates unique constraint",
    }

    def __init__(self):
        super().__init__(IntegrityError)
        self.dialect: _DatabaseDialect | None = None

    def exception_handler(self, request: Request, exc: IntegrityError):
        """Handle IntegrityError exception."""
        if self._is_dialect_matched(exc):
            exception_id = get_random_string()
            stacktrace = ""
            for tb in traceback.format_tb(exc.__traceback__):
                stacktrace += tb

            log_message = f"Error with id {exception_id}, statement: {exc.statement}\n{stacktrace}"
            log.error(log_message)
            if conf.get("api", "expose_stacktrace") == "True":
                message = log_message
                statement = str(exc.statement)
                orig_error = str(exc.orig)
            else:
                message = (
                    "Serious error when handling your request. Check logs for more details - "
                    f"you will find it in api server when you look for ID {exception_id}"
                )
                statement = "hidden"
                orig_error = "hidden"

            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "reason": "Unique constraint violation",
                    "statement": statement,
                    "orig_error": orig_error,
                    "message": message,
                },
            )

    def _is_dialect_matched(self, exc: IntegrityError) -> bool:
        """Check if the exception matches the unique constraint error message for any dialect."""
        exc_orig_str = str(exc.orig)
        for dialect, error_msg in self.unique_constraint_error_prefix_dict.items():
            if error_msg in exc_orig_str:
                self.dialect = dialect
                return True
        return False


class DagErrorHandler(BaseErrorHandler[DeserializationError]):
    """Handler for Dag related errors."""

    def __init__(self):
        super().__init__(DeserializationError)

    def exception_handler(self, request: Request, exc: DeserializationError):
        """Handle Dag deserialization exceptions."""
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while trying to deserialize Dag: {exc}",
        )


class ExecutionHTTPException(HTTPException):
    """
    HTTPException subclass used by Execution API.

    Enforces consistent error response format containing `reason` and `message` keys.
    """

    def __init__(
        self,
        status_code: int,
        *,
        reason: str,
        message: str,
        extra: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        """
        Initialize with explicit reason/message and optional extra fields.

        detail will be constructed as a dict: {"reason": reason, "message": message, **extra}
        """
        detail: dict[str, Any] = {"reason": reason, "message": message}
        if extra:
            # Do not allow overriding reason/message through extra accidentally.
            for k, v in extra.items():
                if k not in ("reason", "message"):
                    detail[k] = v
        super().__init__(status_code=status_code, detail=detail, headers=headers)


ERROR_HANDLERS: list[BaseErrorHandler] = [_UniqueConstraintErrorHandler(), DagErrorHandler()]


# --------------------
# Global exception normalization utilities and registration
# --------------------


def _ensure_detail_dict(detail: dict[str, Any] | str | None) -> dict[str, Any]:
    """Normalize detail into a dict with at least reason/message keys."""
    if isinstance(detail, str) or detail is None:
        return {"reason": "error", "message": detail or "An error occurred"}
    if isinstance(detail, dict):
        # Flatten legacy/nested payloads: {"detail": "..."} -> {"reason": "error", "message": "..."}
        if set(detail.keys()) == {"detail"}:
            nested_detail = detail["detail"]
            if isinstance(nested_detail, dict):
                normalized = dict(nested_detail)
                normalized.setdefault("reason", "error")
                normalized.setdefault("message", "An error occurred")
                return normalized
            return {"reason": "error", "message": str(nested_detail)}

        # Idempotent path: keep already structured details intact.
        normalized = dict(detail)
        normalized.setdefault("reason", "error")
        normalized.setdefault("message", "An error occurred")
        return normalized
    return {"reason": "error", "message": str(detail)}


def register_exception_handlers(app: FastAPI) -> None:
    """
    Register global and specific exception handlers on the FastAPI app.

    Guarantees JSON error responses carry a `detail` object with `reason` and `message`.
    """
    # Specific handlers remain in place (e.g., DB unique constraint, Dag errors)
    for handler in ERROR_HANDLERS:
        app.add_exception_handler(handler.exception_cls, handler.exception_handler)  # type: ignore[arg-type]

    # Normalize any HTTPException to have dict detail with reason/message
    @app.exception_handler(HTTPException)
    async def _http_exception_handler(request: Request, exc: HTTPException):
        detail = _ensure_detail_dict(getattr(exc, "detail", None))
        headers = getattr(exc, "headers", None)
        return JSONResponse(status_code=exc.status_code, content={"detail": detail}, headers=headers)

    # Catch-all for unhandled Exceptions
    @app.exception_handler(Exception)
    async def _unhandled_exception_handler(request: Request, exc: Exception):
        exception_id = get_random_string()
        log.exception("Unhandled error id %s while handling request %s", exception_id, request.url.path)

        if conf.get("api", "expose_stacktrace") == "True":
            message = f"Unhandled error id {exception_id}: {exc}"
        else:
            message = (
                "Serious error when handling your request. Check logs for more details - "
                f"you will find it in api server when you look for ID {exception_id}"
            )

        detail = {
            "reason": "unhandled_error",
            "message": message,
            "error_id": exception_id,
        }
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"detail": detail})
