#
# Copyright (2024) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Structured CloudWatch logging utilities for the Glue/Delta ETL pipeline.

AWS Glue forwards each job's process ``stdout`` to Amazon CloudWatch Logs
automatically. This module therefore emits structured metrics as single,
self-contained JSON lines written to ``stdout`` so that CloudWatch Logs
Insights can parse and aggregate them without any additional agent.

The centerpiece is the **mandated six-field job-completion event**. Every
``jobs/stage_*.py`` entrypoint emits exactly one completion event at the end
of its run, containing precisely the following six fields:

1. ``job_name``         -- the Glue job name.
2. ``glue_run_id``      -- the Glue job-run identifier.
3. ``input_rows``       -- number of rows read.
4. ``output_rows``      -- number of rows written.
5. ``bad_record_count`` -- number of quarantined / malformed records.
6. ``elapsed_seconds``  -- wall-clock duration of the stage, in seconds.

The event is tagged with an ``"event": "job_completion"`` discriminator so it
is queryable and distinguishable from other log lines. Optional ``**extra``
fields (for example ``stage_id`` or ``output_table``) may be appended, but they
must never replace, rename, or remove any of the six mandated fields.

This module is intentionally dependency-free: it imports only the Python
standard library so that it is safe to import from any Glue job, the Airflow
DAGs, or the validation harness without pulling in PySpark, Delta, or
``awsglue`` runtime dependencies.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any, Optional

# Discriminator value stamped on every completion event so CloudWatch Logs
# Insights queries can filter with ``filter event = "job_completion"``.
COMPLETION_EVENT_TYPE: str = "job_completion"

# Default logger name used when a caller does not supply its own logger.
DEFAULT_LOGGER_NAME: str = "pipeline"

# Simple, human-readable log line format. The structured payload itself is the
# JSON document in the ``%(message)s`` field, so the surrounding format only
# needs the timestamp, level, and logger name for operator readability when
# scanning raw CloudWatch log streams.
_LOG_FORMAT: str = "%(asctime)s %(levelname)s %(name)s %(message)s"

# The reserved keys that ``**extra`` is never permitted to overwrite: the
# discriminator plus the six mandated completion-event fields.
_RESERVED_EVENT_KEYS = frozenset(
    {
        "event",
        "job_name",
        "glue_run_id",
        "input_rows",
        "output_rows",
        "bad_record_count",
        "elapsed_seconds",
    }
)


def get_logger(name: str) -> logging.Logger:
    """Return an idempotently configured ``stdout`` logger.

    The logger writes to ``sys.stdout`` (which Glue captures to CloudWatch), is
    set to the ``INFO`` level, and does not propagate to the root logger so that
    each record is emitted exactly once.

    The function is idempotent: if the named logger already has handlers
    attached (for example because the module was re-imported on a Spark
    executor, or the same job called :func:`get_logger` more than once), the
    existing logger is returned unchanged. This avoids attaching duplicate
    handlers and emitting duplicate lines, which would corrupt downstream
    metric aggregation.

    Args:
        name: The logger name, typically the Glue job name or ``"pipeline"``.

    Returns:
        A configured :class:`logging.Logger` that writes single lines to stdout.
    """
    logger = logging.getLogger(name)
    # Idempotency guard: never attach a second handler to an already-configured
    # logger, otherwise every emitted line would be duplicated once per import.
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    # Do not bubble records up to the root logger; this logger owns the single
    # stdout handler and is solely responsible for emission.
    logger.propagate = False
    return logger


def emit_completion_event(
    job_name: str,
    glue_run_id: str,
    input_rows: int,
    output_rows: int,
    bad_record_count: int,
    elapsed_seconds: float,
    logger: Optional[logging.Logger] = None,
    **extra: Any,
) -> dict[str, Any]:
    """Emit the mandated six-field job-completion event as one JSON line.

    The event always contains the ``"event": "job_completion"`` discriminator
    followed by exactly the six mandated fields. Numeric fields are coerced to
    real JSON numbers (never strings) so CloudWatch Logs Insights can aggregate
    them: the four count fields via :func:`int`, and ``elapsed_seconds`` via
    ``round(float(...), 3)``.

    Optional ``**extra`` keyword arguments are appended to the payload, but any
    key that would collide with the discriminator or one of the six mandated
    fields is dropped, so the mandated contract can never be violated.

    Args:
        job_name: The Glue job name.
        glue_run_id: The Glue job-run identifier.
        input_rows: Number of rows read by the stage.
        output_rows: Number of rows written by the stage.
        bad_record_count: Number of quarantined / malformed records.
        elapsed_seconds: Wall-clock duration of the stage, in seconds.
        logger: Optional logger to use; defaults to ``get_logger("pipeline")``.
        **extra: Optional additional fields (for example ``stage_id`` or
            ``output_table``); these never override the discriminator or any of
            the six mandated fields.

    Returns:
        The emitted event as a ``dict`` (returned for testability so callers
        and tests can assert on the exact payload that was logged).
    """
    event: dict[str, Any] = {
        "event": COMPLETION_EVENT_TYPE,
        "job_name": job_name,
        "glue_run_id": glue_run_id,
        "input_rows": int(input_rows),
        "output_rows": int(output_rows),
        "bad_record_count": int(bad_record_count),
        "elapsed_seconds": round(float(elapsed_seconds), 3),
    }
    # Append optional extras, but never allow them to replace or rename the
    # discriminator or any of the six mandated fields (hard contract). Keys that
    # collide with a reserved key are silently ignored.
    for key, value in extra.items():
        if key not in _RESERVED_EVENT_KEYS:
            event[key] = value

    (logger or get_logger(DEFAULT_LOGGER_NAME)).info(json.dumps(event))
    return event


class StageTimer:
    """Context manager that measures wall-clock elapsed seconds for a stage.

    Uses :func:`time.monotonic` so the measurement is unaffected by system
    clock adjustments. The measured duration is exposed via the :attr:`elapsed`
    attribute once the ``with`` block exits, ready to be passed straight to
    :func:`emit_completion_event`.

    Example:
        >>> with StageTimer() as timer:
        ...     run_stage()
        >>> emit_completion_event(..., elapsed_seconds=timer.elapsed)

    :meth:`__exit__` deliberately returns ``False`` so that any exception raised
    inside the ``with`` block propagates unchanged. This preserves the
    pipeline's ACID strictness: a failed Delta / DynamoDB conditional write must
    never be silently swallowed.
    """

    def __init__(self) -> None:
        # Initialized up front so ``.elapsed`` is always safe to read, even if
        # the block raised before ``__exit__`` recomputed the final duration.
        self.elapsed: float = 0.0
        self._start: float = 0.0

    def __enter__(self) -> "StageTimer":
        self._start = time.monotonic()
        return self

    def __exit__(self, *exc: Any) -> bool:
        self.elapsed = time.monotonic() - self._start
        # Falsy return value -> exceptions are NOT suppressed (ACID strictness).
        return False
