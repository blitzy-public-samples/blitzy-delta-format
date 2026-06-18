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
"""Pipeline manifest loader — the authoritative stored-procedure -> Glue-job map.

This module parses and validates ``config/pipeline_manifest.yaml``, the *single
source of truth* for the Glue/PySpark/Delta pipeline that replaces the legacy
Informatica-orchestrated SQL Server stored-procedure chain. The manifest is the
only place the pipeline topology lives, so the DAG (``dags/<pipeline>_dag.py``)
and the per-stage jobs (``jobs/stage_*.py``) can be reconstructed deterministically
without hardcoding any stage order, write mode, or merge condition.

The loader is intentionally **pure Python + PyYAML** — it must remain importable
at Amazon MWAA DAG-parse time and inside the lightweight ``validate/`` harness, so
it deliberately avoids any ``pyspark``/``delta``/``awsglue`` imports.

Expected YAML shape
-------------------
A ``pipeline:`` block carrying ``name`` and ``domain`` (these feed the MWAA task
naming convention ``{env}-{domain}-{name}-{step}``), followed by a ``stages:``
list. Each stage has ``id``, ``order`` (contiguous, starting at 0), ``type``
(``ingest`` | ``transform`` | ``output``), and ``job_script``. Staging stages are
expected to use ``write_mode: overwrite``; the single ``output`` stage carries an
``outputs:`` list where each table may independently be ``merge`` (with a required
``merge_condition``) or ``overwrite``.

Example::

    pipeline:
      name: sp-chain-replacement
      domain: finance

    stages:
      - id: stage-0-ingest
        order: 0
        type: ingest
        job_script: jobs/stage_0_ingest.py
        output_table: staging_raw
        write_mode: overwrite

      - id: stage-1-normalize
        order: 1
        type: transform
        job_script: jobs/stage_1_normalize.py
        source_procedure: dbo.usp_NormalizeLedger
        input_table: staging_raw
        output_table: staging_1
        write_mode: overwrite

      - id: stage-n-output
        order: 2
        type: output
        job_script: jobs/stage_n_output.py
        input_table: staging_1
        outputs:
          - table: dim_account
            write_mode: merge
            merge_condition: "t.account_id = s.account_id"
            merge_keys: [account_id]
            partition_by: [region]
          - table: fact_ledger
            write_mode: overwrite
            partition_by: [as_of_date]

Any structural problem with the manifest raises :class:`ManifestError` (a
:class:`ValueError` subclass); the loader never silently defaults invalid input.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import yaml

__all__ = [
    "ManifestError",
    "OutputTableSpec",
    "StageSpec",
    "Manifest",
    "load_manifest",
    "VALID_WRITE_MODES",
    "VALID_STAGE_TYPES",
]

# Delta write modes permitted anywhere a ``write_mode`` is declared (stage level
# or per output table). ``merge`` performs a conditional upsert and therefore
# always requires a ``merge_condition``; ``overwrite`` truncates and reloads.
VALID_WRITE_MODES = {"overwrite", "merge"}

# The three stage roles. ``ingest`` reads flat files into ``staging_raw``;
# ``transform`` replays one stored procedure 1:1; ``output`` writes the final
# Delta table(s). The pipeline must contain exactly one ``output`` stage.
VALID_STAGE_TYPES = {"ingest", "transform", "output"}


class ManifestError(ValueError):
    """Raised for any structural problem in the pipeline manifest.

    Subclasses :class:`ValueError` so callers can catch it either specifically
    (``except ManifestError``) or as part of broader value-validation handling.
    """


@dataclass(frozen=True)
class OutputTableSpec:
    """A single final-output table description from the output stage's ``outputs``.

    Attributes:
        table: Logical name of the output Delta table.
        write_mode: ``"overwrite"`` (truncate-and-reload) or ``"merge"`` (upsert).
        path_key: Optional logical key/variable naming the output S3 path.
        merge_condition: SQL merge predicate; required when ``write_mode == "merge"``.
        merge_keys: Optional list of business-key columns used by the merge.
        partition_by: Optional list of partition columns for the write.
    """

    table: str
    write_mode: str
    path_key: Optional[str] = None
    merge_condition: Optional[str] = None
    merge_keys: Optional[list] = None
    partition_by: Optional[list] = None


@dataclass(frozen=True)
class StageSpec:
    """A single pipeline stage (one Glue job) parsed from the manifest.

    Attributes:
        id: Unique stage identifier (also used to build the MWAA task id).
        order: Zero-based contiguous position in the strictly sequential chain.
        type: One of :data:`VALID_STAGE_TYPES`.
        job_script: Path to the PySpark entrypoint, e.g. ``jobs/stage_0_ingest.py``.
        source_procedure: Legacy SQL Server stored procedure replicated 1:1 (if any).
        input_table: Delta table this stage reads (the prior stage's output).
        output_table: Delta table this stage writes (for ingest/transform stages).
        write_mode: Optional stage-level write mode (validated when present).
        partition_by: Optional list of partition columns for the stage write.
        merge_condition: SQL merge predicate when ``write_mode == "merge"``.
        outputs: List of :class:`OutputTableSpec` for the output stage.
        options: Free-form per-stage options forwarded to the job (never ``None``).
    """

    id: str
    order: int
    type: str
    job_script: str
    source_procedure: Optional[str] = None
    input_table: Optional[str] = None
    output_table: Optional[str] = None
    write_mode: Optional[str] = None
    partition_by: Optional[list] = None
    merge_condition: Optional[str] = None
    outputs: Optional[list] = None
    options: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Manifest:
    """A fully validated pipeline manifest.

    Attributes:
        pipeline_name: ``pipeline.name`` from the YAML.
        pipeline_domain: ``pipeline.domain`` from the YAML.
        stages: List of :class:`StageSpec` (in declaration order).
        raw: The raw parsed mapping, retained for callers needing extra keys.
    """

    pipeline_name: str
    pipeline_domain: str
    stages: list
    raw: dict

    def ordered_stages(self) -> list:
        """Return the stages sorted by their zero-based ``order``."""
        return sorted(self.stages, key=lambda s: s.order)

    def get_stage(self, stage_id: str) -> StageSpec:
        """Return the stage whose ``id`` matches ``stage_id``.

        Raises:
            ManifestError: If no stage with that id exists.
        """
        for stage in self.stages:
            if stage.id == stage_id:
                return stage
        raise ManifestError(f"stage id not found: {stage_id!r}")

    def transform_stages(self) -> list:
        """Return the ``transform`` stages in execution order."""
        return [s for s in self.ordered_stages() if s.type == "transform"]

    @property
    def output_stage(self) -> StageSpec:
        """Return the single ``output`` stage.

        Raises:
            ManifestError: If there is not exactly one stage of type ``output``.
        """
        matches = [s for s in self.stages if s.type == "output"]
        if len(matches) != 1:
            raise ManifestError(
                f"manifest must declare exactly one 'output' stage, found {len(matches)}"
            )
        return matches[0]


def _validate_write_mode(write_mode: object, merge_condition: object, *, context: str) -> None:
    """Validate a ``write_mode`` and its companion ``merge_condition``.

    Args:
        write_mode: The declared write mode (must be in :data:`VALID_WRITE_MODES`).
        merge_condition: The merge predicate (required, non-empty, when merging).
        context: Human-readable location used in error messages.

    Raises:
        ManifestError: If the write mode is invalid, or a merge lacks a condition.
    """
    if write_mode not in VALID_WRITE_MODES:
        raise ManifestError(
            f"{context}: 'write_mode' must be one of "
            f"{sorted(VALID_WRITE_MODES)}, got {write_mode!r}"
        )
    if write_mode == "merge" and not merge_condition:
        raise ManifestError(
            f"{context}: 'write_mode' 'merge' requires a non-empty 'merge_condition'"
        )


def _optional_list(value: object, *, context: str) -> Optional[list]:
    """Return ``value`` if it is ``None`` or a list, else raise.

    Raises:
        ManifestError: If ``value`` is present but is not a list.
    """
    if value is None:
        return None
    if not isinstance(value, list):
        raise ManifestError(f"{context} must be a list when present, got {type(value).__name__}")
    return value


def _build_output(d: dict) -> OutputTableSpec:
    """Build and validate a single :class:`OutputTableSpec` from a mapping.

    Raises:
        ManifestError: On any missing/invalid field.
    """
    if not isinstance(d, dict):
        raise ManifestError(f"output table entry must be a mapping, got {type(d).__name__}")

    table = d.get("table")
    if not isinstance(table, str) or not table:
        raise ManifestError("output table entry requires a non-empty string 'table'")

    write_mode = d.get("write_mode")
    merge_condition = d.get("merge_condition")
    _validate_write_mode(write_mode, merge_condition, context=f"output table {table!r}")

    path_key = d.get("path_key")
    if path_key is not None and not isinstance(path_key, str):
        raise ManifestError(f"output table {table!r}: 'path_key' must be a string when present")

    merge_keys = _optional_list(d.get("merge_keys"), context=f"output table {table!r} 'merge_keys'")
    partition_by = _optional_list(
        d.get("partition_by"), context=f"output table {table!r} 'partition_by'"
    )

    return OutputTableSpec(
        table=table,
        write_mode=write_mode,
        path_key=path_key,
        merge_condition=merge_condition,
        merge_keys=merge_keys,
        partition_by=partition_by,
    )


def _build_stage(d: dict) -> StageSpec:
    """Build and validate a single :class:`StageSpec` from a mapping.

    Raises:
        ManifestError: On any missing/invalid field.
    """
    if not isinstance(d, dict):
        raise ManifestError(f"stage entry must be a mapping, got {type(d).__name__}")

    stage_id = d.get("id")
    if not isinstance(stage_id, str) or not stage_id:
        raise ManifestError("each stage requires a non-empty string 'id'")

    # ``bool`` is a subclass of ``int``; reject it explicitly so a YAML ``true``
    # is never silently accepted as order 1.
    order = d.get("order")
    if isinstance(order, bool) or not isinstance(order, int):
        raise ManifestError(f"stage {stage_id!r}: 'order' must be an integer, got {order!r}")

    stage_type = d.get("type")
    if stage_type not in VALID_STAGE_TYPES:
        raise ManifestError(
            f"stage {stage_id!r}: 'type' must be one of "
            f"{sorted(VALID_STAGE_TYPES)}, got {stage_type!r}"
        )

    job_script = d.get("job_script")
    if not isinstance(job_script, str) or not job_script:
        raise ManifestError(f"stage {stage_id!r}: requires a non-empty string 'job_script'")

    # Optional plain string fields.
    for key in ("source_procedure", "input_table", "output_table"):
        val = d.get(key)
        if val is not None and not isinstance(val, str):
            raise ManifestError(f"stage {stage_id!r}: {key!r} must be a string when present")

    write_mode = d.get("write_mode")
    merge_condition = d.get("merge_condition")
    if write_mode is not None:
        _validate_write_mode(write_mode, merge_condition, context=f"stage {stage_id!r}")

    raw_outputs = d.get("outputs")
    outputs: Optional[list] = None
    if raw_outputs is not None:
        if not isinstance(raw_outputs, list):
            raise ManifestError(f"stage {stage_id!r}: 'outputs' must be a list when present")
        outputs = [_build_output(o) for o in raw_outputs]

    partition_by = _optional_list(
        d.get("partition_by"), context=f"stage {stage_id!r} 'partition_by'"
    )

    options = d.get("options")
    if options is None:
        options = {}
    if not isinstance(options, dict):
        raise ManifestError(f"stage {stage_id!r}: 'options' must be a mapping when present")

    return StageSpec(
        id=stage_id,
        order=order,
        type=stage_type,
        job_script=job_script,
        source_procedure=d.get("source_procedure"),
        input_table=d.get("input_table"),
        output_table=d.get("output_table"),
        write_mode=write_mode,
        partition_by=partition_by,
        merge_condition=merge_condition,
        outputs=outputs,
        options=options,
    )


def load_manifest(path: str) -> Manifest:
    """Load, parse, and fully validate the pipeline manifest at ``path``.

    Validation steps:
        1. Parse the YAML and require the document root to be a mapping.
        2. Require a ``pipeline`` block with non-empty ``name`` and ``domain``.
        3. Require a non-empty ``stages`` list.
        4. Validate each stage (and its nested output tables), including
           ``write_mode``/``merge_condition`` rules.
        5. Require stage ``order`` values to be contiguous from 0 (no gaps/dupes)
           and stage ``id`` values to be unique.

    Args:
        path: Filesystem path to ``pipeline_manifest.yaml``.

    Returns:
        A validated :class:`Manifest`.

    Raises:
        ManifestError: On any structural problem (including malformed YAML).
    """
    try:
        with open(path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise ManifestError(f"failed to parse YAML manifest at {path!r}: {exc}") from exc

    if not isinstance(raw, dict):
        raise ManifestError(
            f"manifest root must be a mapping, got {type(raw).__name__} from {path!r}"
        )

    pipeline = raw.get("pipeline") or {}
    if not isinstance(pipeline, dict):
        raise ManifestError(
            f"'pipeline' must be a mapping when present, got {type(pipeline).__name__}"
        )
    pipeline_name = pipeline.get("name")
    pipeline_domain = pipeline.get("domain")
    if not isinstance(pipeline_name, str) or not pipeline_name:
        raise ManifestError("manifest requires 'pipeline.name' as a non-empty string")
    if not isinstance(pipeline_domain, str) or not pipeline_domain:
        raise ManifestError("manifest requires 'pipeline.domain' as a non-empty string")

    raw_stages = raw.get("stages")
    if not isinstance(raw_stages, list) or not raw_stages:
        raise ManifestError("manifest requires a non-empty 'stages' list")

    stages = [_build_stage(s) for s in raw_stages]

    stage_ids = [s.id for s in stages]
    duplicate_ids = sorted({sid for sid in stage_ids if stage_ids.count(sid) > 1})
    if duplicate_ids:
        raise ManifestError(f"duplicate stage id(s): {duplicate_ids}")

    orders = sorted(s.order for s in stages)
    if orders != list(range(len(orders))):
        raise ManifestError(
            f"stage 'order' values must be contiguous starting at 0 "
            f"(0..{len(orders) - 1}); got {orders}"
        )

    return Manifest(
        pipeline_name=pipeline_name,
        pipeline_domain=pipeline_domain,
        stages=stages,
        raw=raw,
    )
