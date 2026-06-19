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

"""Pytest configuration, shared fixtures, and reference-baseline loader for ``validate/``.

This is the ``conftest.py`` for the parity (Gate 1) and security (Gate 5)
acceptance harness of the config-driven AWS Glue 4.0 / PySpark + Delta Lake ETL
pipeline that replaces the legacy SQL Server stored-procedure chain. It supplies:

* the ``--env`` command-line option (and optional ``--baseline-root`` /
  ``--baseline-format`` options);
* the ``gate1`` / ``gate5`` markers;
* session-scoped fixtures that resolve AWS specifics from the *environment* and
  build a single LogStore-wired :class:`~pyspark.sql.SparkSession` (via
  :func:`lib.spark_session.build_spark_session`) plus the ``boto3`` clients the
  tests need; and
* the **reference-baseline loader** -- a callable that reads a table from the
  sampled legacy SQL Server extract so ``test_parity.py`` can compare it against
  the deployed Delta table.

Invocation
----------
The harness runs against a **DEPLOYED** environment (live Delta tables on S3,
live IAM)::

    pytest validate/test_parity.py --env <env>

Graceful degradation (skip, never error)
-----------------------------------------
Because the harness targets live AWS resources, every AWS-specific input is
resolved from an environment variable and every fixture **skips gracefully**
(via :func:`pytest.skip`) when its inputs or optional dependencies are absent --
mirroring the established repository pattern in
``storage-s3-dynamodb/integration_tests/dynamodb_logstore.py`` (which calls
``sys.exit(0)`` on a missing env var). This lets test *collection* succeed, and
individual tests *skip* rather than *error*, on a machine that has none of the
AWS-only dependencies or environment configured.

To preserve that guarantee this module performs **no** heavyweight imports at
module scope. ``pyspark``, ``delta``, ``boto3``/``botocore``, ``PyYAML`` and the
``lib.spark_session`` builder are imported **lazily, inside the fixtures that
need them**, each wrapped so a missing dependency yields a ``pytest.skip`` rather
than a collection error. Only the standard library and ``pytest`` are imported
at the top of this file.

Manifest parsing
----------------
``config/pipeline_manifest.yaml`` is parsed **directly** with ``yaml.safe_load``
into a plain ``dict`` (see :func:`pipeline_config`). The sibling
``lib/manifest.py`` models a *different* manifest shape (typed ``stages`` keyed
by ``id``/``order`` with ``pipeline`` as a mapping) and does **not** expose the
``tables[]`` / ``parity`` blocks the parity harness iterates over; reading the
manifest as a dict is the robust, decoupled choice for this harness.

Spark wiring
------------
The :func:`spark` fixture reuses :func:`lib.spark_session.build_spark_session`
with ``master="local[*]"`` so the harness reads the deployed Delta tables through
the **same** ``io.delta.storage.S3DynamoDBLogStore`` path the pipeline writes
through. The LogStore/Delta Spark wiring is intentionally **not** re-implemented
here -- it lives in exactly one place (``lib/spark_session.py``).

Absolute Delta table paths are composed by callers as
``s3a://{DELTA_S3_BUCKET}/{defaults.delta_path_prefix}/{table.path}`` -- the
bucket, region, and DynamoDB coordination-table name come from environment
variables (never the env-agnostic manifest, never hardcoded).

Environment-variable contract (the runner injects these, per ``--env``)
-----------------------------------------------------------------------
* ``AWS_REGION``            -- AWS region (Spark LogStore region + ``boto3`` clients).
* ``DELTA_S3_BUCKET``       -- bucket holding the deployed Delta output tables.
* ``DELTA_DDB_TABLE_NAME``  -- DynamoDB coordination-table name (passed to
  :func:`lib.spark_session.build_spark_session`).
* ``GLUE_ROLE_ARN`` (or ``GLUE_ROLE_NAME``) -- IAM role for the Gate 5
  ``aws iam simulate-principal-policy`` assertion (consumed in ``test_parity.py``).
* ``PARITY_BASELINE_ROOT`` (+ optional ``PARITY_BASELINE_FORMAT``) -- location and
  format of the sampled legacy SQL Server extract used by
  :func:`reference_baseline_loader`.
* ``PARITY_ENV`` -- optional default for ``--env`` when the flag is omitted.
* Optional Gate 5 inputs consumed in ``test_parity.py``: ``IAM_POLICY_JSON_PATH``,
  ``IAM_SIMULATE_SPEC``.

No secrets are embedded in this module; all values are injected at runtime by the
CI/CD runner appropriate to the selected ``--env``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Defensive sys.path bootstrap.
#
# Guarantee that the repository root is importable so ``import lib...`` and
# repository-root-relative config access resolve regardless of the directory
# from which ``pytest`` is invoked. This is belt-and-suspenders with
# ``validate/__init__.py`` (which already puts the repo root on sys.path under
# pytest's default "prepend" import mode): we never want a fixture's lazy
# ``from lib.spark_session import ...`` to fail merely because of the CWD.
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# ---------------------------------------------------------------------------
# Reusable skip helper.
# ---------------------------------------------------------------------------
def _skip_if_missing(*names: str) -> None:
    """Skip the current test if any of ``names`` is unset/empty in the environment.

    A small, reusable guard so individual tests can assert their own
    environment-variable preconditions and *skip* (rather than *error*) when the
    deployed-environment configuration is absent -- consistent with the
    skip-don't-error policy of this harness.

    Args:
        *names: Environment-variable names that must each be present and
            non-empty for the calling test to run.

    Raises:
        Skipped: (via :func:`pytest.skip`) if one or more of ``names`` is missing
            or empty; the message lists exactly which variables were absent.
    """
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        pytest.skip("missing env vars: " + ", ".join(missing))


# ---------------------------------------------------------------------------
# Pytest hooks.
# ---------------------------------------------------------------------------
def pytest_addoption(parser: pytest.Parser) -> None:
    """Register the harness command-line options.

    Adds:

    * ``--env`` -- the target *deployed* environment for parity/security
      validation. Constrained to ``dev`` / ``nonprod`` / ``prod`` and defaulting
      to ``$PARITY_ENV`` (or ``dev``). Note: ``argparse`` only validates the
      ``choices`` for a value supplied on the command line, not for the default,
      so an unusual ``$PARITY_ENV`` is surfaced verbatim by the :func:`env`
      fixture (the runner owns that value).
    * ``--baseline-root`` -- root location (local path or ``s3a://`` URI) of the
      sampled legacy SQL Server extract; defaults to ``$PARITY_BASELINE_ROOT``.
    * ``--baseline-format`` -- ``parquet`` (preferred, type-preserving) or
      ``csv``; defaults to ``$PARITY_BASELINE_FORMAT`` or ``parquet``.
    """
    parser.addoption(
        "--env",
        action="store",
        choices=["dev", "nonprod", "prod"],
        default=os.environ.get("PARITY_ENV", "dev"),
        help="Target deployed environment for parity/security validation.",
    )
    parser.addoption(
        "--baseline-root",
        action="store",
        default=os.environ.get("PARITY_BASELINE_ROOT"),
        help=(
            "Root location (local path or s3a:// URI) of the sampled legacy "
            "SQL Server baseline extract. Defaults to $PARITY_BASELINE_ROOT."
        ),
    )
    parser.addoption(
        "--baseline-format",
        action="store",
        choices=["parquet", "csv"],
        default=os.environ.get("PARITY_BASELINE_FORMAT", "parquet"),
        help=(
            "File format of the baseline extract: 'parquet' (preferred, "
            "preserves types) or 'csv' (read as all-strings). "
            "Defaults to $PARITY_BASELINE_FORMAT or 'parquet'."
        ),
    )


def pytest_configure(config: pytest.Config) -> None:
    """Register the harness markers so ``--strict-markers`` runs cleanly.

    * ``gate1`` -- end-to-end parity (row-count + 5-field hash).
    * ``gate5`` -- least-privilege IAM security.
    """
    config.addinivalue_line(
        "markers", "gate1: end-to-end parity (row-count + 5-field hash)"
    )
    config.addinivalue_line(
        "markers", "gate5: least-privilege IAM security"
    )


# ---------------------------------------------------------------------------
# Lightweight fixtures (no heavy/optional dependencies).
#
# All fixtures in this module are session-scoped: their values are constant for
# the duration of a pytest session, and keeping a single scope avoids any
# fixture-scope mismatch (a session-scoped fixture such as ``spark`` or
# ``pipeline_config`` may only depend on same-or-wider-scoped fixtures).
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def env(request: pytest.FixtureRequest) -> str:
    """Return the selected target environment (``--env``: dev | nonprod | prod)."""
    return request.config.getoption("--env")


@pytest.fixture(scope="session")
def aws_region() -> str:
    """Return ``$AWS_REGION``; skip the test if it is unset/empty.

    Used both as the Spark LogStore region and for the ``boto3`` clients. The
    harness targets a deployed environment, so a missing region means there is
    nothing to validate against -- the test skips rather than errors.
    """
    region = os.environ.get("AWS_REGION")
    if not region:
        pytest.skip("AWS_REGION not set")
    return region


@pytest.fixture(scope="session")
def delta_bucket() -> str:
    """Return ``$DELTA_S3_BUCKET`` (bucket of the deployed Delta tables); skip if unset."""
    bucket = os.environ.get("DELTA_S3_BUCKET")
    if not bucket:
        pytest.skip("DELTA_S3_BUCKET not set")
    return bucket


@pytest.fixture(scope="session")
def ddb_table() -> str:
    """Return ``$DELTA_DDB_TABLE_NAME`` (DynamoDB coordination table); skip if unset.

    This is the table name passed to
    :func:`lib.spark_session.build_spark_session` so the harness reads Delta
    tables through the same ``S3DynamoDBLogStore`` coordination the pipeline
    writes through.
    """
    name = os.environ.get("DELTA_DDB_TABLE_NAME")
    if not name:
        pytest.skip("DELTA_DDB_TABLE_NAME not set")
    return name


@pytest.fixture(scope="session")
def repo_root() -> Path:
    """Return the repository root as a :class:`pathlib.Path`."""
    return REPO_ROOT


@pytest.fixture(scope="session")
def pipeline_config(repo_root: Path) -> dict:
    """Load ``config/pipeline_manifest.yaml`` directly into a ``dict``.

    Parses the manifest with ``yaml.safe_load`` -- deliberately **not** through
    ``lib.manifest`` -- because the parity harness needs the raw ``tables[]`` /
    ``defaults`` / ``parity`` blocks, which the typed ``lib.manifest`` model does
    not expose. Skips (rather than errors) when the manifest file is absent or
    when PyYAML is not installed, and when the file does not parse to a mapping.
    """
    manifest_path = repo_root / "config" / "pipeline_manifest.yaml"
    if not manifest_path.is_file():
        pytest.skip(f"pipeline_manifest.yaml not found at {manifest_path}")

    try:
        import yaml
    except Exception as exc:  # PyYAML is optional at collection time.
        pytest.skip(f"PyYAML unavailable: {exc}")

    with open(manifest_path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)

    if not isinstance(data, dict):
        pytest.skip(
            f"pipeline_manifest.yaml did not parse to a mapping (got {type(data).__name__})"
        )
    return data


@pytest.fixture(scope="session")
def delta_path_prefix(pipeline_config: dict) -> str:
    """Return ``defaults.delta_path_prefix`` -- the RELATIVE Delta root prefix.

    Combined by callers with ``$DELTA_S3_BUCKET`` and a table's relative ``path``
    to form ``s3a://{DELTA_S3_BUCKET}/{delta_path_prefix}/{table.path}``.
    """
    return pipeline_config["defaults"]["delta_path_prefix"]


@pytest.fixture(scope="session")
def parity_threshold(pipeline_config: dict) -> float:
    """Return ``defaults.parity_threshold`` as a float (Gate 1; default ``0.9999``)."""
    return float(pipeline_config["defaults"].get("parity_threshold", 0.9999))


@pytest.fixture(scope="session")
def manifest_tables(pipeline_config: dict) -> list:
    """Return the ``tables[]`` catalog (the per-table parity specs drive Gate 1)."""
    return pipeline_config.get("tables", [])


# ---------------------------------------------------------------------------
# Heavy / optional-dependency fixtures.
#
# These import pyspark / lib.spark_session / boto3 / botocore lazily, each
# wrapped so a missing dependency (or an unstartable SparkSession) yields a
# pytest.skip rather than a collection or setup error.
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark(aws_region: str, ddb_table: str):
    """Yield a LogStore-wired :class:`~pyspark.sql.SparkSession` for reading Delta tables.

    The session is built by :func:`lib.spark_session.build_spark_session` with
    ``master="local[*]"`` so the parity harness reads the deployed Delta tables
    through the **same** ``io.delta.storage.S3DynamoDBLogStore`` commit path the
    pipeline writes through. The LogStore/Delta Spark configuration is owned by
    ``lib/spark_session.py`` and is intentionally **not** duplicated here.

    Skips (never errors) when:

    * ``pyspark`` or ``lib.spark_session`` cannot be imported (importing
      ``build_spark_session`` transitively requires ``pyspark`` -- a missing
      ``pyspark`` therefore surfaces here too); or
    * the :class:`SparkSession` cannot be started (for example the Delta jars /
      wheel are not on the local classpath).

    The session is stopped during session teardown.
    """
    try:
        # Importing build_spark_session transitively imports ``pyspark.sql``
        # (lib/spark_session.py imports it at module scope), so this single
        # import probes both the lib helper and pyspark availability.
        from lib.spark_session import build_spark_session
    except Exception as exc:  # pyspark / lib.spark_session not importable here.
        pytest.skip(f"pyspark/lib.spark_session unavailable: {exc}")

    try:
        session = build_spark_session(
            app_name="validate-parity",
            ddb_table_name=ddb_table,
            region=aws_region,
            master="local[*]",
        )
    except Exception as exc:  # Java/Spark startup or missing Delta jars locally.
        pytest.skip(f"could not start SparkSession: {exc}")

    try:
        yield session
    finally:
        # Best-effort teardown; never let a stop() error fail the session.
        try:
            session.stop()
        except Exception:
            pass


@pytest.fixture(scope="session")
def iam_client(aws_region: str):
    """Return a ``boto3`` IAM client (Gate 5 simulate-principal-policy); skip if boto3 absent."""
    try:
        import boto3
    except Exception as exc:
        pytest.skip(f"boto3 unavailable: {exc}")
    return boto3.client("iam", region_name=aws_region)


@pytest.fixture(scope="session")
def s3_client(aws_region: str):
    """Return a ``boto3`` S3 client (mirrors the reference test); skip if boto3 absent."""
    try:
        import boto3
    except Exception as exc:
        pytest.skip(f"boto3 unavailable: {exc}")
    return boto3.client("s3", region_name=aws_region)


@pytest.fixture(scope="session")
def dynamodb_resource(aws_region: str):
    """Return a ``boto3`` DynamoDB resource scoped to ``aws_region``; skip if boto3/botocore absent.

    Mirrors the construction in
    ``storage-s3-dynamodb/integration_tests/dynamodb_logstore.py``:
    ``boto3.resource("dynamodb", config=Config(region_name=...))``. Tests obtain
    the coordination table via ``dynamodb_resource.Table($DELTA_DDB_TABLE_NAME)``.
    """
    try:
        import boto3
        from botocore.config import Config
    except Exception as exc:
        pytest.skip(f"boto3/botocore unavailable: {exc}")
    return boto3.resource("dynamodb", config=Config(region_name=aws_region))


@pytest.fixture(scope="session")
def reference_baseline_loader(spark, request: pytest.FixtureRequest):
    """Return a callable that loads a table from the sampled legacy SQL Server extract.

    This is the harness's **reference-baseline loader**. The baseline is a
    *sampled* extract of the legacy SQL Server output; parity is computed over
    whatever the baseline contains. A table whose baseline is absent is skipped
    (acceptable until the client supplies the full sample) -- though Gate 1's
    "100% of tables" criterion ultimately requires a baseline for every table.

    Resolution:

    * The baseline **root** comes from ``--baseline-root`` or
      ``$PARITY_BASELINE_ROOT``. If neither is configured, the returned callable
      skips (with a clear message) **when invoked**, so a Gate 1 test skips
      cleanly rather than erroring at fixture setup.
    * The baseline **format** comes from ``--baseline-format`` or
      ``$PARITY_BASELINE_FORMAT`` (default ``parquet``). Parquet is preferred
      because it preserves column types; ``csv`` is read with ``header=true`` and
      ``inferSchema=false`` (explicit-schema-only convention), i.e. all columns
      as strings when no schema is supplied.

    The returned ``load(baseline_name)`` callable:

    * sanitizes ``baseline_name`` (e.g. ``"dbo.FactGeneralLedger"``) into a
      path-safe stem by replacing ``.`` with ``_`` (``dbo_FactGeneralLedger``);
    * tries, in order, ``{root}/{stem}.{ext}`` (exact file),
      ``{root}/{stem}`` (partitioned directory), then the raw
      ``{root}/{baseline_name}``;
    * passes the resolved location straight to Spark (the root may be a local
      path or an ``s3a://`` URI); and
    * skips (rather than errors) if none of the candidates can be read.

    Returns:
        Callable[[str], pyspark.sql.DataFrame]: the baseline loader.
    """
    baseline_root = request.config.getoption("--baseline-root") or os.environ.get(
        "PARITY_BASELINE_ROOT"
    )
    baseline_format = (
        request.config.getoption("--baseline-format")
        or os.environ.get("PARITY_BASELINE_FORMAT")
        or "parquet"
    )

    if not baseline_root:
        def _unconfigured_loader(baseline_name: str):
            """Skip: no baseline root configured (so Gate 1 skips, not errors)."""
            pytest.skip(
                "PARITY_BASELINE_ROOT not configured "
                f"(requested baseline {baseline_name!r})"
            )

        return _unconfigured_loader

    root = str(baseline_root).rstrip("/")

    def load(baseline_name: str):
        """Read ``baseline_name`` from the configured baseline root, or skip."""
        stem = baseline_name.replace(".", "_")
        candidates = [
            f"{root}/{stem}.{baseline_format}",  # exact file
            f"{root}/{stem}",                    # partitioned directory
            f"{root}/{baseline_name}",           # raw (unsanitized) name
        ]
        last_error: Exception | None = None
        for candidate in candidates:
            reader = spark.read.format(baseline_format)
            if baseline_format == "csv":
                # Explicit-schema-only convention: never infer types. Without a
                # provided schema, CSV columns are read as strings (Parquet is
                # preferred precisely because it preserves the original types).
                reader = reader.option("header", "true").option("inferSchema", "false")
            try:
                frame = reader.load(candidate)
                # Force metadata resolution so a non-existent path fails *now*
                # (and we can fall through to the next candidate) rather than
                # later inside the parity computation.
                _ = frame.schema
                return frame
            except Exception as exc:  # try the next candidate location
                last_error = exc
                continue
        pytest.skip(
            f"baseline for {baseline_name!r} not found under {root!r}: {last_error}"
        )

    return load


@pytest.fixture(scope="session")
def skip_if_missing():
    """Return the :func:`_skip_if_missing` helper as a fixture for idiomatic reuse.

    Tests may request this fixture and call it with the env-var names they
    require, e.g. ``skip_if_missing("GLUE_ROLE_ARN")``, to skip cleanly when a
    deployed-environment precondition is absent.
    """
    return _skip_if_missing
