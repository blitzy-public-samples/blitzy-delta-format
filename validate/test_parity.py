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

"""Gate 1 (Parity) + Gate 5 (Security) acceptance tests.

This module is the **primary acceptance harness** for the config-driven AWS
Glue 4.0 / PySpark + Delta Lake ETL pipeline that replaces the legacy finance
SQL Server stored-procedure chain. It is run against a **deployed** environment::

    pytest validate/test_parity.py --env <env>

Scope -- exactly two gates
--------------------------
This file implements **only** the following two acceptance gates; the remaining
gates live elsewhere and are deliberately *not* implemented here:

* **Gate 1 -- Parity.** For **100% of staging and final tables** declared in
  ``config/pipeline_manifest.yaml``, the deployed Delta table must match its
  sampled legacy SQL Server baseline on both a row-count signal and a
  deterministic five-field hash signal at **>= 99.99% (0.9999)**. Each manifest
  table becomes its own parametrized test, so Gate 1 passes only when *every*
  table passes -- which is precisely the "100% of tables" criterion.
* **Gate 5 -- Security.** The pipeline's IAM policy must (5a) contain **no
  wildcard ``*`` resources** and (5b) pass ``aws iam simulate-principal-policy``
  for the deployed role.

Gate 2 (ACID), Gate 3 (Idempotency), Gate 4 (Infrastructure drift) and Gate 6
(Performance) are out of scope for this file.

Heavy-import discipline (CRITICAL)
----------------------------------
Only the standard library and ``pytest`` are imported at module scope
(``json``, ``os``, ``pytest``). ``pyspark``, ``delta``, ``lib.delta_io`` and
``validate.reconciliation`` are imported **lazily, inside the Gate 1 test body**
(after the ``spark`` fixture has already gated on their availability), and
``boto3`` is reached **only** through the ``iam_client`` / ``s3_client`` fixtures.
This guarantees that:

* test *collection* succeeds everywhere -- even where ``pyspark`` is absent; and
* **Gate 5 remains runnable without ``pyspark``** (it is a pure ``boto3`` check).

Graceful-skip philosophy
------------------------
Mirroring the repository's own integration test
(``storage-s3-dynamodb/integration_tests/dynamodb_logstore.py``, which calls
``sys.exit(0)`` on a missing env var), every test **skips with a clear reason**
-- rather than *erroring* -- when a precondition is absent: no AWS region/creds,
no deployed Delta table, no baseline, no IAM policy source, no simulate spec, or
``pyspark``/Delta unavailable. The harness is therefore collectable and partially
runnable anywhere, while still enforcing the gates fully in a properly configured
``--env``.

Environment-variable contract (injected by the CI/CD runner per ``--env``)
--------------------------------------------------------------------------
* ``AWS_REGION``           -- region for the ``boto3`` clients and Spark LogStore
  (resolved by the ``aws_region`` fixture; all tests skip when it is unset).
* ``DELTA_S3_BUCKET``      -- bucket holding the deployed Delta tables (Gate 1).
* ``DELTA_DDB_TABLE_NAME`` -- DynamoDB coordination table (Gate 1 Spark session).
* ``PARITY_BASELINE_ROOT`` (+ optional ``PARITY_BASELINE_FORMAT``) -- sampled
  legacy SQL Server extract used by the ``reference_baseline_loader`` (Gate 1).
* ``IAM_POLICY_JSON_PATH`` *or* ``GLUE_ROLE_NAME`` -- IAM policy source (Gate 5a).
* ``GLUE_ROLE_ARN`` (or ``GLUE_ROLE_NAME``) + ``IAM_SIMULATE_SPEC`` -- inputs for
  the ``simulate-principal-policy`` assertion (Gate 5b).

No environment-specific values, ARNs, or secrets are embedded in this module:
every such value is resolved at runtime from the environment or a fixture.
"""

import json
import os

import pytest


# --------------------------------------------------------------------------- #
# Collection-time manifest reader (dependency-free, crash-proof).
#
# Parametrizing Gate 1 requires the table catalog *at collection time*, before
# any fixture runs. We therefore read ``config/pipeline_manifest.yaml`` directly
# here -- consistent with the conftest contract (the harness never routes the
# manifest through ``lib.manifest``) -- and never crash collection on a missing
# file, a missing PyYAML, a parse error, or an unexpected shape.
# --------------------------------------------------------------------------- #
def _manifest_tables_for_ids():
    """Return ``tables[]`` from ``config/pipeline_manifest.yaml`` for parametrization.

    The repository root is computed relative to this file
    (``<root>/validate/test_parity.py`` -> ``<root>``) so the lookup is robust to
    the working directory ``pytest`` is invoked from. ``PyYAML`` is imported
    lazily *inside* this function (never at module scope) so the heavy-import
    discipline holds and Gate 5 stays runnable without it.

    Returns:
        list[dict]: the manifest ``tables[]`` entries (each a mapping), or an
        empty list if the manifest, ``PyYAML``, or a well-formed ``tables`` list
        is unavailable. Any failure returns ``[]`` so test *collection* never
        crashes; an empty result instead surfaces as a visible skip via
        :func:`test_gate1_manifest_present`.
    """
    try:
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        manifest_path = os.path.join(repo_root, "config", "pipeline_manifest.yaml")

        import yaml  # lazy: keep PyYAML out of module-scope imports

        with open(manifest_path, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)

        if not isinstance(data, dict):
            return []
        tables = data.get("tables", [])
        if not isinstance(tables, list):
            return []
        # Keep only mapping entries so the ``ids`` lambda (and the test body's
        # ``table_spec[...]`` access) cannot raise during collection.
        return [table for table in tables if isinstance(table, dict)]
    except Exception:
        # Collection must never fail: a missing/broken manifest becomes an empty
        # catalog, which test_gate1_manifest_present turns into a visible skip.
        return []


#: The table catalog resolved once at import/collection time. Used to
#: parametrize Gate 1 (one test per table) and to decide whether the
#: empty-manifest fallback test is defined.
_MANIFEST_TABLES = _manifest_tables_for_ids()


# --------------------------------------------------------------------------- #
# Pure helpers (no heavy imports). These take already-constructed boto3 clients
# as parameters so the module never imports boto3 itself, and are written to be
# directly unit-testable with synthetic inputs.
# --------------------------------------------------------------------------- #
def _as_list(value):
    """Normalize an IAM JSON field that may be a scalar or a list into a list.

    IAM policy fields such as ``Resource`` and ``Action`` may be encoded either
    as a single string or as a list of strings. Normalizing to a list lets the
    wildcard detector iterate uniformly.

    Args:
        value: ``None``, a string, or a list/tuple of values.

    Returns:
        list: ``[]`` for ``None``; ``[value]`` for a string; a shallow copy for
        a list/tuple; otherwise ``[value]``.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _statements_of(policy_document):
    """Return the ``Statement`` block of an IAM policy document as a list.

    ``Statement`` may be a single statement object or a list of them; this
    normalizes both shapes to a list (and tolerates a malformed/absent block by
    returning ``[]``).

    Args:
        policy_document (dict): A parsed IAM policy document.

    Returns:
        list: The statements as a list (possibly empty).
    """
    statements = policy_document.get("Statement", [])
    if isinstance(statements, dict):
        return [statements]
    if isinstance(statements, list):
        return statements
    return []


def _wildcard_offenders(policy_document):
    """Detect wildcard ``*`` usage in an IAM policy document.

    The Gate 5 criterion is specifically about **resource** wildcards: a
    statement whose ``Resource`` contains a bare ``"*"`` is an offender and must
    fail the gate. A service-wide ``Action`` of ``"*"`` is also surfaced, but
    only as a *warning* (it is not the gate criterion -- a tightly resource-scoped
    statement is least-privilege even with a broad action set, though it is still
    worth flagging for a human reviewer).

    The detector is intentionally robust to the shapes IAM emits:

    * the document may arrive as a JSON *string* (e.g. a ``terraform output``
      render) -- it is parsed defensively;
    * ``Statement`` may be a single object or a list;
    * ``Resource`` / ``Action`` may each be a string or a list.

    Args:
        policy_document: A parsed IAM policy document (``dict``) or its JSON
            string encoding.

    Returns:
        tuple[list, list]: ``(resource_offenders, action_offenders)`` -- the
        ``Sid`` values (default ``"<no-sid>"``) of statements whose ``Resource``
        (respectively ``Action``) contains a bare ``"*"``.
    """
    # Defensive: a policy document fetched from a file or rendered by terraform
    # output may be a JSON string rather than a parsed mapping.
    if isinstance(policy_document, str):
        policy_document = json.loads(policy_document)

    resource_offenders = []
    action_offenders = []
    for statement in _statements_of(policy_document):
        if not isinstance(statement, dict):
            continue
        sid = statement.get("Sid", "<no-sid>")

        # Resource wildcard -> hard Gate 5 failure.
        resources = _as_list(statement.get("Resource"))
        if any(isinstance(res, str) and res.strip() == "*" for res in resources):
            resource_offenders.append(sid)

        # Action wildcard -> warning only (not the gate criterion).
        actions = _as_list(statement.get("Action"))
        if any(isinstance(act, str) and act.strip() == "*" for act in actions):
            action_offenders.append(sid)

    return resource_offenders, action_offenders


def _fetch_role_policy_documents(iam_client, role_name):
    """Return every IAM policy document attached to (or inline on) ``role_name``.

    Resolves both managed (attached) and inline policies via ``boto3`` IAM and
    returns their decoded policy documents (``boto3`` decodes each document to a
    ``dict``):

    * managed: :meth:`list_attached_role_policies` -> :meth:`get_policy`
      (default version id) -> :meth:`get_policy_version` -> ``Document``;
    * inline: :meth:`list_role_policies` -> :meth:`get_role_policy` ->
      ``PolicyDocument``.

    Args:
        iam_client: A ``boto3`` IAM client.
        role_name (str): The IAM role whose policies to enumerate.

    Returns:
        list[dict]: One policy document per attached/inline policy.
    """
    documents = []

    # Managed (attached) policies -- resolve each policy's default version doc.
    attached = iam_client.list_attached_role_policies(RoleName=role_name)
    for attached_policy in attached.get("AttachedPolicies", []):
        policy_arn = attached_policy["PolicyArn"]
        policy_meta = iam_client.get_policy(PolicyArn=policy_arn)["Policy"]
        default_version_id = policy_meta["DefaultVersionId"]
        version = iam_client.get_policy_version(
            PolicyArn=policy_arn, VersionId=default_version_id
        )["PolicyVersion"]
        documents.append(version["Document"])

    # Inline policies embedded directly on the role.
    inline = iam_client.list_role_policies(RoleName=role_name)
    for inline_name in inline.get("PolicyNames", []):
        document = iam_client.get_role_policy(
            RoleName=role_name, PolicyName=inline_name
        )["PolicyDocument"]
        documents.append(document)

    return documents


def _assert_delta_log_has_commit(s3_client, bucket, delta_path_prefix, relative_table_path):
    """Parity-supporting evidence (NOT Gate 2): a committed ``_delta_log`` exists.

    Lists the table's ``_delta_log/`` prefix and asserts it contains at least one
    committed ``.json`` transaction-log entry (excluding in-flight ``.tmp``
    files), confirming the deployed table was written through the ``LogStore``.
    This mirrors the repository's own integration-test pattern
    (``storage-s3-dynamodb/integration_tests/dynamodb_logstore.py`` lines
    196-218) and is deliberately lightweight -- it does **not** duplicate Gate 2
    (ACID verification), which counts conditional DynamoDB writes via CloudWatch.

    The boto3 listing call is guarded: a transient or permission error on this
    *ancillary* check is reported (via ``print``) but never fails the test -- the
    authoritative Gate 1 signal is the Spark-computed parity report. When the
    listing *does* succeed, an empty commit set is a genuine anomaly and is
    asserted against.

    Args:
        s3_client: A ``boto3`` S3 client.
        bucket (str): The Delta S3 bucket (``$DELTA_S3_BUCKET``).
        delta_path_prefix (str): The relative Delta root prefix
            (``defaults.delta_path_prefix``).
        relative_table_path (str): The table's relative ``path`` from the manifest.
    """
    relative_delta_log_path = f"{delta_path_prefix}/{relative_table_path}/_delta_log/"
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=relative_delta_log_path
        )
    except Exception as exc:  # ancillary check: never fail parity on a list error
        print(
            "[gate1-evidence] _delta_log listing skipped for "
            f"s3://{bucket}/{relative_delta_log_path}: {exc}"
        )
        return

    contents = response.get("Contents", [])
    commits = [
        obj["Key"]
        for obj in contents
        if ".json" in obj["Key"] and ".tmp" not in obj["Key"]
    ]
    assert commits, (
        "no committed .json transaction logs under "
        f"s3://{bucket}/{relative_delta_log_path} "
        "(DeltaTable.isDeltaTable reported a table but its _delta_log shows no "
        "committed version)"
    )


# --------------------------------------------------------------------------- #
# Gate 1 -- Parity (one parametrized test per table => enforces 100% of tables).
# --------------------------------------------------------------------------- #
@pytest.mark.gate1
@pytest.mark.parametrize(
    "table_spec",
    _MANIFEST_TABLES,
    ids=lambda table: table.get("name", "unknown"),
)
def test_gate1_table_parity(
    table_spec,
    spark,
    s3_client,
    delta_bucket,
    delta_path_prefix,
    parity_threshold,
    reference_baseline_loader,
):
    """Assert >= 99.99% row-count + 5-field-hash parity for one deployed table.

    Each manifest table is its own parametrized instance, so Gate 1 passes only
    when **every** table passes -- realizing the "100% of staging and final
    tables" acceptance criterion. The test skips (never errors) when its
    prerequisites are absent: ``pyspark``/Delta unavailable, the deployed Delta
    table is not present in this environment, or the baseline is not configured.
    """
    # Heavy imports are deferred to the test body so module import / collection
    # (and Gate 5) never require pyspark. The ``spark`` fixture has already gated
    # on pyspark + a startable session; this additionally probes the Delta
    # python package (lib.delta_io imports ``delta.tables``) and the reconciliation
    # helpers, skipping cleanly if either is unavailable.
    try:
        from lib.delta_io import read_delta, delta_table_exists
        from validate import reconciliation
    except Exception as exc:
        pytest.skip(f"pyspark/Delta reconciliation stack unavailable: {exc}")

    # Manifest-driven parity spec for this table.
    name = table_spec["name"]
    parity = table_spec["parity"]
    key_columns = parity["key_columns"]
    hash_columns = parity["hash_columns"]
    baseline_name = parity["baseline"]

    # Hard invariant (fail, not skip): the mandated parity hash is computed over
    # EXACTLY five fields. A different count means the manifest is malformed.
    assert len(hash_columns) == 5, (
        f"table {name!r}: parity.hash_columns must list exactly 5 columns "
        f"(the mandated 5-field hash), got {len(hash_columns)}: {hash_columns!r}"
    )

    # Absolute Delta location: s3a://{bucket}/{relative_prefix}/{table.path}.
    relative_table_path = table_spec["path"]
    path = f"s3a://{delta_bucket}/{delta_path_prefix}/{relative_table_path}"

    # The deployed table may not exist in this environment yet -> skip cleanly.
    if not delta_table_exists(spark, path):
        pytest.skip(f"actual Delta table absent: {path}")

    actual_df = read_delta(spark, path)

    # Parity-supporting evidence (NOT Gate 2): confirm the table committed through
    # the LogStore by listing its _delta_log/ for >= 1 .json commit. Lightweight
    # and guarded so it can never produce a false parity failure.
    _assert_delta_log_has_commit(
        s3_client, delta_bucket, delta_path_prefix, relative_table_path
    )

    # Sampled legacy SQL Server baseline; the loader skips internally when the
    # baseline root/object is not configured/found for this table.
    baseline_df = reference_baseline_loader(baseline_name)

    # Compute both parity signals; the report's pass/fail uses a >= comparison so
    # the boundary value 0.9999 (one mismatch in 10,000) correctly PASSES.
    report = reconciliation.table_parity_report(
        name,
        actual_df,
        baseline_df,
        key_columns,
        hash_columns,
        threshold=parity_threshold,
    )

    # The assertion message shows both parities, the threshold, both counts and
    # matched/total so a failing gate is immediately diagnosable.
    assert report["passed"], reconciliation.format_report(report)


# When the manifest yields no tables (missing/empty/broken manifest, or PyYAML
# absent at collection time), define a single visible-skip test so the condition
# surfaces as a SKIP rather than as silent non-collection of Gate 1.
if not _MANIFEST_TABLES:

    @pytest.mark.gate1
    def test_gate1_manifest_present():
        """Visible skip when ``config/pipeline_manifest.yaml`` yields no tables."""
        pytest.skip("no tables found in manifest")


# --------------------------------------------------------------------------- #
# Gate 5 -- Security (boto3-only; NO spark fixture, so it runs without pyspark).
# --------------------------------------------------------------------------- #
@pytest.mark.gate5
def test_gate5_no_wildcard_resources(iam_client):
    """Assert the IAM policy JSON contains no wildcard ``*`` *resources* (Gate 5a).

    The policy document(s) are resolved, in priority order, from:

    * ``$IAM_POLICY_JSON_PATH`` -- a JSON file holding a single policy document
      or a list of them (supports a ``terraform output``-rendered policy file); or
    * ``$GLUE_ROLE_NAME`` -- the role's attached (managed) and inline policies,
      enumerated and fetched via ``boto3`` IAM.

    The test skips (never errors) when neither source is configured. A bare
    ``"*"`` ``Resource`` in any statement fails the gate; a service-wide ``"*"``
    ``Action`` is reported as a warning only (it is not the gate criterion).
    """
    policy_json_path = os.environ.get("IAM_POLICY_JSON_PATH")
    role_name = os.environ.get("GLUE_ROLE_NAME")

    if policy_json_path:
        with open(policy_json_path, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
        # Support either a single policy document or a list of documents.
        policy_documents = loaded if isinstance(loaded, list) else [loaded]
    elif role_name:
        policy_documents = _fetch_role_policy_documents(iam_client, role_name)
    else:
        pytest.skip("neither IAM_POLICY_JSON_PATH nor GLUE_ROLE_NAME set")

    assert policy_documents, "no IAM policy documents resolved to inspect"

    resource_offenders = []
    action_offenders = []
    for document in policy_documents:
        res_off, act_off = _wildcard_offenders(document)
        resource_offenders.extend(res_off)
        action_offenders.extend(act_off)

    # Service-wide Action "*" is a smell but NOT the Gate 5 criterion; surface it
    # for a human reviewer without failing the gate (resource scoping is the
    # least-privilege control being enforced here).
    if action_offenders:
        print(
            "[gate5][warning] statements using a wildcard Action '*' "
            f"(resource scoping still enforced): {action_offenders}"
        )

    assert not resource_offenders, (
        "IAM policy contains wildcard '*' resources in statements: "
        f"{resource_offenders}"
    )


@pytest.mark.gate5
def test_gate5_simulate_principal_policy(iam_client):
    """Assert ``aws iam simulate-principal-policy`` allows the required calls (Gate 5b).

    Resolves the principal ARN from ``$GLUE_ROLE_ARN`` (or derives it from
    ``$GLUE_ROLE_NAME`` via :meth:`get_role`) and the (actions, resources) pairs
    to simulate from ``$IAM_SIMULATE_SPEC`` -- a JSON file shaped like::

        {"checks": [
            {"actions": ["s3:GetObject"],
             "resources": ["arn:aws:s3:::bucket/prefix/*"]},
            {"actions": ["dynamodb:PutItem", "dynamodb:GetItem",
                         "dynamodb:UpdateItem", "dynamodb:Query"],
             "resources": ["arn:aws:dynamodb:...:table/<ddb>"]}
        ]}

    ARNs are intentionally **not** fabricated here: they are environment/infra
    specific and supplied by the runner. The test skips (never errors) when the
    role or the simulate spec is not configured. Every evaluation decision must
    be ``"allowed"``; any ``explicitDeny``/``implicitDeny`` fails the gate with a
    message listing the offending action, resource and decision.
    """
    role_arn = os.environ.get("GLUE_ROLE_ARN")
    role_name = os.environ.get("GLUE_ROLE_NAME")
    if not role_arn:
        if role_name:
            role_arn = iam_client.get_role(RoleName=role_name)["Role"]["Arn"]
        else:
            pytest.skip("neither GLUE_ROLE_ARN nor GLUE_ROLE_NAME set")

    simulate_spec_path = os.environ.get("IAM_SIMULATE_SPEC")
    if not simulate_spec_path:
        pytest.skip("IAM_SIMULATE_SPEC not configured")

    with open(simulate_spec_path, "r", encoding="utf-8") as handle:
        spec = json.load(handle)
    checks = spec.get("checks", [])
    assert checks, f"IAM_SIMULATE_SPEC at {simulate_spec_path!r} defines no 'checks'"

    denied = []
    evaluated = 0
    for check in checks:
        actions = check["actions"]
        resources = check["resources"]
        response = iam_client.simulate_principal_policy(
            PolicySourceArn=role_arn,
            ActionNames=actions,
            ResourceArns=resources,
        )
        for result in response.get("EvaluationResults", []):
            evaluated += 1
            decision = result.get("EvalDecision")
            if decision != "allowed":
                denied.append(
                    f"{result.get('EvalActionName')} on "
                    f"{result.get('EvalResourceName')} -> {decision}"
                )

    # A simulation that evaluated nothing cannot have demonstrated the required
    # permissions -> fail loudly rather than passing vacuously.
    assert evaluated > 0, (
        "aws iam simulate-principal-policy returned no EvaluationResults for "
        f"role {role_arn!r} against spec {simulate_spec_path!r}"
    )
    assert not denied, (
        "aws iam simulate-principal-policy returned non-allowed decisions: "
        + "; ".join(denied)
    )
