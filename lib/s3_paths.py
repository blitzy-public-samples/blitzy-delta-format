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
"""Centralized, *validated* S3 URI construction for every ``jobs/stage_*.py`` entrypoint.

Why this module exists (security)
---------------------------------
Every Glue stage composes the fully qualified ``s3a://`` location of the Delta
table(s) it reads / writes from externally supplied / manifest-configured pieces:
the (scheme-less) destination bucket job argument, the manifest
``defaults.delta_path_prefix``, each table's relative ``path``, and -- for Stage 0
-- a ``quarantine_s3_path`` root plus the ``run_date`` argument. Composing those
pieces with nothing more than slash-stripping is a CWE-22 (path-traversal) class
gap: a ``..`` segment, an embedded ``s3://`` / ``file://`` scheme, a backslash, a
control character, a slash-bearing bucket name, or a malformed ``run_date`` would
flow straight into the object location and could redirect a read or (worse) a
write outside the intended prefix.

This module is the single choke point that closes that gap. It validates the
bucket name separately from the key, rejects traversal / scheme / backslash /
control-character / empty / absolute-segment tokens, validates ``run_date`` as a
strict ``YYYY-MM-DD`` calendar date before it is ever appended to a path, and only
then composes a normalized ``s3a://bucket/key`` URI. Routing all S3 URI
construction through these helpers means an individual stage cannot accidentally
hand-roll an unvalidated path.

Purity (leaf dependency, import-safe everywhere)
------------------------------------------------
This is a **pure, standard-library-only** module: it imports neither ``pyspark``,
``delta``, nor ``awsglue``, performs no I/O, builds no Spark session, and makes no
AWS calls. Importing it only defines functions and a small exception type, so it
is safe to import from any Glue job, from the ``validate/`` harness, and from unit
tests alike (mirroring the import discipline of ``schemas`` and ``lib`` -- see
``lib/__init__.py``). It deliberately depends only on ``re`` and ``datetime``.

Public API
----------
* :class:`S3PathError` -- raised for any rejected / malformed component.
* :func:`validate_bucket_name` -- validate a scheme-less S3 bucket name.
* :func:`validate_run_date` -- validate a ``YYYY-MM-DD`` run-date token.
* :func:`build_delta_table_uri` -- compose ``s3a://bucket/<prefix>/<rel_path>``.
* :func:`append_uri_segments` -- safely extend an existing ``s3``/``s3a`` URI.
* :func:`build_quarantine_uri` -- compose a run-scoped quarantine prefix URI.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Tuple

# The canonical scheme every Delta location is composed with. The pipeline commits
# through ``S3DynamoDBLogStore``, which is wired for BOTH the ``s3`` and ``s3a``
# Hadoop schemes (see ``lib/spark_session.py``); table locations are written with
# the ``s3a`` scheme throughout the jobs, so that is what these builders emit.
_DELTA_SCHEME = "s3a"

# Schemes accepted when *parsing* an already-qualified URI argument (for example the
# Stage 0 ``quarantine_s3_path`` root). Only the two S3 Hadoop schemes are allowed;
# any other scheme (``file://``, ``http://``, ``s3n://`` ...) is rejected so a
# misconfigured argument cannot redirect a write off S3.
_ALLOWED_URI_SCHEMES = ("s3a", "s3")

# A general-purpose S3 bucket name: 3-63 characters, lowercase letters / digits /
# dots / hyphens, beginning and ending with a letter or digit. This matches AWS's
# documented bucket-naming rules. The security-critical rejections (scheme, slash,
# backslash, ``..``, control characters, whitespace) are enforced explicitly BEFORE
# this pattern so they fail with a precise message rather than a generic regex miss.
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")

# Strict ``YYYY-MM-DD`` shape gate applied before a calendar-validity check with
# ``datetime.strptime`` (so ``2024-13-40`` is rejected even though it matches the
# shape). The run date becomes a path segment, so it must be a real, canonical date.
_RUN_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class S3PathError(ValueError):
    """Raised when an S3 bucket, path segment, URI, or run-date fails validation.

    Subclasses :class:`ValueError` so existing ``except (ValueError, ...)`` handling
    treats it as the configuration / input error that it is. It is raised eagerly
    (never swallowed) so a malformed or hostile path fails the Glue job fast and
    loud rather than silently resolving to an unintended S3 location.
    """


def _reject_control_chars(value: str, *, context: str) -> None:
    """Raise :class:`S3PathError` if ``value`` contains an ASCII control character.

    Control characters (``0x00``-``0x1F`` and ``0x7F``, which include NUL, tab,
    newline, and carriage return) have no legitimate place in a bucket name or an
    S3 key segment and are a classic smuggling vector, so they are rejected outright.

    :param value: The token to scan.
    :param context: Human-readable role of ``value`` for the error message.
    """
    for ch in value:
        if ord(ch) < 0x20 or ord(ch) == 0x7F:
            raise S3PathError(
                f"{context} contains a disallowed control character "
                f"(code point {ord(ch)!r}): {value!r}"
            )


def validate_bucket_name(bucket: str) -> str:
    """Validate and return a normalized, scheme-less S3 bucket name.

    Surrounding whitespace is tolerated (and stripped) for parity with the prior
    ``.strip()`` behavior, but the bucket is then validated strictly: it must carry
    no URI scheme (no ``:``), no slash, no backslash, no ``..`` traversal token, no
    control character, no internal whitespace, and must match AWS's documented
    general-purpose bucket-naming rules (3-63 chars, lowercase
    letters / digits / dots / hyphens, beginning and ending alphanumeric).

    :param bucket: The candidate bucket name (e.g. the scheme-less ``DELTA_S3_BUCKET``
        job argument). A leading/trailing-whitespace-only wrapper is allowed.
    :returns: The validated bucket name with surrounding whitespace removed.
    :raises S3PathError: If ``bucket`` is empty or violates any rule above.
    """
    if not isinstance(bucket, str):
        raise S3PathError(f"bucket name must be a string, got {type(bucket).__name__}")
    name = bucket.strip()
    if not name:
        raise S3PathError("bucket name is empty")
    _reject_control_chars(name, context="bucket name")
    if ":" in name or "://" in name:
        raise S3PathError(f"bucket name must not contain a URI scheme: {bucket!r}")
    if "/" in name:
        raise S3PathError(f"bucket name must not contain a slash: {bucket!r}")
    if "\\" in name:
        raise S3PathError(f"bucket name must not contain a backslash: {bucket!r}")
    if ".." in name:
        raise S3PathError(f"bucket name must not contain '..': {bucket!r}")
    if any(ch.isspace() for ch in name):
        raise S3PathError(f"bucket name must not contain whitespace: {bucket!r}")
    if not _BUCKET_RE.match(name):
        raise S3PathError(
            f"invalid S3 bucket name {bucket!r}: must be 3-63 characters of "
            f"lowercase letters, digits, dots, or hyphens and begin/end alphanumeric"
        )
    return name


def _validate_key_component(component: str, *, context: str) -> None:
    """Validate one ``/``-free component of an S3 key.

    A component is rejected when it is empty (which would mean a ``//`` or a
    leading / trailing slash, i.e. an absolute / malformed segment), is ``.`` or
    ``..`` (path traversal), or contains a backslash, a colon (an embedded scheme
    such as ``s3:``), a control character, or leading / trailing whitespace.

    :param component: A single key component, already split on ``/``.
    :param context: Human-readable role of the segment for the error message.
    :raises S3PathError: If the component violates any rule above.
    """
    if component == "":
        raise S3PathError(
            f"{context} contains an empty path segment (a leading/trailing or "
            f"doubled slash is not allowed)"
        )
    if component in (".", ".."):
        raise S3PathError(f"{context} contains a disallowed '{component}' path segment")
    if "/" in component:
        # A single key component must never itself contain a separator. When this
        # helper is reached after splitting on ``/`` the case cannot arise; it does
        # arise when a caller passes a value expected to be ONE component (for
        # example the Stage 0 quarantine ``table_name``), where a slash would
        # smuggle extra path levels -- so it is rejected outright.
        raise S3PathError(f"{context} contains a slash: {component!r}")
    if "\\" in component:
        raise S3PathError(f"{context} contains a backslash: {component!r}")
    if ":" in component:
        raise S3PathError(
            f"{context} contains a ':' (embedded URI scheme): {component!r}"
        )
    _reject_control_chars(component, context=context)
    if component != component.strip():
        raise S3PathError(
            f"{context} has leading/trailing whitespace: {component!r}"
        )


def _normalize_relative_key(segment: str, *, context: str) -> str:
    """Validate a relative path segment that may itself contain ``/`` and return it.

    Surrounding slashes / whitespace are trimmed, then every ``/``-delimited
    component is validated with :func:`_validate_key_component`. The empty string
    (after trimming) is allowed and returns ``""`` so an absent / optional prefix
    contributes nothing to the composed key.

    :param segment: A relative path piece such as ``"finance/sp_chain_replacement"``
        or ``"staging/staging_raw"``.
    :param context: Human-readable role of ``segment`` for the error message.
    :returns: The trimmed, validated relative key (possibly ``""``).
    :raises S3PathError: If any component is invalid.
    """
    if not isinstance(segment, str):
        raise S3PathError(
            f"{context} must be a string, got {type(segment).__name__}"
        )
    trimmed = segment.strip().strip("/")
    if trimmed == "":
        return ""
    for component in trimmed.split("/"):
        _validate_key_component(component, context=context)
    return trimmed


def _compose_key(*segments: str, context: str) -> str:
    """Validate and join relative ``segments`` into a single normalized S3 key.

    Each segment is normalized / validated independently (so traversal cannot be
    hidden across a segment boundary), empty results are dropped, and the survivors
    are joined with a single ``/``.

    :param segments: Relative key pieces, in order.
    :param context: Human-readable role for the error message.
    :returns: The composed, validated key (never starts or ends with ``/``).
    :raises S3PathError: If any component is invalid.
    """
    parts = [
        normalized
        for segment in segments
        if (normalized := _normalize_relative_key(segment, context=context))
    ]
    return "/".join(parts)


def build_delta_table_uri(bucket: str, prefix: str, rel_path: str) -> str:
    """Compose the fully qualified ``s3a://`` location of a Delta table, validated.

    Returns ``s3a://{bucket}/{prefix}/{rel_path}`` where ``bucket`` is validated by
    :func:`validate_bucket_name` and ``prefix`` / ``rel_path`` are each validated
    and joined by :func:`_compose_key` (rejecting ``..``, embedded schemes,
    backslashes, control characters, and empty / absolute segments). This replaces
    the prior slash-stripping-only composition used by the stage jobs.

    :param bucket: The scheme-less destination bucket (``DELTA_S3_BUCKET`` job arg).
    :param prefix: The manifest ``defaults.delta_path_prefix`` (relative).
    :param rel_path: The table's relative ``path`` from the manifest.
    :returns: The validated ``s3a://`` Delta table location.
    :raises S3PathError: If the bucket or any path segment is invalid.
    """
    valid_bucket = validate_bucket_name(bucket)
    key = _compose_key(prefix, rel_path, context="Delta table path")
    if not key:
        raise S3PathError(
            "Delta table path is empty after validation; a non-empty prefix or "
            "relative table path is required"
        )
    return f"{_DELTA_SCHEME}://{valid_bucket}/{key}"


def _parse_s3_uri(uri: str, *, context: str) -> Tuple[str, str, str]:
    """Parse and validate an already-qualified ``s3``/``s3a`` URI into its parts.

    :param uri: A URI such as ``s3a://bucket/key/prefix``.
    :param context: Human-readable role of ``uri`` for the error message.
    :returns: ``(scheme, bucket, key)`` where ``key`` may be ``""``.
    :raises S3PathError: If the scheme is missing / unsupported, or the bucket / key
        is invalid.
    """
    if not isinstance(uri, str):
        raise S3PathError(f"{context} must be a string, got {type(uri).__name__}")
    candidate = uri.strip()
    if "://" not in candidate:
        raise S3PathError(
            f"{context} must be a fully qualified s3a:// or s3:// URI: {uri!r}"
        )
    scheme, _, remainder = candidate.partition("://")
    scheme = scheme.lower()
    if scheme not in _ALLOWED_URI_SCHEMES:
        raise S3PathError(
            f"{context} has unsupported scheme {scheme!r}; allowed schemes are "
            f"{list(_ALLOWED_URI_SCHEMES)}: {uri!r}"
        )
    bucket_part, _, key_part = remainder.partition("/")
    valid_bucket = validate_bucket_name(bucket_part)
    valid_key = _compose_key(key_part, context=context)
    return scheme, valid_bucket, valid_key


def append_uri_segments(base_uri: str, *segments: str) -> str:
    """Return ``base_uri`` extended with additional validated path ``segments``.

    The base URI is parsed and validated by :func:`_parse_s3_uri` (scheme + bucket +
    existing key), each appended segment is validated by :func:`_compose_key`, and
    the pieces are rejoined into a single normalized URI. The base URI's original
    scheme is preserved.

    :param base_uri: A fully qualified ``s3a://`` / ``s3://`` root URI.
    :param segments: Additional relative path pieces to append, in order.
    :returns: The extended, validated URI.
    :raises S3PathError: If the base URI or any appended segment is invalid.
    """
    scheme, bucket, base_key = _parse_s3_uri(base_uri, context="S3 URI")
    appended = _compose_key(*segments, context="S3 URI path segment")
    key = "/".join(part for part in (base_key, appended) if part)
    if not key:
        raise S3PathError(f"S3 URI {base_uri!r} resolves to a bucket root with no key")
    return f"{scheme}://{bucket}/{key}"


def validate_run_date(run_date: str) -> str:
    """Validate ``run_date`` as a strict ``YYYY-MM-DD`` calendar date and return it.

    The value is first matched against the ``\\d{4}-\\d{2}-\\d{2}`` shape and then
    parsed with :func:`datetime.datetime.strptime` so an impossible date (for
    example ``2024-13-40``) is rejected even though it matches the shape. This must
    be called before a run date is appended to any S3 path segment.

    :param run_date: The run-date token (must be non-empty here; callers decide
        whether an empty run date is permitted before calling).
    :returns: The validated ``YYYY-MM-DD`` string (unchanged).
    :raises S3PathError: If ``run_date`` is not a real ``YYYY-MM-DD`` date.
    """
    if not isinstance(run_date, str):
        raise S3PathError(
            f"run_date must be a string, got {type(run_date).__name__}"
        )
    if not _RUN_DATE_RE.match(run_date):
        raise S3PathError(
            f"run_date must match YYYY-MM-DD, got {run_date!r}"
        )
    try:
        datetime.strptime(run_date, "%Y-%m-%d")
    except ValueError as exc:
        raise S3PathError(f"run_date {run_date!r} is not a valid calendar date") from exc
    return run_date


def build_quarantine_uri(quarantine_root: str, table_name: str, run_date: str = "") -> str:
    """Compose a run-scoped quarantine prefix URI from validated pieces.

    Returns ``<quarantine_root>/<table_name>[/<run_date>]``. The root is parsed and
    validated as an ``s3a``/``s3`` URI, ``table_name`` is validated as a single key
    component, and ``run_date`` -- when non-empty -- is validated as a strict
    ``YYYY-MM-DD`` date by :func:`validate_run_date` before being appended. An empty
    ``run_date`` is permitted and simply omits the trailing date segment.

    :param quarantine_root: The ``s3a://`` quarantine root (Stage 0 job argument).
    :param table_name: The logical table the quarantined records belong to.
    :param run_date: Optional ``YYYY-MM-DD`` run date; ``""`` omits the date segment.
    :returns: The validated quarantine prefix URI.
    :raises S3PathError: If the root, table name, or run date is invalid.
    """
    # ``table_name`` must be a single, ``/``-free component: validate it explicitly so
    # a slash-bearing or traversal table token cannot smuggle extra path levels.
    _validate_key_component(table_name.strip(), context="quarantine table name")
    segments = [table_name]
    if run_date:
        segments.append(validate_run_date(run_date))
    return append_uri_segments(quarantine_root, *segments)


__all__ = [
    "S3PathError",
    "validate_bucket_name",
    "validate_run_date",
    "build_delta_table_uri",
    "append_uri_segments",
    "build_quarantine_uri",
]
