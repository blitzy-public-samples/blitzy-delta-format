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
"""AWS Glue job-argument resolution helpers.

This module wraps AWS Glue's :func:`awsglue.utils.getResolvedOptions` so that
every Glue job entrypoint under ``jobs/`` resolves its parameters uniformly,
supporting both *required* arguments and *optional* arguments that fall back to
a default value when they are not supplied on the command line.

Glue parameter convention
-------------------------
AWS Glue passes job arguments on the command line as ``--KEY value`` pairs and
exposes them to the job keyed by ``KEY`` (without the leading ``--``). The
``JOB_NAME`` argument is always present on a Glue job run.

Native ``getResolvedOptions(sys.argv, names)`` raises if *any* requested name is
absent from ``sys.argv``; it therefore supports required arguments only. This
module emulates optional-with-default arguments by asking Glue to resolve only
the optional keys that are actually present in ``sys.argv`` and then filling in
the configured defaults for the rest.

All resolved values are returned as strings, because Glue passes every argument
as a string. Callers are responsible for any casting they require, for example
``float(args["bad_record_threshold"])``.

Expected stage parameter contract (documentation only)
------------------------------------------------------
The functions here are fully generic; the parameter names below are documented
purely as a contract for the ``jobs/`` stage entrypoints and are intentionally
*not* hardcoded into the resolution logic:

* required: ``JOB_NAME``
* common: ``--ddb_table_name``, ``--aws_region``, ``--input_table_path``,
  ``--output_table_path``, ``--run_date``, ``--source_s3_path``,
  ``--quarantine_s3_path``
* optional-with-default: ``--bad_record_threshold`` (default ``"0.0"``),
  ``--write_mode``, ``--merge_condition``

The ``awsglue`` package is provided exclusively by the AWS Glue runtime and is
not pip-installable in local or test contexts. Its import is therefore deferred
into the function body so this module can be imported safely in lightweight
contexts (unit tests, the ``validate/`` harness) without raising ``ImportError``.
"""

from __future__ import annotations

import sys


def resolve_options(required: list[str], optional: dict[str, str] | None = None) -> dict[str, str]:
    """Resolve Glue job arguments with required and optional-with-default support.

    Parameters
    ----------
    required:
        Names of arguments that must be present on the Glue job run (each passed
        as ``--NAME value``). If any required name is missing, the underlying
        :func:`awsglue.utils.getResolvedOptions` raises, failing the job fast.
    optional:
        Mapping of optional argument name to the default value used when the
        argument is absent from ``sys.argv``. A present optional argument always
        overrides its configured default.

    Returns
    -------
    dict[str, str]
        Mapping of every requested argument name to its resolved string value.
        Optional names absent from the command line are populated with their
        configured default. Every value is a string, matching Glue semantics.
    """
    from awsglue.utils import getResolvedOptions  # deferred: Glue-runtime only

    optional = optional or {}
    # Only ask Glue to resolve optional keys that are actually present in argv,
    # because getResolvedOptions raises on any missing requested name.
    present_optional = [k for k in optional if f"--{k}" in sys.argv]
    resolved = getResolvedOptions(sys.argv, list(required) + present_optional)
    out = dict(resolved)
    for key, default in optional.items():
        out.setdefault(key, default)
    return out


def get_job_name() -> str:
    """Return the Glue ``JOB_NAME`` argument.

    Convenience wrapper for callers (such as the logging helpers) that only need
    the job name. ``JOB_NAME`` is always present on a Glue job run, so this never
    falls back to a default.
    """
    return resolve_options(["JOB_NAME"])["JOB_NAME"]
