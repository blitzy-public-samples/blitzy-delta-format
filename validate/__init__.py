#
# Copyright (2021) The Delta Lake Project Authors.
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

"""validate - parity (Gate 1) and security (Gate 5) acceptance-test harness.

This package houses the pytest-based acceptance gates for the config-driven
AWS Glue 4.0 / PySpark + Delta Lake ETL pipeline that replaces the legacy SQL
Server stored-procedure chain. It validates end-to-end functional parity
(row-count plus a five-field hash) against the legacy baseline and asserts the
least-privilege IAM policy via ``aws iam simulate-principal-policy``.

Run with::

    pytest validate/test_parity.py --env <env>

Notes
-----
This module is intentionally a lightweight package marker. It performs no
heavyweight imports (``pyspark``, ``delta``, ``boto3``, ``awsglue``) at import
time so that test collection and the security gate remain runnable in
interpreters where those optional, AWS-only dependencies are absent. The
presence of this file also places the repository root on ``sys.path`` under
pytest's default "prepend" import mode, which makes sibling top-level packages
(for example ``lib``) importable from the test modules.
"""

__version__ = "0.1.0"
