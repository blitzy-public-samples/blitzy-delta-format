#
# Copyright (2026) The Delta Lake Project Authors.
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
"""Shared PySpark library for the config-driven, ACID Delta-on-Glue ETL pipeline.

Provides a LogStore-wired ``SparkSession`` bootstrap, Delta I/O helpers, schema
validation/quarantine, manifest + source-contract loaders, structured CloudWatch
logging, and Glue argument resolution. These building blocks are consumed by
every ``jobs/stage_*.py`` AWS Glue 4.0 (Apache Spark 3.3.x / Python 3.10)
entrypoint and by the ``validate/`` parity-and-security harness.

Submodules (imported explicitly by consumers, never eagerly from this package):

* ``lib.spark_session`` -- build a ``SparkSession`` that injects the four
  ``io.delta.storage.S3DynamoDBLogStore`` properties plus the Delta SQL
  extension and catalog required by the DeltaTable APIs.
* ``lib.delta_io`` -- Delta read / overwrite / merge helpers that always set
  ``mergeSchema=false``.
* ``lib.schema_validation`` -- explicit ``StructType`` conformance checks with
  bad-record routing, quarantine, and counting.
* ``lib.manifest`` -- loader for ``config/pipeline_manifest.yaml``.
* ``lib.source_contract`` -- loader for the per-pipeline source-contract YAML.
* ``lib.logging_utils`` -- emit the six-field structured CloudWatch completion
  event (job name, Glue run id, input rows, output rows, bad records, seconds).
* ``lib.job_args`` -- AWS Glue ``getResolvedOptions`` argument resolution.

Design note: this package initializer is intentionally lightweight and performs
**no** imports of ``pyspark``, ``delta``, or ``awsglue`` at package-import time.
Those libraries exist only inside the Glue / MWAA runtime, whereas pure-Python
contexts (the ``validate/`` harness and unit tests) import only
``lib.manifest`` / ``lib.source_contract``. Keeping this initializer import-free
is what lets ``lib`` function as a package in both environments. Consumers import
the specific submodule they need, for example::

    from lib.spark_session import build_spark_session
"""

__version__ = "0.1.0"

# The package deliberately exposes no eager re-exports: submodules are imported
# explicitly by consumers (see the module docstring). An empty ``__all__`` keeps
# ``from lib import *`` a deliberate no-op instead of an implicit, heavy import.
__all__ = []
