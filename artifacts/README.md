# artifacts/ — Staged Delta Lake 3.2.0 Binaries

This directory is the **local staging area** for the three **released Delta Lake
3.2.0** binaries that the additive AWS Glue 4.0 / PySpark ETL feature depends on
(Technical Specification / AAP §0.5.1 **Group H**). Terraform
(`infra/s3_objects.tf`) uploads each file from here to `ARTIFACT_S3_BUCKET`,
because public **PyPI** and **Maven Central** are **prohibited as runtime
resolution paths** (AAP §0.1.2, §0.3.2). At AWS Glue 4.0 runtime the jobs
therefore resolve these artifacts **exclusively from `ARTIFACT_S3_BUCKET`** — the
two JARs via `--extra-jars` and the wheel via `--additional-python-modules`,
wired by `infra/glue_jobs.tf`.

These are the **published, released** Delta Lake `3.2.0` artifacts. They are
intentionally **not** built from this monorepo's in-development `4.1.0-SNAPSHOT`
source (`version.sbt:L1`): the repository's current `main` already targets
Spark 4.x (`pyspark>=4.0.1`, `setup.py:L35`), whereas this feature deliberately
consumes the prior released line.

---

## Inventory & provenance

The `SHA-256` column holds the literal placeholder `<sha256>`; see
**Filling in the checksums** below for how to populate it at staging time.
Reference **only** the three filenames staged in this directory — no other
artifact is in scope.

| File | Upstream coordinate / source | Version | Scala | Glue arg | SHA-256 |
|---|---|---|---|---|---|
| `delta-spark_2.12-3.2.0.jar` | Maven `io.delta:delta-spark_2.12:3.2.0` | 3.2.0 | 2.12 | `--extra-jars` | `<sha256>` |
| `delta-storage-s3-dynamodb-3.2.0.jar` | Maven `io.delta:delta-storage-s3-dynamodb:3.2.0` | 3.2.0 | n/a | `--extra-jars` | `<sha256>` |
| `delta_spark-3.2.0-py3-none-any.whl` | PyPI `delta-spark==3.2.0` | 3.2.0 | n/a (py3) | `--additional-python-modules` | `<sha256>` |

**What each binary provides:**

- `delta-spark_2.12-3.2.0.jar` — the Delta Spark SQL connector:
  `org.apache.spark.sql.delta.catalog.DeltaCatalog` (wired via
  `spark.sql.catalog.spark_catalog`) and
  `io.delta.sql.DeltaSparkSessionExtension` (wired via `spark.sql.extensions`),
  matching the configuration in
  `storage-s3-dynamodb/integration_tests/dynamodb_logstore.py:L118-L124`.
- `delta-storage-s3-dynamodb-3.2.0.jar` — `io.delta.storage.S3DynamoDBLogStore`,
  the mandated multi-cluster ACID LogStore.
- `delta_spark-3.2.0-py3-none-any.whl` — the Python `delta.tables.DeltaTable`
  API (including `merge`) used by the Glue PySpark jobs. PyPI project identity
  `delta_spark` (`setup.py:L39`); `python_requires='>=3.10'` (`setup.py:L36`).

### Filling in the checksums

The `<sha256>` cells are placeholders. **At staging time a maintainer must
replace each `<sha256>` with the actual SHA-256 digest of the corresponding
staged file**, and cross-check it against the upstream published checksum (the
`.sha256` / `.sha1` sidecar files on Maven Central for the JARs, or the digest
recorded on the PyPI release page for the wheel). Compute the local digests with:

```bash
cd artifacts
sha256sum \
  delta-spark_2.12-3.2.0.jar \
  delta-storage-s3-dynamodb-3.2.0.jar \
  delta_spark-3.2.0-py3-none-any.whl
```

Do **not** record any checksum that has not been computed from the actual staged
bytes and reconciled with the upstream source.

---

## Sourcing & constraints

- **Build/staging time vs. runtime.** Fetching these artifacts from Maven
  Central / PyPI (or an approved internal mirror) at **build / staging time is
  permitted**; **runtime resolution against public repositories is prohibited**.
  Every artifact must resolve from `ARTIFACT_S3_BUCKET` once a Glue job runs.
- **Pinned coordinates.** Scala **2.12** — the connector JAR must be the `_2.12`
  build, because AWS Glue 4.0 runs Scala 2.12 (the `_2.13` build is **not**
  used). Runtime target: **AWS Glue 4.0 = Apache Spark 3.3.x, Python 3.10**. All
  three files are the **released** Delta `3.2.0` line — **not** this repository's
  `4.1.0-SNAPSHOT` build.
- **Consumers.**
  - `infra/s3_objects.tf` — uploads each file from this directory to
    `ARTIFACT_S3_BUCKET` via `aws_s3_object`.
  - `infra/glue_jobs.tf` — references the two JARs through `--extra-jars` and the
    wheel through `--additional-python-modules`, by their `ARTIFACT_S3_BUCKET`
    S3 URIs.
- **Scope.** Only the three files above are staged. The transitive
  `io.delta:delta-storage:3.2.0` JAR is **intentionally out of scope** here and
  is **not** staged in this directory.

---

## ⚠️ Validation item — Delta 3.2.0 ↔ Glue 4.0 (Spark) compatibility

> **⚠️ Validation Item (AAP §0.3.3, §0.7.3) — platform-team confirmation required.**
>
> The Delta Lake 3.x line — **including `delta-spark` 3.2.0** — is published
> against the **Apache Spark 3.5.x** line, whereas **AWS Glue 4.0 ships Apache
> Spark 3.3.x**. The versions pinned by this feature (Delta 3.2.0 on Glue 4.0)
> are **authoritative inputs from the prompt** and are staged here **verbatim**.
>
> This tension **must be confirmed with the platform team — or a coordinated
> version adjustment escalated — before production cutover.** Per the Minimal
> Change Mandate it is **documented here, not silently changed**, so that it is
> resolved up front rather than discovered at job-execution time.

---

## Constraints honored

- **Exact filenames** preserved, including the mandatory `_2.12` Scala suffix on
  the connector JAR.
- **Released bytes, staged verbatim** for Terraform upload — no repackaging or
  recompilation.
- **No invented data** — checksums are `<sha256>` placeholders to be filled at
  staging time, and no download URLs are fabricated.
- **Purely additive** — no existing monorepo file is modified (Minimal Change
  Mandate).
- **No secrets** are recorded in this directory.
