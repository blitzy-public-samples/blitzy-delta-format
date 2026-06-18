# `artifacts/` — Staged Delta Lake 3.2.0 Binaries (Provenance & Checksums)

This directory holds **net-new, pre-staged released binaries** for the additive
AWS Glue 4.0 / Delta Lake ETL feature (Technical Specification / AAP §0.5.1
**Group H**). The files here are **official, unmodified released artifacts** that
are committed to the repository so that Terraform can upload them to
`ARTIFACT_S3_BUCKET` and the Glue jobs can resolve them **without any runtime
access to public Maven Central or PyPI**.

> **Runtime sourcing rule (AAP §0.1.2, §0.3.2):** fetching these artifacts from
> Maven Central (or an approved internal mirror) at **build / staging time is
> permitted**; **public-repository resolution at job runtime is prohibited**.
> That is the entire reason the binaries are staged here for upload to
> `ARTIFACT_S3_BUCKET` rather than downloaded by the Glue runtime.

---

## How these artifacts are consumed

| Stage | Mechanism | Reference |
|---|---|---|
| Staging-time upload | `aws_s3_object` resources upload each file from this directory to `ARTIFACT_S3_BUCKET` | `infra/s3_objects.tf` |
| Glue JVM classpath | Glue jobs reference the two JARs by S3 URI via `--extra-jars s3://<ARTIFACT_S3_BUCKET>/.../<jar>` | `infra/glue_jobs.tf` |
| Glue Python modules | The wheel is resolved via `--additional-python-modules` from the same bucket | `infra/glue_jobs.tf` |
| Spark wiring | `lib/spark_session.py` sets `spark.sql.extensions` and `spark.sql.catalog.spark_catalog` to classes provided by `delta-spark_2.12-3.2.0.jar` (pattern from `storage-s3-dynamodb/integration_tests/dynamodb_logstore.py:L118-L119`) | `lib/spark_session.py` |

---

## Staged artifacts — provenance & checksums

All three artifacts are the **Delta Lake 3.2.0** release line, published by
`io.delta`. The canonical upstream source is **Maven Central** (the JARs) and the
matching **PyPI release** (the wheel). In this build environment the exact bytes
were obtained from the pre-provisioned setup cache (`/opt/delta-artifacts/`,
populated at environment-setup time from the upstream coordinates below) and
copied here **byte-for-byte unmodified** (no repackaging, recompilation, or
stripping).

### 1. `delta-spark_2.12-3.2.0.jar` — Delta Spark SQL connector (primary deliverable)

- **Maven coordinate:** `io.delta:delta-spark_2.12:3.2.0`
- **Scala version:** **2.12** (required — AWS Glue 4.0 runs Scala 2.12; the `_2.13` build is intentionally **not** used)
- **Delta version:** `3.2.0` (the released line — **not** this monorepo's in-development `4.1.0-SNAPSHOT`, and **not** the Spark-4.x / `pyspark>=4.0.1` line that the repo's `main` currently targets)
- **Upstream URL (canonical):** `https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar`
- **Size:** 6,111,817 bytes
- **SHA-256:** `51d473537d1bc10c81f48b03d8e2a6b604e1b421a70835ec12e917a4245a31d5`
- **SHA-1:** `0e648d893d3e6cda312d0a6dc4c8f64f3dbe0fb5`
- **MD5:** `c3e3fb64badc64e7bcf2553a339d4dd4`
- **Provides (verified present in the JAR):**
  - `org.apache.spark.sql.delta.catalog.DeltaCatalog` — wired via `spark.sql.catalog.spark_catalog`
  - `io.delta.sql.DeltaSparkSessionExtension` — wired via `spark.sql.extensions`
- **MANIFEST identity (verified):** `Implementation-Title: delta-spark`, `Implementation-Version: 3.2.0`, `Implementation-Vendor: io.delta`

### 2. `delta-storage-s3-dynamodb-3.2.0.jar` — multi-cluster ACID LogStore

- **Maven coordinate:** `io.delta:delta-storage-s3-dynamodb:3.2.0`
- **Upstream URL (canonical):** `https://repo1.maven.org/maven2/io/delta/delta-storage-s3-dynamodb/3.2.0/delta-storage-s3-dynamodb-3.2.0.jar`
- **Size:** 15,606 bytes
- **SHA-256:** `375b202558c1809ba28dfd2f3b3f1e1a51155ceff9561b83f31809800d3759c2`
- **SHA-1:** `2c1ecf58670310509500c2c80f816bd4067d0d69`
- **MD5:** `2f5457042cb9805c835be98575a30fd1`
- **Provides:** `io.delta.storage.S3DynamoDBLogStore` (the mandated multi-cluster ACID LogStore)

> Note: this file is part of the same staging set and is listed here for a
> complete directory provenance record. It is delivered by its own task.

### Transitive runtime dependency (platform-team note)

`io.delta:delta-storage-s3-dynamodb` depends transitively on
`io.delta:delta-storage`. The build cache for this environment also contains:

| Artifact | Upstream source | Version | Size (bytes) | SHA-256 |
|---|---|---|---|---|
| `delta-storage-3.2.0.jar` | Maven Central `io.delta:delta-storage` | `3.2.0` | 24946 | `58aab63eba7736fea9e03eafb0dde6704a34a70f570c1a69ab8e4012c25a95d4` |

AAP §0.5.1 (Group H) enumerates only the two `delta-spark_2.12` and
`delta-storage-s3-dynamodb` JARs plus this wheel. **Action for the platform team:**
confirm whether `delta-storage-3.2.0.jar` must also be staged to
`ARTIFACT_S3_BUCKET` and added to `--extra-jars` for the LogStore to load at Glue
runtime. It is intentionally **not** added here to honor the AAP's explicit scope;
this is recorded as an open item rather than a silent decision.

### 3. `delta_spark-3.2.0-py3-none-any.whl` — Python Delta Lake API

- **PyPI project:** `delta-spark` (import package `delta`), version `3.2.0`
- **Upstream URL (canonical):** `https://files.pythonhosted.org/packages/py3/d/delta-spark/delta_spark-3.2.0-py3-none-any.whl`
- **Size:** 21,186 bytes
- **SHA-256:** `c4ff3fa7218e58a702cb71eb64384b0005c4d6f0bbdd0fe0b38a53564d946e09`
- **SHA-1:** `8ee124b668d826f05fc5c218827a2b143c56e1ce`
- **MD5:** `fd3e7e4b86dbaffd1261f4b64cae4de9`
- **Provides:** the Python `DeltaTable` / `merge` API used by the Glue PySpark jobs

> Note: this file is part of the same staging set and is listed here for a
> complete directory provenance record. It is delivered by its own task.

---

## ⚠️ Compatibility validation item (AAP §0.3.3) — platform-team confirmation required

The Delta 3.x line — **including `delta-spark` 3.2.0** — is published against the
**Apache Spark 3.5.x** line, whereas **AWS Glue 4.0 ships Apache Spark 3.3.x**.
The Delta release that aligns to the Spark 3.3.x line is the **2.1.x–2.3.x**
series. The mandated versions in this feature (Delta 3.2.0 on Glue 4.0) are
**authoritative inputs from the prompt** and are staged here **verbatim** — they
are **deliberately not silently swapped** to a different Delta version.

**Action for the platform team:** confirm runtime compatibility of Delta 3.2.0
on the Glue 4.0 (Spark 3.3.x) runtime, **or** escalate a coordinated version
adjustment, **before production cutover**. This must be resolved up front rather
than discovered at job-execution time.

---

## Verification

The staged copies were verified byte-for-byte against the upstream cache
(`cmp` reported no differences) and structurally validated (`unzip -t` reported
no CRC errors). To re-verify the primary JAR locally:

```bash
# 1) Confirm exact filename (note the mandatory _2.12 Scala suffix)
ls -l artifacts/delta-spark_2.12-3.2.0.jar

# 2) Confirm it is a valid Java archive
file artifacts/delta-spark_2.12-3.2.0.jar

# 3) Confirm the SHA-256 matches the value recorded above
sha256sum artifacts/delta-spark_2.12-3.2.0.jar
# expected: 51d473537d1bc10c81f48b03d8e2a6b604e1b421a70835ec12e917a4245a31d5

# 4) Confirm the connector classes the Spark session wires are present
unzip -l artifacts/delta-spark_2.12-3.2.0.jar \
  | grep -E 'org/apache/spark/sql/delta/catalog/DeltaCatalog\.class|io/delta/sql/DeltaSparkSessionExtension\.class'
```

### Note on upstream checksum cross-check

An online cross-check of the recorded SHA-256/SHA-1 against the live Maven
Central `.sha1`/`.sha256` sidecar files **could not be performed in this offline
build environment** (no outbound network / package-index access at staging time).
Authenticity is instead established by the strong, locally verifiable evidence
above: the JAR's `META-INF/MANIFEST.MF` identifies it as `io.delta` `delta-spark`
`3.2.0`; the archive passes integrity checks with no CRC errors; both required
connector classes are present; and the environment's setup step exercised these
exact binaries in a passing PySpark + Delta ACID write/merge smoke test. When
network access is available, the recorded checksums should be reconciled against
the upstream `repo1.maven.org` sidecar files as a final confirmation step.

---

## Constraints honored

- **Exact filenames** preserved (Scala `_2.12` suffix on the connector JAR).
- **Released bytes unmodified** — copied verbatim; not repackaged or recompiled.
- **Purely additive** — no existing monorepo file was modified (Minimal Change Mandate).
- **No secrets** are recorded in this directory; all checksums above are public artifact digests.
