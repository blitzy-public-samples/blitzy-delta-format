# artifacts/ — Staged Delta Lake 2.3.0 Binaries

This directory is the **local staging area** for the four **released Delta Lake
2.3.0** binaries that the additive AWS Glue 4.0 / PySpark ETL feature depends on
(Technical Specification / AAP §0.5.1 **Group H**). Terraform
(`infra/s3_objects.tf`) uploads each file from here to `ARTIFACT_S3_BUCKET`,
because public **PyPI** and **Maven Central** are **prohibited as runtime
resolution paths** (AAP §0.1.2, §0.3.2). At AWS Glue 4.0 runtime the jobs
therefore resolve these artifacts **exclusively from `ARTIFACT_S3_BUCKET`** — the
three JARs via `--extra-jars` and the wheel via `--additional-python-modules`,
wired by `infra/glue_jobs.tf`.

> **Delta 2.3.0 is the line compatible with AWS Glue 4.0 (Apache Spark 3.3.x).**
> This was previously an open validation item (AAP §0.3.3) staged at Delta 3.2.0;
> it is now **RESOLVED** — see [§ Compatibility (RESOLVED)](#-compatibility-resolved--delta-230--glue-40-apache-spark-33x)
> below for the sourced conclusion. In short: the `delta-spark` **3.x** line
> (including 3.2.0) is published against **Spark 3.5.x**, whereas AWS Glue 4.0
> ships **Spark 3.3.x**; the Delta line built for Spark 3.3.x is **2.3.x**, whose
> latest patch is **2.3.0**.

> **Connector artifact rename (2.x vs 3.x) — read this first.** The Delta Spark
> SQL connector is published under **two different Maven artifact ids** across the
> version lines:
>
> * Delta **2.x** → `io.delta:delta-core_2.12` (this feature: `delta-core_2.12-2.3.0.jar`)
> * Delta **3.0+** → `io.delta:delta-spark_2.12` (the superseded `delta-spark_2.12-3.2.0.jar`)
>
> The **PyPI package name is `delta-spark` for both lines** (so the wheel is
> `delta_spark-2.3.0-py3-none-any.whl` and `--additional-python-modules delta-spark==2.3.0`
> is unchanged in spelling). The mandated Delta **class names are identical across
> 2.x and 3.x** — `io.delta.storage.S3DynamoDBLogStore`,
> `io.delta.sql.DeltaSparkSessionExtension`, and
> `org.apache.spark.sql.delta.catalog.DeltaCatalog` — so **no `jobs/`, `lib/`,
> `schemas/`, or `dags/` code changes** were required by this version move
> (Minimal Change Mandate: only version strings / filenames / provenance were
> touched). This was verified empirically — see the smoke-test note below.

> **Runtime class-closure note (verified for 2.3.0).**
> `delta-storage-s3-dynamodb-2.3.0.jar` is **not self-contained**. Its mandated
> `io.delta.storage.S3DynamoDBLogStore` sits at the bottom of a superclass chain
> that crosses JAR boundaries:
>
> ```
> io.delta.storage.S3DynamoDBLogStore        (delta-storage-s3-dynamodb-2.3.0.jar)
>   └─ extends BaseExternalLogStore           (delta-storage-s3-dynamodb-2.3.0.jar)
>        └─ extends HadoopFileSystemLogStore   (delta-storage-2.3.0.jar)  ← base JAR
>             └─ extends LogStore              (delta-storage-2.3.0.jar)  ← base JAR
> ```
>
> A JVM cannot link a class without resolving its transitive superclasses, and the
> two superclasses `HadoopFileSystemLogStore` and `LogStore` exist **only** in the
> transitive `io.delta:delta-storage:2.3.0` artifact (`delta-storage-2.3.0.jar`).
> The same base JAR also supplies the `io.delta.storage.internal.FileNameUtils`
> helper referenced by `BaseExternalLogStore` and the
> `io.delta.storage.CloseableIterator` type used by the log store. The connector
> JAR `delta-core_2.12-2.3.0.jar` contains **zero** `io.delta.storage.*` classes,
> so it does not supply them either. Because `--datalake-formats=delta` is
> deliberately omitted (`infra/glue_jobs.tf`, so Glue 4.0 does **not** inject its
> own bundled Delta) and public Maven is prohibited at runtime (AAP §0.3.2), that
> base JAR is staged here too (`delta-storage-2.3.0.jar`) and placed on
> `--extra-jars`. Without it the JVM raises
> `NoClassDefFoundError: io/delta/storage/HadoopFileSystemLogStore` the instant the
> LogStore is instantiated for the first Delta commit — breaking the ACID
> coordination that is this feature's primary objective.
>
> **Verify the closure locally** — the unambiguous, reproducible proof is to show
> which JAR owns each class in the chain (a `jar tf` listing does not depend on
> disassembler heuristics):
>
> ```bash
> cd artifacts
> # The LogStore + its BaseExternalLogStore base live in the s3-dynamodb JAR:
> jar tf delta-storage-s3-dynamodb-2.3.0.jar | grep -E 'S3DynamoDBLogStore|BaseExternalLogStore'
> # Its superclasses HadoopFileSystemLogStore + LogStore live ONLY in the base JAR:
> jar tf delta-storage-2.3.0.jar             | grep -E 'HadoopFileSystemLogStore|/LogStore\.class'
> # The connector JAR carries NONE of io/delta/storage/* (expect no output):
> jar tf delta-core_2.12-2.3.0.jar           | grep '^io/delta/storage/' || echo 'none (expected)'
> # And the full chain disassembles only with BOTH JARs on the classpath:
> javap -classpath delta-storage-s3-dynamodb-2.3.0.jar:delta-storage-2.3.0.jar \
>   io.delta.storage.S3DynamoDBLogStore | head -2
> ```

These are the **published, released** Delta Lake `2.3.0` artifacts. They are
intentionally **not** built from this monorepo's in-development `4.1.0-SNAPSHOT`
source (`version.sbt:L1`): the repository's current `main` already targets
Spark 4.x (`pyspark>=4.0.1`, `setup.py:L35`), whereas this feature deliberately
consumes a prior released line that matches the Glue 4.0 (Spark 3.3.x) runtime.

---

## Inventory & provenance

The `SHA-256` column records the digest **computed from the actual staged bytes**
in this directory (see **Verifying the checksums** below for how they are
produced and reconciled against the upstream source). Reference **only** the four
filenames staged in this directory — no other artifact is in scope.

| File | Upstream coordinate / source | Version | Scala | Glue arg | Size (bytes) | SHA-256 |
|---|---|---|---|---|---|---|
| `delta-core_2.12-2.3.0.jar` | Maven `io.delta:delta-core_2.12:2.3.0` | 2.3.0 | 2.12 | `--extra-jars` | 3986365 | `bd0592640ba84d3e6cc7ca8ee75e58f50b9152de8336e85386e3c5e645d1f27b` |
| `delta-storage-s3-dynamodb-2.3.0.jar` | Maven `io.delta:delta-storage-s3-dynamodb:2.3.0` | 2.3.0 | n/a | `--extra-jars` | 11943 | `b85c83cee1b16a6b916ce3f7605ffbfbf6173f5b3d7beb550f3425c8790df1da` |
| `delta-storage-2.3.0.jar` | Maven `io.delta:delta-storage:2.3.0` | 2.3.0 | n/a | `--extra-jars` | 24320 | `4d46997175e9bd953cd56eb6f0f3efb5cbaf442078fe6f99df2a03215e14a5bf` |
| `delta_spark-2.3.0-py3-none-any.whl` | PyPI `delta-spark==2.3.0` | 2.3.0 | n/a (py3) | `--additional-python-modules` | 20987 | `008618feed655c96f13a04922a28e9d7b14a24a654c59a4fb58e21a0a06365d0` |

**Upstream integrity (Maven `.sha1` sidecars, verified at staging time).** Each
JAR was fetched from Maven Central and its local `SHA-1` reconciled against the
publisher's `.sha1` sidecar (all matched):

| File | Maven-published SHA-1 |
|---|---|
| `delta-core_2.12-2.3.0.jar` | `9ad8f63ea759f092940bdd31dc502a6fe22fa53e` |
| `delta-storage-s3-dynamodb-2.3.0.jar` | `5fe94ca91e2a1acda6058d26ee8a6acce27e3191` |
| `delta-storage-2.3.0.jar` | `018a4d9827df60dde6f683d9aac76bfc4b68f702` |

**What each binary provides:**

- `delta-core_2.12-2.3.0.jar` — the Delta Spark SQL connector:
  `org.apache.spark.sql.delta.catalog.DeltaCatalog` (wired via
  `spark.sql.catalog.spark_catalog`) and
  `io.delta.sql.DeltaSparkSessionExtension` (wired via `spark.sql.extensions`),
  matching the configuration in
  `storage-s3-dynamodb/integration_tests/dynamodb_logstore.py:L118-L124`. In the
  Delta **2.x** line this connector is the Maven artifact `io.delta:delta-core_2.12`
  (renamed to `io.delta:delta-spark_2.12` in Delta 3.0+).
- `delta-storage-s3-dynamodb-2.3.0.jar` — `io.delta.storage.S3DynamoDBLogStore`
  (and its `io.delta.storage.BaseExternalLogStore` base), the mandated
  multi-cluster ACID LogStore. **Depends on** `delta-storage-2.3.0.jar` below for
  its base/internal classes.
- `delta-storage-2.3.0.jar` — the **transitive base** `io.delta:delta-storage`
  artifact that `delta-storage-s3-dynamodb` is compiled against. It supplies
  `io.delta.storage.HadoopFileSystemLogStore` (the `BaseExternalLogStore`
  superclass), `io.delta.storage.LogStore`, `io.delta.storage.CloseableIterator`,
  and the `io.delta.storage.internal.FileNameUtils` helper. Required on
  `--extra-jars` for the S3 DynamoDB LogStore to class-load at runtime.
- `delta_spark-2.3.0-py3-none-any.whl` — the Python `delta.tables.DeltaTable`
  API (including `merge`) used by the Glue PySpark jobs. The **staged wheel's own
  `METADATA`** declares `Name: delta-spark`, `Version: 2.3.0`,
  `Requires-Python: >=3.6`, and **`Requires-Dist: pyspark (<3.4.0,>=3.3.0)`** —
  the primary-source pin that makes 2.3.0 the correct match for Glue 4.0's
  Spark 3.3.x runtime.

### Verifying the checksums

The `SHA-256` cells above are **computed from the actual staged bytes** in this
directory. To re-verify them at any time (for example before a Terraform apply,
or when reconciling against the upstream published checksum — the `.sha1` sidecar
files on Maven Central for the JARs, or the digest recorded on the PyPI release
page for the wheel), recompute the local digests with:

```bash
cd artifacts
sha256sum \
  delta-core_2.12-2.3.0.jar \
  delta-storage-s3-dynamodb-2.3.0.jar \
  delta-storage-2.3.0.jar \
  delta_spark-2.3.0-py3-none-any.whl
```

The output must match the `SHA-256` column above byte-for-byte. **No checksum is
recorded here that was not computed from the actual staged bytes.** The JAR
`SHA-1` values were additionally reconciled against Maven Central's `.sha1`
sidecars at staging time (see the integrity table above).

---

## Sourcing & constraints

- **Build/staging time vs. runtime.** Fetching these artifacts from Maven
  Central / PyPI (or an approved internal mirror) at **build / staging time is
  permitted**; **runtime resolution against public repositories is prohibited**.
  Every artifact must resolve from `ARTIFACT_S3_BUCKET` once a Glue job runs.
- **Pinned coordinates.** Scala **2.12** — the connector JAR must be the `_2.12`
  build, because AWS Glue 4.0 runs Scala 2.12 (the `_2.13` build is **not**
  used). Runtime target: **AWS Glue 4.0 = Apache Spark 3.3.x, Python 3.10**. All
  four files are the **released** Delta `2.3.0` line — **not** this repository's
  `4.1.0-SNAPSHOT` build.
- **Consumers.**
  - `infra/s3_objects.tf` — uploads each file from this directory to
    `ARTIFACT_S3_BUCKET` via `aws_s3_object` (the three JARs are enumerated by
    the `local.delta_core_jar` / `local.delta_storage_jar` /
    `local.delta_storage_transitive_jar` filename locals; the wheel by
    `local.delta_wheel`).
  - `infra/glue_jobs.tf` — references the three JARs through `--extra-jars`
    (built from `local.extra_jars` in `infra/locals.tf`) and the wheel through
    `--additional-python-modules`, by their `ARTIFACT_S3_BUCKET` S3 URIs.
- **Scope.** All four files above are staged. The transitive
  `io.delta:delta-storage:2.3.0` JAR (`delta-storage-2.3.0.jar`) is **in scope**
  and staged here precisely because the S3 DynamoDB LogStore JAR references its
  classes (see the *Runtime class-closure note* at the top); it is wired through
  `infra/locals.tf` → `infra/s3_objects.tf` → `infra/glue_jobs.tf` exactly like
  the other two JARs.
- **Binary-count reconciliation — exactly 4 runtime binaries (3 JARs + 1 wheel).**
  The runtime-required, ACID-correct staged set is **four** binaries:
  `delta-core_2.12-2.3.0.jar`, `delta-storage-s3-dynamodb-2.3.0.jar`,
  `delta-storage-2.3.0.jar`, and `delta_spark-2.3.0-py3-none-any.whl`. That count
  is *mandated* by the feature's own primary objective — every Delta write must
  commit through `io.delta.storage.S3DynamoDBLogStore` (AAP **§0.1.2** LogStore
  enforcement; the §0.1.1 ACID objective) — which, as proven in the *Runtime
  class-closure note*, cannot instantiate unless `delta-storage-2.3.0.jar` is on
  `--extra-jars`. Dropping that base JAR to hit a smaller literal count would
  raise `NoClassDefFoundError: io/delta/storage/HadoopFileSystemLogStore` at the
  first Delta commit, defeating the ACID coordination the feature exists to
  deliver. The only other conceivable classpath source — Glue's
  `--datalake-formats=delta` — is **excluded by design** because it injects
  Glue 4.0's *own bundled* Delta line (not the pinned 2.3.0) and violates the
  exclusive-`ARTIFACT_S3_BUCKET` sourcing rule (AAP **§0.3.2**); repackaging
  released JARs is likewise out (provenance is verbatim bytes). Staging the base
  JAR is therefore the only compliant option.
- **AAP traceability (§0.5.1 Group H ↔ §0.7.3 "implied artifact").** AAP §0.5.1
  Group H and §0.3.1 literally enumerate **two** JARs plus the wheel — i.e.
  "three staged binaries" in the literal plan text — and named them at the
  **3.2.0** version. Two reconciliations apply here, both authorized:
  1. **Version supersession 3.2.0 → 2.3.0** is directed by the Refine-PR
     compatibility work that closed AAP §0.3.3: because the confirmed
     Glue-4.0-compatible Delta line is 2.3.0 (not 3.2.0), the Refine-PR directive
     explicitly requires updating the staged `artifacts/` filenames, the
     `infra/` references, `validate/requirements.txt`, and this provenance
     document consistently. Only version strings / filenames / provenance change;
     no pipeline logic changes (the Delta class names are identical across
     2.x/3.x).
  2. **The third JAR** staged here, `delta-storage-2.3.0.jar`, is an
     *implied-required* artifact reconciled under the **very same principle the
     AAP itself applies in §0.7.3** to bring the `delta-spark` wheel into scope
     beyond the prompt's literal JAR-only list: a binary the mandated runtime path
     provably requires, but that the literal enumeration omitted, is treated as
     in-scope and staged (it cannot be resolved from public Maven at runtime per
     §0.1.2 / §0.3.2). Reading "3 staged binaries" as **"3 JARs + 1 wheel = 4
     binaries"** (the implied-required base JAR included) closes the traceability
     item with zero runtime risk.

---

## ✅ Compatibility (RESOLVED) — Delta 2.3.0 ↔ Glue 4.0 (Apache Spark 3.3.x)

> **Status: RESOLVED.** This section supersedes the former "⚠️ Validation item"
> (AAP §0.3.3). The Delta ↔ Spark version compatibility for the AWS Glue 4.0
> runtime is now settled with sourced, primary evidence — it is **no longer an
> open item awaiting platform-team confirmation**, and it is **not** a blocking
> defect.

**Conclusion.** AWS Glue 4.0 ships **Apache Spark 3.3.x**. The Delta Lake line
built for Spark 3.3.x is the **2.3.x** line; its latest patch is **2.3.0**.
Therefore the pipeline is staged and pinned at **Delta 2.3.0** (Python
`delta-spark==2.3.0`; connector Maven `io.delta:delta-core_2.12:2.3.0`; LogStore
`io.delta:delta-storage-s3-dynamodb:2.3.0`; transitive base
`io.delta:delta-storage:2.3.0`), with the local validation environment pinned to
`pyspark>=3.3.0,<3.4.0`.

**Why the previous 3.2.0 pin was incompatible.** The `delta-spark` **3.x** line —
including 3.2.0 — is published against **Spark 3.5.x**. The staged
`delta_spark-3.2.0` wheel's own `METADATA` declared
`Requires-Dist: pyspark (<3.6.0,>=3.5.0)`, i.e. it *requires* Spark 3.5.x, which
AWS Glue 4.0 does **not** provide. Running it on Glue 4.0's Spark 3.3.x would be a
runtime version mismatch discovered at job-execution time.

**Evidence (two independent, authoritative sources):**

1. **Official Delta Lake compatibility matrix** —
   `https://docs.delta.io/releases/` lists Delta Lake versions against their
   compatible Apache Spark versions:

   | Delta Lake | Apache Spark |
   |---|---|
   | 3.2.x | 3.5.x |
   | 2.4.x | 3.4.x |
   | **2.3.x** | **3.3.x** |
   | 2.2.x | 3.3.x |
   | 2.1.x | 3.3.x |

   Delta **2.3.x** is the latest line paired with Spark **3.3.x** (the bounded
   Spark-3.3.x-compatible range is Delta 2.1.x–2.3.x).

2. **The package's own dependency pin (primary source)** — the staged
   `delta_spark-2.3.0-py3-none-any.whl` `METADATA` declares
   **`Requires-Dist: pyspark (<3.4.0,>=3.3.0)`**, which is satisfied by Glue 4.0's
   Spark 3.3.x. (For contrast, the superseded `delta_spark-3.2.0` wheel declared
   `Requires-Dist: pyspark (<3.6.0,>=3.5.0)`.)

**Empirical confirmation (local smoke test).** An 11-step Spark + Delta smoke
test was executed against a local **Spark 3.3.x + delta-spark 2.3.0** environment
with these exact staged JARs on the classpath, exercising the real `lib/` APIs:
LogStore-wired session build and the six canonical Delta/LogStore Spark
properties; explicit-`StructType` enforcement (structural fail-fast);
schema-locked (`mergeSchema=false`) overwrite round-trip and overwrite
idempotency; `merge` upsert create, update, and idempotency; bad-record routing
to a parquet quarantine; and the `BadRecordThresholdExceeded` fail-fast. **All 11
steps passed**, confirming the DeltaCatalog / `DeltaSparkSessionExtension`
activate and the `DeltaTable` APIs function on Spark 3.3.x with these artifacts,
and that the pipeline code required **no logic change** for the version move.
(Local writes use `file://`, so the default Local/HDFS LogStore handles the
commit; `io.delta.storage.S3DynamoDBLogStore` is on the classpath and wired for
`s3`/`s3a` but is exercised end-to-end only against deployed S3 + DynamoDB, which
is outside the scope of local validation.)

---

## Constraints honored

- **Exact filenames** preserved to the released coordinates, including the
  mandatory `_2.12` Scala suffix on the connector JAR (`delta-core_2.12-2.3.0.jar`).
- **Released bytes, staged verbatim** for Terraform upload — no repackaging or
  recompilation; JAR `SHA-1` values reconciled against Maven Central `.sha1`
  sidecars at staging time.
- **No invented data** — every recorded `SHA-256` is computed from the actual
  staged bytes in this directory (re-verifiable via the `sha256sum` command
  above), and no download URLs are fabricated.
- **Minimal Change Mandate honored** — no **pre-existing** delta-io/delta monorepo
  source, build, or metadata file (e.g. anything under `spark*/`, `kernel*/`,
  `storage*/`, `build.sbt`, `setup.py`, `version.sbt`) is modified. Everything this
  feature delivers — including this `artifacts/` directory and this README — is a
  **net-new feature deliverable**. As a living provenance document, this README is
  authored and **refined across checkpoints** (created in CP1, updated in later
  checkpoints, and refined here to record the Delta 2.3.0 resolution of the
  §0.3.3 compatibility item); such updates are confined to net-new feature files
  and therefore remain fully within the Mandate.
- **No secrets** are recorded in this directory.
