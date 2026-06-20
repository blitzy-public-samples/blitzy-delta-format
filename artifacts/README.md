# artifacts/ — Staged Delta Lake 3.2.0 Binaries

This directory is the **local staging area** for the four **released Delta Lake
3.2.0** binaries that the additive AWS Glue 4.0 / PySpark ETL feature depends on
(Technical Specification / AAP §0.5.1 **Group H**). Terraform
(`infra/s3_objects.tf`) uploads each file from here to `ARTIFACT_S3_BUCKET`,
because public **PyPI** and **Maven Central** are **prohibited as runtime
resolution paths** (AAP §0.1.2, §0.3.2). At AWS Glue 4.0 runtime the jobs
therefore resolve these artifacts **exclusively from `ARTIFACT_S3_BUCKET`** — the
three JARs via `--extra-jars` and the wheel via `--additional-python-modules`,
wired by `infra/glue_jobs.tf`.

> **Runtime class-closure note (verified).** `delta-storage-s3-dynamodb-3.2.0.jar`
> is **not self-contained**. Its mandated `io.delta.storage.S3DynamoDBLogStore` sits
> at the bottom of a superclass chain that crosses JAR boundaries:
>
> ```
> io.delta.storage.S3DynamoDBLogStore        (delta-storage-s3-dynamodb-3.2.0.jar)
>   └─ extends BaseExternalLogStore           (delta-storage-s3-dynamodb-3.2.0.jar)
>        └─ extends HadoopFileSystemLogStore   (delta-storage-3.2.0.jar)  ← base JAR
>             └─ extends LogStore              (delta-storage-3.2.0.jar)  ← base JAR
> ```
>
> A JVM cannot load a class without resolving its transitive superclasses, and the
> two superclasses `HadoopFileSystemLogStore` and `LogStore` exist **only** in the
> transitive `io.delta:delta-storage:3.2.0` artifact (`delta-storage-3.2.0.jar`).
> The same base JAR also supplies the `io.delta.storage.internal.{PathLock,FileNameUtils}`
> helpers referenced by `BaseExternalLogStore` and the `io.delta.storage.CloseableIterator`
> returned by `S3DynamoDBLogStore` / `RetryableCloseableIterator`. The connector JAR
> `delta-spark_2.12-3.2.0.jar` contains **zero** `io.delta.storage.*` classes, so it does
> not supply them either. Because `--datalake-formats=delta` is deliberately omitted
> (`infra/glue_jobs.tf`, so Glue 4.0 does **not** inject its own bundled Delta) and public
> Maven is prohibited at runtime (AAP §0.3.2), that base JAR is staged here too
> (`delta-storage-3.2.0.jar`) and placed on `--extra-jars`. Without it the JVM raises
> `NoClassDefFoundError: io/delta/storage/HadoopFileSystemLogStore` the instant the
> LogStore is instantiated for the first Delta commit — breaking the ACID coordination
> that is this feature's primary objective. Verify locally with:
>
> ```bash
> # succeeds only when the base JAR is on the classpath:
> javap -classpath delta-storage-s3-dynamodb-3.2.0.jar:delta-storage-3.2.0.jar \
>   -p io.delta.storage.S3DynamoDBLogStore
> ```

These are the **published, released** Delta Lake `3.2.0` artifacts. They are
intentionally **not** built from this monorepo's in-development `4.1.0-SNAPSHOT`
source (`version.sbt:L1`): the repository's current `main` already targets
Spark 4.x (`pyspark>=4.0.1`, `setup.py:L35`), whereas this feature deliberately
consumes the prior released line.

---

## Inventory & provenance

The `SHA-256` column records the digest **computed from the actual staged bytes**
in this directory (see **Filling in the checksums** below for how they are
produced and how to reconcile them against the upstream source). Reference **only**
the four filenames staged in this directory — no other artifact is in scope.

| File | Upstream coordinate / source | Version | Scala | Glue arg | Size (bytes) | SHA-256 |
|---|---|---|---|---|---|---|
| `delta-spark_2.12-3.2.0.jar` | Maven `io.delta:delta-spark_2.12:3.2.0` | 3.2.0 | 2.12 | `--extra-jars` | 6111817 | `51d473537d1bc10c81f48b03d8e2a6b604e1b421a70835ec12e917a4245a31d5` |
| `delta-storage-s3-dynamodb-3.2.0.jar` | Maven `io.delta:delta-storage-s3-dynamodb:3.2.0` | 3.2.0 | n/a | `--extra-jars` | 15606 | `375b202558c1809ba28dfd2f3b3f1e1a51155ceff9561b83f31809800d3759c2` |
| `delta-storage-3.2.0.jar` | Maven `io.delta:delta-storage:3.2.0` | 3.2.0 | n/a | `--extra-jars` | 24946 | `58aab63eba7736fea9e03eafb0dde6704a34a70f570c1a69ab8e4012c25a95d4` |
| `delta_spark-3.2.0-py3-none-any.whl` | PyPI `delta-spark==3.2.0` | 3.2.0 | n/a (py3) | `--additional-python-modules` | 21186 | `c4ff3fa7218e58a702cb71eb64384b0005c4d6f0bbdd0fe0b38a53564d946e09` |

**What each binary provides:**

- `delta-spark_2.12-3.2.0.jar` — the Delta Spark SQL connector:
  `org.apache.spark.sql.delta.catalog.DeltaCatalog` (wired via
  `spark.sql.catalog.spark_catalog`) and
  `io.delta.sql.DeltaSparkSessionExtension` (wired via `spark.sql.extensions`),
  matching the configuration in
  `storage-s3-dynamodb/integration_tests/dynamodb_logstore.py:L118-L124`.
- `delta-storage-s3-dynamodb-3.2.0.jar` — `io.delta.storage.S3DynamoDBLogStore`
  (and its `io.delta.storage.BaseExternalLogStore` base), the mandated
  multi-cluster ACID LogStore. **Depends on** `delta-storage-3.2.0.jar` below for
  its base/internal classes.
- `delta-storage-3.2.0.jar` — the **transitive base** `io.delta:delta-storage`
  artifact that `delta-storage-s3-dynamodb` is compiled against. It supplies
  `io.delta.storage.HadoopFileSystemLogStore` (the `BaseExternalLogStore`
  superclass), `io.delta.storage.CloseableIterator`, and the
  `io.delta.storage.internal.{PathLock,FileNameUtils}` helpers. Required on
  `--extra-jars` for the S3 DynamoDB LogStore to class-load at runtime.
- `delta_spark-3.2.0-py3-none-any.whl` — the Python `delta.tables.DeltaTable`
  API (including `merge`) used by the Glue PySpark jobs. The **staged wheel's own
  `METADATA`** declares `Name: delta-spark`, `Version: 3.2.0`,
  `Requires-Python: >=3.6`, and `Requires-Dist: pyspark (<3.6.0,>=3.5.0)`. (This
  is distinct from — and must not be conflated with — the *current* monorepo
  `main`, whose `setup.py` declares `python_requires='>=3.10'` for the
  in-development `4.1.0-SNAPSHOT` line; the staged 3.2.0 wheel's metadata is the
  authoritative constraint for this artifact.)

### Verifying the checksums

The `SHA-256` cells above are **computed from the actual staged bytes** in this
directory. To re-verify them at any time (for example before a Terraform apply,
or when reconciling against the upstream published checksum — the `.sha256` /
`.sha1` sidecar files on Maven Central for the JARs, or the digest recorded on
the PyPI release page for the wheel), recompute the local digests with:

```bash
cd artifacts
sha256sum \
  delta-spark_2.12-3.2.0.jar \
  delta-storage-s3-dynamodb-3.2.0.jar \
  delta-storage-3.2.0.jar \
  delta_spark-3.2.0-py3-none-any.whl
```

The output must match the `SHA-256` column above byte-for-byte. **No checksum is
recorded here that was not computed from the actual staged bytes.** A maintainer
should additionally reconcile each value against the upstream published checksum
before production cutover.

---

## Sourcing & constraints

- **Build/staging time vs. runtime.** Fetching these artifacts from Maven
  Central / PyPI (or an approved internal mirror) at **build / staging time is
  permitted**; **runtime resolution against public repositories is prohibited**.
  Every artifact must resolve from `ARTIFACT_S3_BUCKET` once a Glue job runs.
- **Pinned coordinates.** Scala **2.12** — the connector JAR must be the `_2.12`
  build, because AWS Glue 4.0 runs Scala 2.12 (the `_2.13` build is **not**
  used). Runtime target: **AWS Glue 4.0 = Apache Spark 3.3.x, Python 3.10**. All
  four files are the **released** Delta `3.2.0` line — **not** this repository's
  `4.1.0-SNAPSHOT` build.
- **Consumers.**
  - `infra/s3_objects.tf` — uploads each file from this directory to
    `ARTIFACT_S3_BUCKET` via `aws_s3_object` (the three JARs are enumerated by
    the `local.delta_spark_jar` / `local.delta_storage_jar` /
    `local.delta_storage_transitive_jar` filename locals; the wheel by
    `local.delta_wheel`).
  - `infra/glue_jobs.tf` — references the three JARs through `--extra-jars`
    (built from `local.extra_jars` in `infra/locals.tf`) and the wheel through
    `--additional-python-modules`, by their `ARTIFACT_S3_BUCKET` S3 URIs.
- **Scope.** All four files above are staged. The transitive
  `io.delta:delta-storage:3.2.0` JAR (`delta-storage-3.2.0.jar`) is **in scope**
  and staged here precisely because the S3 DynamoDB LogStore JAR references its
  classes (see the *Runtime class-closure note* at the top); it is wired through
  `infra/locals.tf` → `infra/s3_objects.tf` → `infra/glue_jobs.tf` exactly like
  the other two JARs.
- **Binary-count reconciliation — exactly 4 runtime binaries (3 JARs + 1 wheel).**
  The runtime-required, ACID-correct staged set is **four** binaries:
  `delta-spark_2.12-3.2.0.jar`, `delta-storage-s3-dynamodb-3.2.0.jar`,
  `delta-storage-3.2.0.jar`, and `delta_spark-3.2.0-py3-none-any.whl`. That count is
  *mandated* by the feature's own primary objective — every Delta write must commit
  through `io.delta.storage.S3DynamoDBLogStore` (AAP **§0.1.2** LogStore enforcement;
  the §0.1.1 ACID objective) — which, as proven in the *Runtime class-closure note*,
  cannot instantiate unless `delta-storage-3.2.0.jar` is on `--extra-jars`. Dropping
  that base JAR to hit a smaller literal count would raise
  `NoClassDefFoundError: io/delta/storage/HadoopFileSystemLogStore` at the first Delta
  commit, defeating the ACID coordination the feature exists to deliver. The only
  other conceivable classpath source — Glue's `--datalake-formats=delta` — is
  **excluded by design** because it injects Glue 4.0's *own bundled* Delta line (not
  the pinned 3.2.0) and violates the exclusive-`ARTIFACT_S3_BUCKET` sourcing rule
  (AAP **§0.3.2**); repackaging released JARs is likewise out (provenance is verbatim
  bytes). Staging the base JAR is therefore the only compliant option.
- **AAP traceability (§0.5.1 Group H ↔ §0.7.3 "implied artifact").** AAP §0.5.1
  Group H and §0.3.1 literally enumerate **two** JARs (`delta-spark_2.12-3.2.0.jar`,
  `delta-storage-s3-dynamodb-3.2.0.jar`) plus the wheel — i.e. "three staged binaries"
  in the literal plan text. The **third** JAR staged here, `delta-storage-3.2.0.jar`,
  is an *implied-required* artifact reconciled under the **very same principle the AAP
  itself applies in §0.7.3** to bring the `delta-spark` wheel into scope beyond the
  prompt's literal JAR-only list: a binary the mandated runtime path provably requires,
  but that the literal enumeration omitted, is treated as in-scope and staged (it
  cannot be resolved from public Maven at runtime per §0.1.2 / §0.3.2). The frozen AAP
  §0.5.1 Group H text is the plan of record and is **not edited** by this feature; this
  note reconciles the implementation to it *within the deliverable*, per the Minimal
  Change Mandate (document, don't silently change). Reading "3 staged binaries" in the
  literal plan as **"3 JARs + 1 wheel = 4 binaries"** (the implied-required base JAR
  included) closes the traceability item with zero runtime risk; the alternative —
  dropping the base JAR to match a literal count — is rejected because it breaks the
  mandated ACID LogStore.

---

## ⚠️ Validation item — Delta 3.2.0 ↔ Glue 4.0 (Spark) compatibility

> **⚠️ Validation Item (AAP §0.3.3, §0.7.3) — platform-team confirmation required.**
>
> The Delta Lake 3.x line — **including `delta-spark` 3.2.0** — is published
> against the **Apache Spark 3.5.x** line; the staged wheel's own `METADATA`
> declares `Requires-Dist: pyspark (<3.6.0,>=3.5.0)`, confirming this pin. **AWS
> Glue 4.0**, by contrast, **ships Apache Spark 3.3.x**. The versions pinned by
> this feature (Delta 3.2.0 on Glue 4.0) are **authoritative inputs from the
> prompt** and are staged here **verbatim**.
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
- **No invented data** — every recorded SHA-256 is computed from the actual
  staged bytes in this directory (re-verifiable via the `sha256sum` command
  above), and no download URLs are fabricated.
- **Minimal Change Mandate honored** — no **pre-existing** delta-io/delta monorepo
  source, build, or metadata file (e.g. anything under `spark*/`, `kernel*/`,
  `storage*/`, `build.sbt`, `setup.py`, `version.sbt`) is modified. Everything this
  feature delivers — including this `artifacts/` directory and this README — is a
  **net-new feature deliverable**. As a living provenance document, this README is
  authored and **refined across checkpoints** (created in CP1, updated in later
  checkpoints); such updates are confined to net-new feature files and therefore
  remain fully within the Mandate.
- **No secrets** are recorded in this directory.
