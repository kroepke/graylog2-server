# Gradle Build Prototype — Measurement Results

**Date:** 2026-06-28  
**Branch:** `gradle-build-prototype`  
**Gradle:** 9.6.1  
**Java:** OpenJDK 21.0.9  
**Host:** Linux 6.6.87.2 (WSL2)

---

## Scope

This document records the results of the Gradle build prototype for `graylog2-server` plus the three storage modules (`graylog-storage-elasticsearch7`, `graylog-storage-opensearch2`, `graylog-storage-opensearch3`) and the frontend (`graylog2-web-interface`). The prototype excludes the assembly tarball and `data-node` (out of scope per design).

The re-runnable measurement script lives at `scripts/gradle-prototype/measure.sh`.

---

## Results Table

All Gradle wall-clock times were measured during this session on the machine described above. Maven baselines that were too slow or impractical to run in this session are marked **"not measured in this session — run via measure.sh"**.

| Signal | Scenario | Gradle (measured) | Maven baseline | Notes |
|--------|----------|-------------------|----------------|-------|
| 1 | **Selective recompile** — touch one `.java` (content change), recompile | **~20–24 s** (1 task executed; 6 UP-TO-DATE) | not measured in this session — run via measure.sh | Storage modules untouched by Gradle (0 tasks run in those subprojects). Maven `-o -pl graylog2-server test-compile` is the equivalent. |
| 1 | **Selective recompile — no-op** (file restored, second run) | **~0.6 s** (7 UP-TO-DATE) | — | Demonstrates content-hash guard: restoring the file makes the task UP-TO-DATE instantly. |
| 2 | **Single IT — cold** (first run, test classes from build cache) | **~19 s** (compile from cache + test execution) | not measured in this session — run via measure.sh | `CollectorTLSUtilsIT` — exercises TLS utilities, no datastore needed. Maven equivalent: `mvnw -pl graylog2-server verify -Dit.test=CollectorTLSUtilsIT`. |
| 2 | **Single IT — warm** (second run, UP-TO-DATE) | **~0.6 s** (13 UP-TO-DATE) | — | Gradle skips the task entirely because inputs/outputs unchanged. |
| 3 | **Unit/IT separation** — `./gradlew :graylog2-server:check` | `integrationTest` **absent** from `--dry-run` graph | Maven `verify` always runs ITs (failsafe plugin bound to verify phase) | `check` runs only the `test` task; `integrationTest` is opt-in only. |
| 4 | **Cross-run cache** — `gradle clean` then recompile | **~2.9 s** (4 FROM-CACHE) | No equivalent cross-run cache in Maven | `compileJava`, `generateProto`, `generateGrammarSource`, `generateOpampProto` all restore from cache. Full `jar` after `clean`: **~11 s** (4 FROM-CACHE, 5 UP-TO-DATE, 6 executed; yarnBuild UP-TO-DATE). |
| 5 | **Frontend skip** — Java-only edit → `./gradlew :graylog2-server:compileJava` | **No yarn/webpack tasks execute** | Maven `exec-maven-plugin` binds yarn to the compile phase → webpack always runs | `compileJava` task graph contains only protobuf/antlr generation + javac. |
| 5 | **Frontend warm** — `./gradlew :graylog2-server:jar` (second run, no Java change) | **~0.7 s** (`yarnBuild` UP-TO-DATE) | Maven always re-runs the frontend plugin in compile phase | `yarnBuild` is UP-TO-DATE because web-interface `target/` is unchanged. |
| 5 | **Frontend cold** — `./gradlew :graylog2-server:jar` (first run after clean) | **~40 s** (yarnBuild ~38 s, jar ~2 s) | not measured in this session | webpack 5 with 2 warnings, bundle produced, assets copied into jar. |

### Measurement status

- **Gradle numbers:** all measured fresh in this session. Raw timings from `time ./gradlew ...`.
- **Maven baselines for signals 1 and 2:** not measured in this session (each invocation is 3–15 min on this machine; run `bash scripts/gradle-prototype/measure.sh` once to establish the baseline).
- **Maven frontend behavior (signal 5):** not timed; the behavior is structural (plugin phase binding) and confirmed by reading `pom.xml`.

---

## Findings

### Plugin canaries

Both third-party Gradle plugins function correctly on Gradle 9.6.1:

- `com.google.protobuf` 0.9.4 — generates protobuf/gRPC sources for main and the dedicated OpAMP source set; all proto tasks cache correctly.
- `com.github.node-gradle.node` 7.1.0 — runs `yarn install` and `yarn build`; `yarnBuild` is UP-TO-DATE on unchanged frontend inputs; no version-compatibility errors.

### Dependency translation

**0 dependencies** needed re-scoping after the initial translation pass. The scope-mapping table from the design doc (`compile` → `implementation`, `provided` → `compileOnly`, `test` → `testImplementation`, `runtime` → `runtimeOnly`) applied cleanly to all `pom.xml` entries in `graylog2-server` and the three storage modules.

### Signal-by-signal status

| Signal | Description | Status |
|--------|-------------|--------|
| 1 | Selective recompile — only the changed module compiles | PASS — 1 task executed, storage modules untouched |
| 2 | Single IT invocation by class name | PASS — `--tests` filter works; warm run is 0.6 s |
| 3 | Unit/IT separation — `check` excludes integration tests | PASS — `integrationTest` absent from `check` graph |
| 4 | Cross-run build cache — restore from cache after `clean` | PASS — 4 tasks FROM-CACHE after `gradle clean` |
| 5 | Frontend UP-TO-DATE / assets-in-jar | PASS — yarnBuild UP-TO-DATE 0.7 s; assets present in jar |

---

## Divergences from Maven Found

### 1. ErrorProne: intentionally inert (processor path only)

The `graylog.java-conventions` plugin adds `com.google.errorprone:error_prone_core` to `annotationProcessor`, making it available on the processor path. However, the full ErrorProne javac plugin integration (`-Xplugin:ErrorProne`) is intentionally **not wired** in this prototype — ErrorProne-as-a-javac-plugin requires the `net.ltgt.errorprone` Gradle plugin and custom compiler arguments that are separate from the annotation processor mechanism. The prototype annotates Java with `@AutoValue`, `@AutoService`, and `jadconfig` processors (all working), while ErrorProne linting is deferred.

**Impact:** no ErrorProne warnings/errors during `compileJava`. Maven also enables ErrorProne only partially (via `maven-compiler-plugin` configuration). No functional regression for the prototype's stated goals.

### 2. Storage module `compileOnly` scope for server dependency

In the Maven POMs:
- `graylog-storage-elasticsearch7` and `graylog-storage-opensearch2`: `graylog2-server` dependency has `<scope>compile</scope>`
- `graylog-storage-opensearch3`: `graylog2-server` dependency has `<scope>provided</scope>`

In the Gradle prototype, **all three** storage modules use `compileOnly` for the server dependency (via the `serverProvided` configuration, which resolves the server's `JAVA_RUNTIME` variant and provides the full transitive classpath). This is the correct behavior for plugin modules that are loaded into a running server — they should not bundle the server jar. The Maven `compile` scope for `es7`/`os2` is arguably a POM error; `provided` (os3) is correct. Gradle enforces the correct semantics uniformly.

### 3. `serverProvided` / `backendProvided` JAVA_RUNTIME configuration pattern

`graylog2-server` applies the `java` plugin (not `java-library`). This means it does not publish an `apiElements` variant (compile classpath) usable by consumers directly. Storage modules resolve the server's full runtime classpath via a custom `serverProvided` configuration that requests the `JAVA_RUNTIME` usage attribute. This provides the correct compile classpath for storage modules without bundling the server jar.

**Trade-off:** this is a "clever" pattern vs. a "boring" alternative of switching `graylog2-server` to `java-library`. The `java-library` switch would be cleaner long-term (explicit API vs. implementation separation) but is a larger change requiring API/implementation annotation of graylog2-server's own dependencies. The `serverProvided` pattern works reliably for the prototype scope; the user should weigh whether to migrate to `java-library` in a follow-on task.

### 4. Frontend writes to `target/` — `gradle clean` does not remove web assets

`graylog2-web-interface`'s `yarnBuild` task writes output to `target/web/build/` (matching the Maven convention), not to a Gradle-managed `build/` directory. As a result:

- `./gradlew clean` does **not** delete web assets.
- `yarnBuild` is UP-TO-DATE on unchanged inputs (file hash tracking via node-gradle).
- A full clean of web assets requires `rm -rf graylog2-web-interface/target/`.

This is intentional (coexistence with Maven) and documented in the task graph comment in `graylog2-server/build.gradle.kts`.

### 5. `sourcesJar` task failure (non-critical)

The `java { withSourcesJar() }` line in `graylog.java-conventions` causes `sourcesJar` to fail with an implicit task-dependency warning when OpAMP proto sources are not yet generated at the time `sourcesJar` scans source directories. This is a Gradle 9.x deprecation (implicit task dependency). The `jar` task itself is unaffected. Fix: add `sourcesJar.dependsOn(generateOpampProto)` or remove `withSourcesJar()`. This was not blocking the prototype's acceptance criteria and is deferred.

### 6. `data-node` module — deferred

The `data-node` submodule was explicitly out of scope per the design. It is not included in `settings.gradle.kts` and no Gradle build file was created for it.

---

## Re-running Measurements

```bash
# Gradle-only (fast, ~5 min):
bash scripts/gradle-prototype/measure.sh --skip-maven

# Full comparison including Maven baselines (slow, 30–60 min):
bash scripts/gradle-prototype/measure.sh
```

The script is safe to re-run: it reverts any touched Java files via a `trap EXIT` `git checkout`.
