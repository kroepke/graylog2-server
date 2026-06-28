# Gradle build prototype for `graylog2-server` — Design

**Date:** 2026-06-28
**Status:** Approved (design); implementation plan to follow
**Scope owner:** Kay Roepke

## Motivation

The current Graylog build is a generated Maven meta-build: a Go CLI
(`graylog-project-cli`) renders `pom.xml` from `pom.xml-tmpl` using JSON
manifests, clones the sibling repos, and runs one large Maven reactor that ties
`graylog2-server`, `graylog-plugin-enterprise`, and the Yarn/Webpack frontend
together.

The concrete pain that motivates this prototype: **the integration-test loop
recompiles everything and runs nearly every test, taking hours.** This is
classic Maven reactor behaviour — `mvn verify` walks every module, Failsafe runs
every `*IT`, and there is no cross-invocation build cache (the root POM even sets
`maven.compiler.useIncrementalCompilation=false`).

Goals, in priority order:

1. A verification loop that is fast and selective — editing one file or running
   one test does not rebuild and re-test the world. This directly improves both
   developer inner-loop time and AI-agent verification loops.
2. Less brittle than the generated-POM + 4-deep-parent-POM setup.
3. Better compile times via incrementality and a build cache.

Explicit non-goal: cleverness. The build should be boring, explicit, and
maintainable. No exotic Gradle features where a plain one will do.

## Scope

**In scope** — a Gradle build covering the entire `graylog2-server` repo
*minus the final assembly tarball*:

- `graylog2-server` (the main module)
- `graylog-storage-elasticsearch7`, `graylog-storage-opensearch2`,
  `graylog-storage-opensearch3`
- `full-backend-tests`
- the `graylog2-web-interface` frontend (Yarn/Webpack), built and bundled into
  the server jar

**Out of scope (this prototype):**

- The assembly tarball and the `distribution` module
- The manifest / `graylog-project-cli` POM generation
- The `graylog-plugin-enterprise` repo
- A multi-repo Gradle composite build (`includeBuild`)
- `maven-shade` fat-jar (we run from the runtime classpath, like the existing
  `runner` module)
- `.deb` / `.rpm` packaging, SBOM generation, license-header enforcement,
  Chromium binary downloads
- `data-node` — tracked as a separate concern, deferred

## Approach

**Standalone, hand-authored Gradle build rooted in the `graylog2-server` repo,
coexisting with Maven**, using the **Kotlin DSL**.

Chosen over (a) a meta-project composite build from day one — more moving parts
to debug before the basics are proven — and (b) automated POM conversion — the
generated/complex POMs convert into tangled output that fights exactly the
codegen/frontend/test parts that carry the value.

The Gradle files are added *alongside* the POMs; nothing is deleted. `mvn` and
`gradle` both work on the same checkout, so every performance and behaviour claim
is A/B-comparable.

## Architecture

### 1. Repository layout

```
graylog2-server/                      (repo root = Gradle root project)
├─ settings.gradle.kts               # module graph, repositories, build cache
├─ build.gradle.kts                  # root: version-catalog plumbing only
├─ gradle.properties                 # caching, parallel, configuration cache
├─ gradle/libs.versions.toml         # version catalog (mirrors graylog-parent)
├─ buildSrc/                         # convention plugins (parent-POM replacement)
│   └─ src/main/kotlin/graylog.*.gradle.kts
├─ pom.xml                           # UNTOUCHED — Maven still works
├─ graylog2-server/build.gradle.kts
├─ graylog-storage-elasticsearch7/build.gradle.kts
├─ graylog-storage-opensearch2/build.gradle.kts
├─ graylog-storage-opensearch3/build.gradle.kts
├─ full-backend-tests/build.gradle.kts
└─ graylog2-web-interface/build.gradle.kts
```

Gradle writes to `build/` per project; Maven keeps `target/`. `.gitignore` is
extended for Gradle outputs so the two never collide.

### 2. Module → Gradle project mapping (`settings.gradle.kts`)

One Gradle subproject per existing Maven module. Inter-module edges mirror the
POMs:

- Storage plugins depend on `:graylog2-server` as **`compileOnly`** (the Gradle
  analog of Maven `provided` — they are loaded as plugins at runtime, not bundled).
- `full-backend-tests` depends on `:graylog2-server` and its test-fixtures.
- The `distribution` module is **not** registered (assembly out of scope).

### 3. Convention plugins (`buildSrc`) — parent-POM replacement

The 4-deep parent-POM chain
(`graylog-parent → graylog-project-parent → graylog-plugin-parent →
graylog-plugin-web-parent`) collapses into a small set of convention plugins.
This is the boring, idiomatic Gradle replacement for parent POMs: shared config
is applied code, not inherited XML, and it is explicit — each module declares
`plugins { id("graylog.java-conventions") }`, so there is no "where did this
setting come from" mystery.

- **`graylog.java-conventions`** — Java 21 toolchain, `-parameters`, UTF-8
  encoding, common annotation processors (AutoValue, AutoService, JadConfig,
  ErrorProne), the BOM `platform()` imports (so every module shares pinned
  versions), and the unit/IT test-suite wiring (§7).
- **`graylog.storage-plugin-conventions`** — applies java-conventions, adds
  `compileOnly(project(":graylog2-server"))`, and the plugin-jar manifest
  entries.

### 4. Dependency & version management

A Gradle **version catalog** (`gradle/libs.versions.toml`) holds the direct
dependency versions, seeded from `graylog-parent`. The **same BOMs** the POMs
import — log4j, netty, jackson, grpc, junit, aws-sdk — are imported via
`platform()` inside `graylog.java-conventions`, so transitive versions resolve
to the same numbers Maven uses.

During the prototype the catalog is hand-maintained. Keeping it in sync with
`graylog-parent` is a known, accepted cost (see Risks); a later iteration could
generate the catalog from the POM.

### 5. Code generation

Each codegen step becomes a real Gradle task with declared inputs and outputs,
so Gradle skips it when nothing changed (the property Maven's `exec`-based steps
lack):

| Codegen | Gradle mechanism |
|---|---|
| protobuf + gRPC | `com.google.protobuf` plugin, pinned `protoc` + `protoc-gen-grpc-java` |
| ANTLR4 | Gradle built-in `antlr` plugin |
| AutoValue / AutoService / JadConfig / ErrorProne | native `annotationProcessor` configuration |
| Swagger → OpenAPI → TS API types | `JavaExec` task running `GenerateApiDefinition`; inputs = compiled REST classes, output = `build/swagger` |

The `--add-exports` / `--add-opens` flags currently in `.mvn/jvm.config` (needed
by Graylog's annotation processors to reach internal `javac` APIs) are
replicated for Gradle's compiler and test JVMs.

### 6. Frontend integration

The **`com.github.node-gradle.node`** plugin (the direct analog of
`frontend-maven-plugin`) pins Node 24.13.0 / Yarn 1.22.22 and downloads from the
existing Graylog CI cache. Tasks, in order:
`yarnInstall` → `generateApiDefs` → `yarnBuild` → `yarnTest`.

The key improvement over the Maven plugin is real up-to-date checking:
`yarnBuild` declares its inputs (`src/**`, `package.json`, `yarn.lock`, the
generated API defs) and its output (`target/web/build`). A Java-only change skips
the entire multi-minute Webpack build. The built assets are wired into
`:graylog2-server`'s `processResources` so they land at `web-interface/assets`
in the jar, exactly as today. `yarnTest` is its own task, decoupled from the Java
test tasks.

### 7. Test architecture (centerpiece)

This is what kills the "recompiles everything, runs every test for hours"
problem.

- **JVM Test Suite plugin** gives each module two suites: `test` (unit:
  `*Test`, `*Spec`) and `integrationTest` (`*IT`, `*IntegrationTest`). Separate
  source sets, tasks, and classpaths.
- `check` depends on `test` only. `integrationTest` is **opt-in**. The heavy
  `full-backend-test`-tagged suites live in `:full-backend-tests` and never run
  unless explicitly requested.
- **`test-jar` → `java-test-fixtures`**: shared test base classes become a
  first-class `testFixtures` artifact other modules consume, removing the
  fragile re-declaration of transitive test dependencies Maven's `test-jar`
  requires.
- **Selective execution** falls out for free:
  `gradle :graylog2-server:integrationTest --tests "*SomeIT"` compiles only
  changed modules (incremental) and runs only that test.
- Mockito's Java-21 inline-mock javaagent is wired per suite (the Gradle
  equivalent of the current Surefire/Failsafe `argLine`).

### 8. Build cache & performance

`gradle.properties` enables `org.gradle.caching=true`,
`org.gradle.parallel=true`, and the **configuration cache**. A **local** build
cache is used for the prototype (a remote/CI cache is noted as a later step).
Combined with the per-task input/output declarations above, an unchanged module
recompiles and re-tests zero times across runs.

## Acceptance criteria & measurement runbook

The prototype is judged against the four success signals, each measured against
the Maven baseline on the same machine and committed as a results document
(`docs/superpowers/specs/2026-06-28-gradle-prototype-results.md`). The runbook is
a deliverable, not an afterthought.

1. **Selective recompile** — touch one `.java` in `graylog2-server`; time the
   rebuild. Assert the storage modules do **not** recompile. (Maven baseline:
   same edit, time `mvn -o test-compile`.)
2. **Single-test run** — run one `*IT` by name. Assert no full recompile and no
   unrelated tests run. (Maven baseline: the equivalent single-IT invocation.)
3. **Unit/IT separation** — `gradle check` runs **zero** integration tests;
   `gradle integrationTest` runs them on demand.
4. **Cross-run cache** — a second no-op build reports every task
   `UP-TO-DATE` / `FROM-CACHE`.
5. **Frontend** — the `graylog2-server` jar contains the web assets at
   `web-interface/assets`; a Java-only edit skips the Webpack build.

Each measurement records wall-clock time for both Maven and Gradle so the
comparison is concrete.

## Risks & open questions

- **Version-catalog drift** vs `graylog-parent` during coexistence. Accepted for
  the prototype; could later be generated from the POM.
- **Annotation-processor JVM flags** — the `.mvn/jvm.config` `--add-exports` /
  `--add-opens` set must be replicated for Gradle's compiler and test JVMs, or
  annotation processing fails.
- **Resolution differences** between Gradle and Maven (e.g. `provided` /
  optional / dependency-mediation rules) may surface a handful of dependency
  mismatches to reconcile. This is expected, and flushing them out is part of
  the prototype's value.
- **Codegen ordering** — the Swagger→TS step depends on compiled server classes;
  the task graph must encode that the frontend's `generateApiDefs` runs after the
  relevant Java compilation.

## Out-of-scope follow-ons (future specs)

- `data-node` module.
- `graylog-plugin-enterprise` as a second Gradle build joined via `includeBuild`
  (the boring, well-trodden multi-repo path — adopted once the basics are proven).
- Assembly tarball / `distribution`.
- Replacing or feeding the manifest / `graylog-project-cli` generation.
- Remote build cache for CI.
