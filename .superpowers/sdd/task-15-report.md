# Task 15 Report — Measurement runbook + results document

## Measurements actually run (all Gradle, this session)

| Scenario | Real number | Source |
|----------|-------------|--------|
| Selective recompile (content change, 1 file) | ~20–24 s (1 task executed, 6 UP-TO-DATE) | `time ./gradlew :graylog2-server:compileJava --build-cache` after appending a comment to `Configuration.java` |
| No-op compile (file restored) | ~0.6 s (7 UP-TO-DATE) | second run of same command |
| Single IT cold (`CollectorTLSUtilsIT`, test classes from cache) | ~19 s (22 actionable: 4 executed, 1 from-cache, 17 UP-TO-DATE) | `time ./gradlew :graylog2-server:integrationTest --tests "org.graylog.collectors.CollectorTLSUtilsIT"` |
| Single IT warm (UP-TO-DATE) | ~0.6 s | second run of same command |
| Cross-run cache: `clean` then `compileJava` | ~2.9 s (4 FROM-CACHE: compileJava, generateProto, generateGrammarSource, generateOpampProto) | `./gradlew clean && time ./gradlew :graylog2-server:compileJava --build-cache` |
| Cross-run cache: `clean` then `jar` | ~11 s (4 FROM-CACHE, 5 UP-TO-DATE, 6 executed) | same with `:graylog2-server:jar` |
| Frontend skip: `compileJava` after Java change | ~20 s (no yarn/webpack tasks) | task list verified: no yarnBuild in output |
| Frontend warm: `jar` second run | ~0.7 s (yarnBuild UP-TO-DATE) | `time ./gradlew :graylog2-server:jar` second run |
| Frontend cold: `jar` first run after clean | ~40 s (yarnBuild ~38 s) | seen during scenario 4 jar build |
| `check --dry-run` (no integrationTest) | confirmed: `integrationTest` absent | `./gradlew :graylog2-server:check --dry-run` output inspected |

## Maven baselines

**Not measured in this session.** Maven `test-compile` and `verify` cycles are 5–15 min on this machine and were deferred. Results doc marks these as "not measured in this session — run via measure.sh" (no fabricated numbers).

## Files created

- `scripts/gradle-prototype/measure.sh` (executable, 295 lines, syntax-checked with `bash -n`, run end-to-end with `--skip-maven`)
- `docs/superpowers/specs/2026-06-28-gradle-prototype-results.md` (120 lines, real numbers only)

## Self-review

- No fabricated numbers: all Gradle timings were measured with `time` in this session; Maven entries are explicitly labeled "not measured in this session".
- Tree clean: `git status` shows `nothing to commit, working tree clean` after commit.
- No Maven files modified: confirmed — only new files added (`scripts/` and `docs/superpowers/specs/`).
- Touched file reverted: `Configuration.java` was restored via `git checkout` after each measurement; verified clean tree before and after commit.

## Commit

SHA: `4acaac047d`  
Subject: `docs(gradle): measurement runbook and prototype results`

## Concerns

1. `sourcesJar` task fails with an implicit task-dependency deprecation warning (OpAMP proto sources). Documented in results doc under Divergences §5. Not a blocker.
2. Maven baselines remain unmeasured. The `measure.sh` script is ready to collect them; one run without `--skip-maven` gives the full comparison table.
3. The `measure.sh` cache-count capture uses a subshell for FROM-CACHE counting — not perfectly atomic, but reliable enough for a runbook.

## Fix: final review findings

Six fixes applied to the `gradle-build-prototype` branch. No Maven files (`pom.xml`, `pom.xml-tmpl`, `.mvn/`, `target/`) were modified.

### Fix 1 — .gitignore glob too broad

**Change:** Replaced `**/build/` with eight anchored entries (`/build/`, `/buildSrc/build/`, `/graylog2-server/build/`, `/graylog-storage-elasticsearch7/build/`, `/graylog-storage-opensearch2/build/`, `/graylog-storage-opensearch3/build/`, `/full-backend-tests/build/`, `/graylog2-web-interface/build/`). The broad glob was silently ignoring Java source files in packages named `build` (e.g., `data-node/src/main/java/org/graylog/datanode/build/`).

**Verification:**
- `git check-ignore data-node/src/main/java/org/graylog/datanode/build/InstallOpensearchPlugins.java` → exit code 1 (NOT ignored) ✓
- `git check-ignore graylog2-server/build/` → `graylog2-server/build/` (still ignored) ✓

### Fix 2 — Harden javac fork-args

**Change:** Replaced `options.forkOptions.jvmArgs?.addAll(compilerJvmArgs)` with `options.forkOptions.jvmArgs = (options.forkOptions.jvmArgs ?: emptyList()) + compilerJvmArgs`. The null-safe call silently dropped all `--add-exports`/`--add-opens` flags when `jvmArgs` was initially null.

**Verification:**
- `./gradlew :graylog2-server:compileJava` → BUILD SUCCESSFUL in 11s ✓
- `./gradlew :graylog2-server:compileJava --info --rerun-tasks 2>&1 | grep -c add-exports` → 2 (> 0) ✓

### Fix 3 — Correct ErrorProne characterization in results doc

**Change:** Rewrote the ErrorProne divergence section in `docs/superpowers/specs/2026-06-28-gradle-prototype-results.md`. The old wording incorrectly implied Maven only partially ran ErrorProne. Corrected to: Maven runs ErrorProne as a real javac plugin via `-Xplugin:ErrorProne` (parent `pom.xml` ~line 1153); the Gradle prototype intentionally leaves it INERT (processor path only, no `-Xplugin:ErrorProne`); the dangling `annotationProcessor(errorprone-core)` does nothing at compile time. Framed as an intentional, in-scope prototype simplification.

**Verification:** Document edit — no build command needed. ✓

### Fix 4 — Remove withSourcesJar() deprecation

**Change:** Removed `withSourcesJar()` call from `buildSrc/src/main/kotlin/graylog.java-conventions.gradle.kts`. It caused an implicit task-dependency deprecation warning on Gradle 9.x (sourcesJar scanning OpAMP proto source dirs before generation). The prototype does not publish sources jars.

**Verification:**
- `./gradlew :graylog2-server:compileJava 2>&1 | grep -i sourcesJar` → no output (exit 1) ✓

### Fix 5 — Add opt-in heavy-test task

**Change:** Added `tasks.register<Test>("fullBackendTest") { ... }` at the end of `full-backend-tests/build.gradle.kts`. The task is in the `verification` group, uses `includeTags("full-backend-test")`, and is NOT wired into `check`.

**Verification:**
- `./gradlew :full-backend-tests:tasks --group verification 2>&1 | grep fullBackendTest` → `fullBackendTest - Runs the heavy @FullBackendTest integration tests (requires Docker).` ✓

### Fix 6 — Guard server unit test against heavy tags

**Change:** Added `useJUnitPlatform { excludeTags("full-backend-test") }` inside `testTask.configure { ... }` in `graylog2-server/build.gradle.kts`. Prevents `./gradlew :graylog2-server:test` from accidentally picking up `@FullBackendTest`-annotated tests that require Docker/Testcontainers.

**Verification:**
- `./gradlew :graylog2-server:test --tests "org.graylog.grn.GRNTest"` → BUILD SUCCESSFUL in 3s ✓

---

## Fix: Task 15 review findings

**Commit:** `6be4d1a870` ("fix(gradle): tidy measure.sh and clarify cache count in results")

Four cleanup fixes applied:

1. **Remove dead CACHE_COUNT block** (lines 237–240, measure.sh): The `CACHE_COUNT=$(...)` assignment ran an extra `gradlew clean` + `gradlew :graylog2-server:compileJava` cycle but `$CACHE_COUNT` was never referenced. Removed entire block; verified with `grep -n CACHE_COUNT` → no output.

2. **Clarify Signal-4 FROM-CACHE count** (results.md, line 30): Simplified `(4 FROM-CACHE, 3 extracted)` to `(4 FROM-CACHE)`; "3 extracted" was unexplained and conflicted with the count. Timing `~2.9 s` unchanged.

3. **Collapse duplicate if/else in Scenario 5** (measure.sh, lines 276–282): Both branches of the `if [ "$SKIP_MAVEN" = "false" ]` conditional held identical `table_row` calls. Replaced with single unconditional call.

4. **Clean up RESULTS_TMP on abnormal exit** (cleanup() trap, measure.sh): Added `rm -f "${RESULTS_TMP:-}"` inside the EXIT trap to prevent temp file leakage on early exit.

**Verification:** `bash -n scripts/gradle-prototype/measure.sh` → no syntax errors; `git status --short` → only the two edited files modified.
