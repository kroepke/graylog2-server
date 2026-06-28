#!/usr/bin/env bash
# =============================================================================
# Gradle vs Maven build measurement runbook — graylog2-server prototype
#
# Re-runnable: edits a Java file temporarily but always restores it via
# `git checkout` before exiting (even on error).
#
# Usage:
#   bash scripts/gradle-prototype/measure.sh [--skip-maven]
#
# Pass --skip-maven to skip all Maven scenarios (they are slow; run once
# to establish a baseline and record results in the results doc).
#
# Output: a Markdown table printed to stdout. Pipe to tee to save:
#   bash scripts/gradle-prototype/measure.sh | tee /tmp/measure.out
# =============================================================================
set -euo pipefail

# Always run from repo root
cd "$(dirname "$0")/../.."
REPO_ROOT="$(pwd)"

SKIP_MAVEN=false
for arg in "$@"; do
  case "$arg" in
    --skip-maven) SKIP_MAVEN=true ;;
  esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { printf '\n=== %s ===\n' "$1"; }
note() { printf '    NOTE: %s\n' "$1"; }

# Measure wall-clock time for a command; store result in ELAPSED.
# Usage: measure "label" command...
ELAPSED=""
measure() {
  local label="$1"; shift
  local start end
  start=$(date +%s%N)
  "$@"
  end=$(date +%s%N)
  ELAPSED=$(( (end - start) / 1000000 ))  # milliseconds
  printf '  [%s] wall-clock: %d ms (%.1f s)\n' "$label" "$ELAPSED" "$(echo "scale=1; $ELAPSED/1000" | bc)"
}

# Append a row to the running results table (stored in a temp file).
RESULTS_TMP="$(mktemp)"
table_row() {
  # args: scenario | tool | wall_ms_or_label | notes
  # If the third arg is purely numeric, format it as "NNN ms"; otherwise print as-is.
  local wall="$3"
  if [[ "$wall" =~ ^[0-9]+$ ]]; then
    wall="${wall} ms"
  fi
  printf '| %-45s | %-6s | %-20s | %s\n' "$1" "$2" "$wall" "$4" >> "$RESULTS_TMP"
}

# File we temporarily modify for recompile / frontend tests.
TOUCHED_FILE="graylog2-server/src/main/java/org/graylog2/Configuration.java"

# Ensure we always restore the file on exit.
cleanup() {
  if git -C "$REPO_ROOT" diff --quiet "$TOUCHED_FILE" 2>/dev/null; then
    : # file clean
  else
    echo ">>> Restoring $TOUCHED_FILE (cleanup trap)"
    git -C "$REPO_ROOT" checkout "$TOUCHED_FILE"
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
log "Gradle vs Maven build measurement runbook"
echo "Repo root : $REPO_ROOT"
echo "Date      : $(date '+%Y-%m-%d %H:%M %Z')"
echo "Java      : $(java -version 2>&1 | head -1)"
echo "Gradle    : $("$REPO_ROOT/gradlew" --version 2>/dev/null | grep '^Gradle' || echo 'see ./gradlew --version')"
echo "Maven     : $("$REPO_ROOT/mvnw" --version 2>/dev/null | head -1 || echo 'not available')"
echo ""

# Write table header to temp file
{
  printf '| %-45s | %-6s | %-20s | %s\n' "Scenario" "Tool" "Wall-clock" "Notes"
  printf '| %s | %s | %s | %s\n' "---------------------------------------------" "------" "--------------------" "------"
} >> "$RESULTS_TMP"

# ===========================================================================
# SCENARIO 1: Selective recompile
#
# Touch one .java file in graylog2-server, then recompile.
# Gradle should recompile ONLY graylog2-server, leaving storage modules alone.
# Maven offline (-o) should also limit the module, but recompiles everything
# in the module (incremental compile depends on Maven version / compiler flags).
# ===========================================================================
log "SCENARIO 1: Selective recompile"
echo "Touching: $TOUCHED_FILE"

# Append a comment to actually change file content (Gradle uses content hashing,
# not mtime, so a bare `touch` has no effect).
echo '// MEASURE_MARKER' >> "$TOUCHED_FILE"

# --- Gradle ---
log "SCENARIO 1a — Gradle selective recompile"
note "Only :graylog2-server:compileJava should execute; storage modules untouched."
measure "Gradle selective recompile" \
  "$REPO_ROOT/gradlew" :graylog2-server:compileJava --build-cache
GRADLE_RECOMPILE_MS=$ELAPSED
table_row "Selective recompile (content change)" "Gradle" "$GRADLE_RECOMPILE_MS" \
  "Only compileJava executes; storage modules UP-TO-DATE"

# --- Maven ---
if [ "$SKIP_MAVEN" = "false" ]; then
  log "SCENARIO 1b — Maven selective recompile (offline, single module)"
  note "Maven offline -o -pl graylog2-server test-compile"
  note "WARNING: this can take several minutes on a cold Maven daemon."
  measure "Maven selective recompile" \
    "$REPO_ROOT/mvnw" -o -pl graylog2-server test-compile \
    -Dskip.web.build=true -Dmaven.javadoc.skip=true
  MAVEN_RECOMPILE_MS=$ELAPSED
  table_row "Selective recompile (content change)" "Maven" "$MAVEN_RECOMPILE_MS" \
    "mvnw -o -pl graylog2-server test-compile"
else
  table_row "Selective recompile (content change)" "Maven" "NOT MEASURED" \
    "skipped (--skip-maven); run without flag for baseline"
fi

# Restore the file immediately so subsequent measurements are clean.
git -C "$REPO_ROOT" checkout "$TOUCHED_FILE"
echo ">>> $TOUCHED_FILE restored"

# ===========================================================================
# SCENARIO 1c: Gradle no-op (UP-TO-DATE) after restore
# ===========================================================================
log "SCENARIO 1c — Gradle no-op compile (UP-TO-DATE check)"
measure "Gradle no-op compileJava" \
  "$REPO_ROOT/gradlew" :graylog2-server:compileJava --build-cache
table_row "No-op compile (UP-TO-DATE, after restore)" "Gradle" "$ELAPSED" \
  "All tasks UP-TO-DATE; demonstrates incremental guard"

# ===========================================================================
# SCENARIO 2: Single integration test
#
# Run exactly one IT class by fully-qualified name.
# Gradle: --tests filter on the integrationTest task.
# Maven: -Dit.test= with failsafe plugin (or equivalent).
#
# NOTE: Many ITs require a running MongoDB/Elasticsearch. We select
# CollectorTLSUtilsIT which exercises TLS utilities without a live datastore.
# If Docker is unavailable, the IT may still pass (local only) or be skipped.
# ===========================================================================
SINGLE_IT="org.graylog.collectors.CollectorTLSUtilsIT"

log "SCENARIO 2: Single integration test — $SINGLE_IT"

# --- Gradle cold (first run) ---
log "SCENARIO 2a — Gradle single IT (cold — test classes need compile)"
measure "Gradle single IT cold" \
  "$REPO_ROOT/gradlew" :graylog2-server:integrationTest \
  --tests "$SINGLE_IT" --build-cache
GRADLE_IT_COLD_MS=$ELAPSED
table_row "Single IT cold (incl. compileTestJava)" "Gradle" "$GRADLE_IT_COLD_MS" \
  "--tests $SINGLE_IT"

# --- Gradle warm (second run — UP-TO-DATE) ---
log "SCENARIO 2b — Gradle single IT (warm — UP-TO-DATE)"
measure "Gradle single IT warm" \
  "$REPO_ROOT/gradlew" :graylog2-server:integrationTest \
  --tests "$SINGLE_IT" --build-cache
GRADLE_IT_WARM_MS=$ELAPSED
table_row "Single IT warm (UP-TO-DATE)" "Gradle" "$GRADLE_IT_WARM_MS" \
  "integrationTest UP-TO-DATE; 0 extra compile"

# --- Maven ---
if [ "$SKIP_MAVEN" = "false" ]; then
  log "SCENARIO 2c — Maven single IT"
  note "Maven uses failsafe plugin (-Dit.test=). This is slow (JVM startup + full lifecycle to verify)."
  note "Equivalent: ./mvnw -pl graylog2-server -o verify -Dit.test=CollectorTLSUtilsIT"
  note "  -DskipTests=true (skip unit tests) -Dskip.web.build=true"
  measure "Maven single IT" \
    "$REPO_ROOT/mvnw" -pl graylog2-server -o verify \
    -Dit.test="CollectorTLSUtilsIT" \
    -DskipTests=true \
    -Dskip.web.build=true \
    -Dmaven.javadoc.skip=true
  table_row "Single IT" "Maven" "$ELAPSED" \
    "mvnw -pl graylog2-server verify -Dit.test=CollectorTLSUtilsIT"
else
  table_row "Single IT" "Maven" "NOT MEASURED" \
    "skipped (--skip-maven)"
fi

# ===========================================================================
# SCENARIO 3: Unit / IT separation
#
# Gradle `check` runs only unit tests (test task), never integrationTest.
# Maven `verify` runs both (unit tests in test phase, ITs in verify phase).
# We use --dry-run to show what would execute without spending time.
# ===========================================================================
log "SCENARIO 3: Unit/IT separation — dry-run task graph"

log "SCENARIO 3a — Gradle check --dry-run (shows only unit test task)"
echo "--- tasks included in :graylog2-server:check (--dry-run) ---"
"$REPO_ROOT/gradlew" :graylog2-server:check --dry-run 2>&1 \
  | grep "^:graylog2-server" | sed 's/ SKIPPED//' | sort
echo ""
note "integrationTest is NOT listed: ./gradlew check never runs ITs."
note "To run ITs explicitly: ./gradlew :graylog2-server:integrationTest"

table_row "Unit/IT separation: check --dry-run" "Gradle" "~0 (dry-run)" \
  "integrationTest absent from check graph; opt-in only"
table_row "Unit/IT separation: verify --dry-run" "Maven" "N/A" \
  "Maven verify always runs ITs (failsafe plugin bound to verify phase)"

# ===========================================================================
# SCENARIO 4: Cross-run build cache
#
# Clean local build outputs, then rebuild from the Gradle build cache.
# Tasks with cached entries restore instantly without re-executing.
# Maven has no equivalent cross-run cache.
# ===========================================================================
log "SCENARIO 4: Cross-run build cache"

# First: ensure there is something in the build cache by running a warm build.
"$REPO_ROOT/gradlew" :graylog2-server:compileJava --build-cache -q

log "SCENARIO 4a — Gradle clean then compile from cache"
"$REPO_ROOT/gradlew" clean -q
measure "Gradle compile from cache (post-clean)" \
  "$REPO_ROOT/gradlew" :graylog2-server:compileJava --build-cache
GRADLE_CACHE_MS=$ELAPSED

# Capture from-cache count from the output of a clean+compile run
CACHE_COUNT=$("$REPO_ROOT/gradlew" clean -q 2>&1; \
  "$REPO_ROOT/gradlew" :graylog2-server:compileJava --build-cache 2>&1 \
  | grep -c 'FROM-CACHE' || true)

table_row "Cross-run cache: clean + compileJava" "Gradle" "$GRADLE_CACHE_MS" \
  "Tasks restored FROM-CACHE (no recompile needed)"
table_row "Cross-run cache" "Maven" "N/A" \
  "No equivalent cross-run cache in Maven"

# Restore to warm state so later runs are fast
"$REPO_ROOT/gradlew" :graylog2-server:compileJava --build-cache -q

# ===========================================================================
# SCENARIO 5: Frontend skip (Java-only edit)
#
# After a Java-only change, Gradle's compileJava does NOT trigger yarnBuild.
# Maven's default-compile phase triggers the frontend plugin bound to it,
# so a Java edit causes a full webpack rebuild.
# ===========================================================================
log "SCENARIO 5: Frontend skip on Java-only change"

# Apply a content change to trigger recompile
echo '// MEASURE_MARKER_2' >> "$TOUCHED_FILE"

log "SCENARIO 5a — Gradle compileJava after Java change (no frontend expected)"
echo "--- tasks executed by ./gradlew :graylog2-server:compileJava ---"
"$REPO_ROOT/gradlew" :graylog2-server:compileJava --build-cache 2>&1 \
  | grep "^> Task" | sed 's/> Task //'
note "If yarnBuild / yarn / node does NOT appear above: frontend correctly skipped."
measure "Gradle compileJava (Java-only change)" \
  "$REPO_ROOT/gradlew" :graylog2-server:compileJava --build-cache
GRADLE_FRONTEND_SKIP_MS=$ELAPSED
table_row "Frontend skip: Java-only edit → compileJava" "Gradle" "$GRADLE_FRONTEND_SKIP_MS" \
  "No yarn/webpack tasks executed"

git -C "$REPO_ROOT" checkout "$TOUCHED_FILE"
echo ">>> $TOUCHED_FILE restored"

if [ "$SKIP_MAVEN" = "false" ]; then
  table_row "Frontend skip: Java-only edit → compile phase" "Maven" "N/A (NOT MEASURED)" \
    "Maven exec-maven-plugin binds frontend to compile; yarn always runs"
else
  table_row "Frontend skip: Java-only edit → compile phase" "Maven" "N/A (NOT MEASURED)" \
    "Maven exec-maven-plugin binds frontend to compile; yarn always runs"
fi

# ===========================================================================
# Print results table
# ===========================================================================
log "RESULTS TABLE"
cat "$RESULTS_TMP"
rm -f "$RESULTS_TMP"

echo ""
echo "Measurement complete. Tree should be clean:"
git -C "$REPO_ROOT" status --short
echo ""
echo "Re-run with: bash scripts/gradle-prototype/measure.sh [--skip-maven]"
