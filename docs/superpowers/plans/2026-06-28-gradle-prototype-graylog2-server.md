# Gradle Build Prototype for graylog2-server — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a boring, hand-authored Gradle build (Kotlin DSL) for the entire `graylog2-server` repo — backend modules plus the Yarn/Webpack frontend, minus the assembly tarball — that coexists with the existing Maven build and proves four signals: selective recompile, single-test runs, unit/IT separation, and a cross-run build cache.

**Architecture:** One Gradle root project at the `graylog2-server` repo root, one subproject per existing Maven module. Shared config lives in `buildSrc` convention plugins (the parent-POM replacement); versions live in a Gradle version catalog plus `platform()` BOM imports mirroring `graylog-parent`. Codegen (protobuf+gRPC, ANTLR4, annotation processors, Swagger→TS) becomes real tasks with declared inputs/outputs. The frontend is driven by the `node-gradle` plugin with up-to-date checking. The Gradle files are added alongside the POMs — nothing in Maven is deleted or modified.

**Tech Stack:** Gradle 9.6.1 (Kotlin DSL), Java 21 toolchain, `com.google.protobuf` plugin 0.9.4, `com.github.node-gradle.node` plugin 7.1.0, Gradle built-in `antlr` and `jvm-test-suite` plugins, Node 24.13.0 / Yarn 1.22.22, Webpack 5, JUnit, Mockito.

> **Gradle 9 plugin-compatibility note:** Gradle 9.6.1 requires JDK 17+ to run the daemon (Java 21 satisfies this). Two third-party plugins carry compatibility risk on Gradle 9 and must be verified, each with a boring fallback that removes the third-party dependency entirely:
> - **`com.google.protobuf` (Task 4)** — its published compatibility states "Gradle 7.6 up to the latest 8.x". Treat Task 4 as a canary on Gradle 9. **Fallback:** if it misbehaves, drop the plugin and invoke `protoc` from a plain `JavaExec`/`Exec` task with declared inputs/outputs (download `protoc` + `protoc-gen-grpc-java` via dependencies, resolve their files, exec them). Fully controlled, zero plugin-compat surface.
> - **`com.github.node-gradle.node` (Task 13)** — verify on Gradle 9. **Fallback:** run `yarn` directly via `Exec` tasks (install Node/Yarn out of band or via a small download task), keeping the same declared inputs/outputs for up-to-date checking.
>
> Do not assume either plugin works on 9 — confirm it in the task's verify step before proceeding.

## Global Constraints

These apply to **every** task. Values are copied verbatim from the spec and the existing POMs.

- **Never modify or delete any `pom.xml`, `pom.xml-tmpl`, the `.mvn/` Maven config, or `target/` output.** Maven must keep working on the same checkout. Gradle writes only to `build/` dirs and new `*.gradle.kts` / catalog / `buildSrc` files.
- **Branch:** all work happens on `gradle-build-prototype` in the `graylog2-server` repo (already created). Commits in this repo: if the signing agent is unavailable, use `git commit --no-gpg-sign`.
- **Java toolchain:** 21 (exact, `languageVersion = JavaLanguageVersion.of(21)`).
- **Compiler/processor JVM flags** (required by Graylog's annotation processors; from `.mvn/jvm.config`):
  ```
  --add-exports jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED
  --add-exports jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED
  --add-exports jdk.compiler/com.sun.tools.javac.main=ALL-UNNAMED
  --add-exports jdk.compiler/com.sun.tools.javac.model=ALL-UNNAMED
  --add-exports jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED
  --add-exports jdk.compiler/com.sun.tools.javac.processing=ALL-UNNAMED
  --add-exports jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED
  --add-exports jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED
  --add-opens jdk.compiler/com.sun.tools.javac.code=ALL-UNNAMED
  --add-opens jdk.compiler/com.sun.tools.javac.comp=ALL-UNNAMED
  ```
- **Annotation processors:** `com.google.errorprone:error_prone_core:2.50.0`, `com.google.auto.service:auto-service:1.1.1`, `com.google.auto.value:auto-value:1.11.1`, `org.graylog:jadconfig:1.1.0`.
- **Codegen versions:** `com.google.protobuf:protoc:4.35.1`, `io.grpc:protoc-gen-grpc-java:1.82.0`, ANTLR `4.13.2`.
- **Frontend:** Node `24.13.0`, Yarn `1.22.22`, downloaded from `https://graylog-ci-cache.s3.eu-west-1.amazonaws.com/downloads/node/`.
- **Unit test patterns:** `**/*Spec.class`, `**/*Test.class`. **Integration test patterns:** `**/*IntegrationTest.*`, `**/*IT.*`. `check` runs units only; integration tests are opt-in.
- **Unit test JVM args:** `-javaagent:<mockito-core jar> -Dio.netty.leakDetectionLevel=paranoid -Djava.awt.headless=true`. **Integration test JVM args:** `-Djava.awt.headless=true`.
- **Out of scope (do not build):** assembly tarball, `distribution` module, `data-node`, enterprise repo, `maven-shade` fat-jar, `.deb`/`.rpm`, SBOM, license-header enforcement, Chromium downloads, the manifest/CLI generation.
- **DSL:** Kotlin (`*.gradle.kts`). Keep it boring — no custom Gradle plugins beyond `buildSrc` convention plugins, no dynamic/`afterEvaluate` tricks unless a task explicitly calls for it.

**Repo root for all paths below:** `/home/kroepke/projects/graylog/gradle/graylog-project-repos/graylog2-server/` (referred to as `<root>`). All `./gradlew` commands run from `<root>`.

---

### Task 1: Gradle skeleton and wrapper

Stand up an empty-but-valid Gradle build that knows about the `:graylog2-server` subproject, with no build logic yet.

**Files:**
- Create: `<root>/settings.gradle.kts`
- Create: `<root>/build.gradle.kts`
- Create: `<root>/gradle.properties`
- Create: `<root>/gradle/wrapper/gradle-wrapper.properties` (+ wrapper jar/scripts via `gradle wrapper`)
- Modify: `<root>/.gitignore` (append Gradle ignores)

**Interfaces:**
- Produces: a working `./gradlew`, root project named `graylog`, subproject `:graylog2-server` mapped to dir `graylog2-server/`.

- [ ] **Step 1: Generate the wrapper**

Run from `<root>` (uses a system Gradle if available; otherwise install Gradle 9.6.1 first):
```bash
gradle wrapper --gradle-version 9.6.1 --distribution-type bin
```
If no system `gradle`, download once: `sdk install gradle 9.6.1` (SDKMAN) or fetch the distribution and run its `gradle wrapper`. Expected: creates `gradlew`, `gradlew.bat`, `gradle/wrapper/gradle-wrapper.jar`, `gradle/wrapper/gradle-wrapper.properties`. After generation, confirm `gradle/wrapper/gradle-wrapper.properties` pins `gradle-9.6.1-bin.zip` and run `./gradlew --version` to verify Gradle reports `9.6.1`.

- [ ] **Step 2: Write `settings.gradle.kts`**

```kotlin
rootProject.name = "graylog"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        // Shibboleth repo for OpenSAML, mirroring the Maven repo allowlist.
        maven("https://build.shibboleth.net/maven/releases/")
    }
}

include(":graylog2-server")
project(":graylog2-server").projectDir = file("graylog2-server")
```

- [ ] **Step 3: Write a minimal root `build.gradle.kts`**

```kotlin
// Root project intentionally has no plugins applied.
// Shared configuration lives in buildSrc convention plugins (added in Task 2).
```

- [ ] **Step 4: Write `gradle.properties`**

```properties
org.gradle.parallel=true
org.gradle.caching=true
org.gradle.configuration-cache=true
# Give the Gradle daemon enough heap for the large graylog2-server module.
org.gradle.jvmargs=-Xmx4g -Dfile.encoding=UTF-8
```

- [ ] **Step 5: Append Gradle entries to `.gitignore`**

Append (do not remove existing lines):
```
# Gradle
.gradle/
**/build/
!gradle/wrapper/gradle-wrapper.jar
```

- [ ] **Step 6: Verify the skeleton**

Run: `./gradlew projects`
Expected: SUCCESS, output lists root project `graylog` and `+--- Project ':graylog2-server'`.

- [ ] **Step 7: Commit**

```bash
git add settings.gradle.kts build.gradle.kts gradle.properties gradle/ gradlew gradlew.bat .gitignore
git commit --no-gpg-sign -m "build(gradle): add Gradle wrapper and project skeleton"
```

---

### Task 2: Version catalog and `java-conventions` convention plugin

Create the shared Java config (the parent-POM replacement) and the version catalog with the BOM platforms.

**Files:**
- Create: `<root>/gradle/libs.versions.toml`
- Create: `<root>/buildSrc/settings.gradle.kts`
- Create: `<root>/buildSrc/build.gradle.kts`
- Create: `<root>/buildSrc/src/main/kotlin/graylog.java-conventions.gradle.kts`

**Interfaces:**
- Produces: convention plugin id `graylog.java-conventions` applying the `java` plugin, Java 21 toolchain, the required compiler `--add-exports/--add-opens` args, UTF-8, the four annotation processors, and the BOM `platform()` imports. Other tasks apply it via `plugins { id("graylog.java-conventions") }`.

- [ ] **Step 1: Seed the version catalog**

Create `<root>/gradle/libs.versions.toml`. Seed it from `graylog-parent` (`<root>/pom.xml`) — these are the codegen/processor pins plus the BOM coordinates the build needs first. Add more entries as later tasks need them.
```toml
[versions]
protoc = "4.35.1"
grpcJava = "1.82.0"
antlr = "4.13.2"
errorprone = "2.50.0"
autoService = "1.1.1"
autoValue = "1.11.1"
jadconfig = "1.1.0"

[libraries]
# Annotation processors
errorprone-core = { module = "com.google.errorprone:error_prone_core", version.ref = "errorprone" }
auto-service = { module = "com.google.auto.service:auto-service", version.ref = "autoService" }
auto-value = { module = "com.google.auto.value:auto-value", version.ref = "autoValue" }
jadconfig = { module = "org.graylog:jadconfig", version.ref = "jadconfig" }
# Codegen artifacts
protoc = { module = "com.google.protobuf:protoc", version.ref = "protoc" }
grpc-protoc-gen = { module = "io.grpc:protoc-gen-grpc-java", version.ref = "grpcJava" }
antlr = { module = "org.antlr:antlr4", version.ref = "antlr" }
```

- [ ] **Step 2: Configure `buildSrc`**

`<root>/buildSrc/settings.gradle.kts`:
```kotlin
rootProject.name = "buildSrc"
```

`<root>/buildSrc/build.gradle.kts`:
```kotlin
plugins {
    `kotlin-dsl`
}
repositories {
    mavenCentral()
    gradlePluginPortal()
}
```

- [ ] **Step 3: Write `graylog.java-conventions`**

`<root>/buildSrc/src/main/kotlin/graylog.java-conventions.gradle.kts`:
```kotlin
import org.gradle.api.tasks.compile.JavaCompile

plugins {
    java
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
    withSourcesJar() // harmless; helps IDE/agents. Remove if it slows things.
}

repositories {
    mavenCentral()
    maven("https://build.shibboleth.net/maven/releases/")
}

// Compiler JVM flags required by Graylog's annotation processors (from .mvn/jvm.config).
val compilerJvmArgs = listOf(
    "--add-exports", "jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED",
    "--add-exports", "jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
    "--add-exports", "jdk.compiler/com.sun.tools.javac.main=ALL-UNNAMED",
    "--add-exports", "jdk.compiler/com.sun.tools.javac.model=ALL-UNNAMED",
    "--add-exports", "jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED",
    "--add-exports", "jdk.compiler/com.sun.tools.javac.processing=ALL-UNNAMED",
    "--add-exports", "jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED",
    "--add-exports", "jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED",
    "--add-opens", "jdk.compiler/com.sun.tools.javac.code=ALL-UNNAMED",
    "--add-opens", "jdk.compiler/com.sun.tools.javac.comp=ALL-UNNAMED",
)

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
    // Pass the module flags to the forked javac so annotation processors can reach javac internals.
    options.isFork = true
    options.forkOptions.jvmArgs?.addAll(compilerJvmArgs)
}

// Make the version catalog accessible inside this precompiled script plugin.
val libs = the<org.gradle.accessors.dm.LibrariesForLibs>()

dependencies {
    "compileOnly"(libs.auto.service)
    "annotationProcessor"(libs.auto.service)
    "compileOnly"(libs.auto.value)
    "annotationProcessor"(libs.auto.value)
    "compileOnly"(libs.jadconfig) // jadconfig annotations
    "annotationProcessor"(libs.jadconfig)
    // ErrorProne is wired as a processor path; full ErrorProne compiler integration
    // (the javac plugin) is out of scope for the prototype — processor path only.
    "annotationProcessor"(libs.errorprone.core)
}
```

> Note on `LibrariesForLibs`: precompiled script plugins in `buildSrc` access the catalog via the generated `LibrariesForLibs` type. For this to compile, `buildSrc/build.gradle.kts` needs the catalog on its classpath — add this to `buildSrc/build.gradle.kts` dependencies if Step 4 fails to resolve `libs`:
> ```kotlin
> dependencies {
>     implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))
> }
> ```
> The robust, boring alternative if the above is fiddly: skip `LibrariesForLibs` in the convention plugin and declare processor coordinates as plain strings (e.g. `"annotationProcessor"("com.google.auto.value:auto-value:1.11.1")`). Prefer the plain-string form if you hit catalog-access friction — it is more boring and always works.

- [ ] **Step 4: Verify buildSrc compiles and the plugin is available**

Run: `./gradlew :graylog2-server:help` — this forces `buildSrc` to compile even though `:graylog2-server` does not yet apply the plugin.
Expected: SUCCESS (buildSrc compiles). If `LibrariesForLibs` fails to resolve, switch the convention plugin to plain-string processor coordinates as noted above, then re-run.

- [ ] **Step 5: Commit**

```bash
git add gradle/libs.versions.toml buildSrc/
git commit --no-gpg-sign -m "build(gradle): add version catalog and java-conventions plugin"
```

---

### Task 3: Declare graylog2-server dependencies and BOM platforms

Translate the `graylog2-server` module's dependencies into Gradle so its configurations resolve. No compilation yet.

**Files:**
- Create: `<root>/graylog2-server/build.gradle.kts`
- Modify: `<root>/gradle/libs.versions.toml` (add BOM coordinates + the module's direct deps)

**Interfaces:**
- Consumes: `graylog.java-conventions`.
- Produces: a resolvable `:graylog2-server` with `implementation`/`testImplementation` deps and BOM `platform()` imports matching `graylog-parent`'s imported BOMs.

- [ ] **Step 1: Add BOM platforms to the catalog**

Read the `<dependencyManagement>` `<scope>import</scope>` BOMs in `<root>/pom.xml` (graylog-parent) — log4j, netty, jackson, grpc, junit, aws-sdk, and any others present. Add each as a catalog entry, e.g.:
```toml
# [versions] add the BOM versions found in graylog-parent, then under [libraries]:
bom-log4j   = { module = "org.apache.logging.log4j:log4j-bom", version.ref = "log4j" }
bom-netty   = { module = "io.netty:netty-bom", version.ref = "netty" }
bom-jackson = { module = "com.fasterxml.jackson:jackson-bom", version.ref = "jackson" }
bom-grpc    = { module = "io.grpc:grpc-bom", version.ref = "grpcJava" }
bom-junit   = { module = "org.junit:junit-bom", version.ref = "junit" }
bom-awssdk  = { module = "software.amazon.awssdk:bom", version.ref = "awssdk" }
```

- [ ] **Step 2: Write `graylog2-server/build.gradle.kts` (dependencies first)**

```kotlin
plugins {
    id("graylog.java-conventions")
}

val libs = the<org.gradle.accessors.dm.LibrariesForLibs>()

dependencies {
    // Import the same BOMs graylog-parent imports, so transitive versions match Maven.
    implementation(platform(libs.bom.log4j))
    implementation(platform(libs.bom.netty))
    implementation(platform(libs.bom.jackson))
    implementation(platform(libs.bom.grpc))
    implementation(platform(libs.bom.awssdk))
    testImplementation(platform(libs.bom.junit))

    // TODO-DURING-EXECUTION: translate every <dependency> from
    // graylog2-server/graylog2-server/pom.xml into a Gradle dependency here,
    // using this mapping:
    //   <scope>compile</scope>  -> implementation(...)
    //   <scope>provided</scope> -> compileOnly(...)
    //   <scope>runtime</scope>  -> runtimeOnly(...)
    //   <scope>test</scope>     -> testImplementation(...)
    //   <optional>true</optional> -> compileOnly(...) (prototype simplification)
    // Add a [libraries] catalog entry per artifact and reference it as libs.<name>.
    // Versions managed by an imported BOM above can be declared without a version.
}
```

- [ ] **Step 3: Translate the dependencies**

Open `<root>/graylog2-server/graylog2-server/pom.xml`, and for every `<dependency>` add the corresponding catalog entry + `dependencies { }` line per the mapping in Step 2. Group related artifacts in the catalog (e.g. `[bundles]` for jackson, log4j). Replace the `TODO-DURING-EXECUTION` comment block as you go.

- [ ] **Step 4: Verify resolution**

Run: `./gradlew :graylog2-server:dependencies --configuration compileClasspath`
Expected: SUCCESS, prints the resolved compile classpath with no `FAILED` markers. Fix any unresolved coordinate (wrong groupId/artifactId, or a repo the artifact needs that is missing from `settings.gradle.kts`).

- [ ] **Step 5: Commit**

```bash
git add gradle/libs.versions.toml graylog2-server/build.gradle.kts
git commit --no-gpg-sign -m "build(gradle): declare graylog2-server dependencies and BOM platforms"
```

---

### Task 4: Protobuf + gRPC code generation

Wire the protobuf plugin with the main proto set and the separate OpAMP set.

**Files:**
- Modify: `<root>/graylog2-server/build.gradle.kts`

**Interfaces:**
- Produces: `generateProto` task output on the main source set's generated sources; OpAMP protos compiled from their own source dir.

- [ ] **Step 1: Apply the protobuf plugin and configure both proto sets**

Add to `graylog2-server/build.gradle.kts`:
```kotlin
plugins {
    id("graylog.java-conventions")
    id("com.google.protobuf") version "0.9.4"
}

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:4.35.1" }
    plugins {
        create("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:1.82.0" }
    }
    generateProtoTasks {
        all().forEach { it.plugins { create("grpc") } }
    }
}

sourceSets {
    main {
        proto {
            // Default proto dir is src/main/proto. Add the OpAMP set explicitly;
            // its imports resolve within the same dir tree.
            srcDir("src/main/proto/opamp/proto")
        }
    }
}
```

> The Maven build excludes `opamp/proto/**` from the main run and compiles it separately because of distinct import roots. With the Gradle plugin, adding both as proto source dirs lets `protoc` see all of them; if OpAMP imports collide, split into a second source set mirroring the Maven separation. Verify in Step 2 before assuming the simple form works.

- [ ] **Step 2: Verify proto generation**

Run: `./gradlew :graylog2-server:generateProto`
Expected: SUCCESS; generated Java appears under `graylog2-server/build/generated/source/proto/main/java` and `.../grpc`. If OpAMP imports fail, create a dedicated source set for `src/main/proto/opamp/proto` and configure a separate generate task; re-run.

**Gradle 9 canary:** this is the first real test of `com.google.protobuf` on Gradle 9.6.1. If the plugin fails to apply or errors with a Gradle-version/incompatible-API message, switch to the plugin-free fallback from the Tech Stack note: declare `protoc`/`protoc-gen-grpc-java` as dependencies, resolve their files, and run them from a `JavaExec`/`Exec` task with `inputs.dir("src/main/proto")` + `outputs.dir(<generated>)`, then add the generated dir to the main source set. Do not spend long fighting plugin internals — the fallback is the boring path.

- [ ] **Step 3: Commit**

```bash
git add graylog2-server/build.gradle.kts
git commit --no-gpg-sign -m "build(gradle): protobuf and gRPC code generation"
```

---

### Task 5: ANTLR4 grammar generation

**Files:**
- Modify: `<root>/graylog2-server/build.gradle.kts`

**Interfaces:**
- Produces: parser/lexer sources for `RuleLang.g4` in package `org.graylog.plugins.pipelineprocessor.parser`.

- [ ] **Step 1: Apply the antlr plugin and point it at the grammar**

The grammar is at `src/main/antlr4/org/graylog/plugins/pipelineprocessor/parser/RuleLang.g4`. Add:
```kotlin
plugins {
    id("graylog.java-conventions")
    id("com.google.protobuf") version "0.9.4"
    antlr
}

dependencies {
    antlr("org.antlr:antlr4:4.13.2")
}

tasks.generateGrammarSource {
    // Preserve the package directory layout the grammar lives in.
    arguments = arguments + listOf("-visitor", "-package", "org.graylog.plugins.pipelineprocessor.parser")
    // ANTLR's source dir is src/main/antlr4 by convention — matches the existing layout.
}
```

> Check whether the existing Maven build uses the `-visitor` flag (search `graylog2-server/pom.xml` for `antlr4-maven-plugin` config). Match its flags exactly. If the Maven build does not generate visitors, drop `-visitor`.

- [ ] **Step 2: Verify grammar generation**

Run: `./gradlew :graylog2-server:generateGrammarSource`
Expected: SUCCESS; generated parser/lexer Java appears under `graylog2-server/build/generated-src/antlr/main/org/graylog/plugins/pipelineprocessor/parser/`.

- [ ] **Step 3: Commit**

```bash
git add graylog2-server/build.gradle.kts
git commit --no-gpg-sign -m "build(gradle): ANTLR4 grammar generation"
```

---

### Task 6: Compile graylog2-server (the milestone)

Get `compileJava` green with generated sources and annotation processors active. Expect to iterate on dependency gaps surfaced by the compiler.

**Files:**
- Modify: `<root>/graylog2-server/build.gradle.kts` (and `gradle/libs.versions.toml` for any missing deps)

**Interfaces:**
- Produces: compiled `:graylog2-server` main classes in `graylog2-server/build/classes/java/main`.

- [ ] **Step 1: Run the compile and read the first failures**

Run: `./gradlew :graylog2-server:compileJava`
Expected (first run): likely FAIL with `package x.y.z does not exist` or `cannot find symbol`. These indicate dependencies present in Maven but not yet translated.

- [ ] **Step 2: Resolve each gap**

For each missing package: find the providing dependency (search the Maven dependency tree `./mvnw -pl graylog2-server dependency:tree` from `<root>`, or the artifact online), add a catalog entry + `dependencies` line, and re-run. Repeat until compilation succeeds. If a failure is an annotation-processor error (e.g. AutoValue/JadConfig not running), confirm the `--add-exports/--add-opens` fork args from Task 2 are actually applied (`./gradlew :graylog2-server:compileJava --info | grep add-exports`).

- [ ] **Step 3: Verify a clean compile**

Run: `./gradlew :graylog2-server:compileJava`
Expected: SUCCESS (`BUILD SUCCESSFUL`), no warnings about skipped annotation processing.

- [ ] **Step 4: Verify generated sources are wired automatically**

Run: `./gradlew :graylog2-server:compileJava --console=plain` after `./gradlew clean` of that module.
Expected: `generateProto` and `generateGrammarSource` run *before* `compileJava` (visible in task order), confirming the codegen→compile dependency is automatic.

- [ ] **Step 5: Commit**

```bash
git add graylog2-server/build.gradle.kts gradle/libs.versions.toml
git commit --no-gpg-sign -m "build(gradle): graylog2-server compiles under Gradle"
```

---

### Task 7: Swagger/OpenAPI generation task

A `JavaExec` task that runs `GenerateApiDefinition`, with declared inputs/outputs so it is skipped when unchanged. Needed by the frontend later.

**Files:**
- Modify: `<root>/graylog2-server/build.gradle.kts`

**Interfaces:**
- Produces: a `generateApiDefinition` task whose output dir is `graylog2-server/build/swagger`. Consumed by the frontend's `generateApiDefs` (Task 13).

- [ ] **Step 1: Register the JavaExec task**

The Maven exec runs main class `org.graylog.api.GenerateApiDefinition` with args `target/swagger org.graylog org.graylog2`. Add:
```kotlin
val generateApiDefinition by tasks.registering(JavaExec::class) {
    dependsOn(tasks.compileJava, tasks.processResources)
    mainClass.set("org.graylog.api.GenerateApiDefinition")
    classpath = sourceSets.main.get().runtimeClasspath
    val outDir = layout.buildDirectory.dir("swagger")
    args(outDir.get().asFile.absolutePath, "org.graylog", "org.graylog2")
    // Declared I/O so Gradle can skip this when nothing changed.
    inputs.files(sourceSets.main.get().output)
    outputs.dir(outDir)
}
```

- [ ] **Step 2: Verify it produces the spec**

Run: `./gradlew :graylog2-server:generateApiDefinition`
Expected: SUCCESS; `graylog2-server/build/swagger/` contains the generated OpenAPI JSON. A second run prints `UP-TO-DATE`.

- [ ] **Step 3: Commit**

```bash
git add graylog2-server/build.gradle.kts
git commit --no-gpg-sign -m "build(gradle): Swagger/OpenAPI generation task"
```

---

### Task 8: Unit test suite + jar + resources

Configure the `test` (unit) suite with the correct includes and Mockito javaagent, plus the jar with web-assets resource wiring (assets filled in Task 13).

**Files:**
- Modify: `<root>/graylog2-server/build.gradle.kts`

**Interfaces:**
- Produces: a `test` task running only unit tests; a `jar` task. Establishes the `mockitoAgent` configuration reused by integrationTest.

- [ ] **Step 1: Configure the unit `test` suite via the JVM Test Suite plugin**

Add:
```kotlin
plugins {
    id("graylog.java-conventions")
    id("com.google.protobuf") version "0.9.4"
    antlr
    `jvm-test-suite`
}

// Mockito as a javaagent (Java 21 inline-mock requirement), resolved to a file path.
val mockitoAgent = configurations.create("mockitoAgent") { isTransitive = false }
dependencies {
    // Pin the same mockito-core version graylog-parent uses (read from pom.xml).
    mockitoAgent("org.mockito:mockito-core") // version via junit/mockito BOM or explicit catalog entry
}

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter() // confirm: JUnit 5. If suite uses JUnit 4/vintage, add vintage engine.
            targets.all {
                testTask.configure {
                    include("**/*Spec.class", "**/*Test.class")
                    exclude("**/*IntegrationTest.class", "**/*IT.class")
                    jvmArgs(
                        "-javaagent:${mockitoAgent.singleFile}",
                        "-Dio.netty.leakDetectionLevel=paranoid",
                        "-Djava.awt.headless=true",
                    )
                }
            }
        }
    }
}
```

> Confirm the test engine: search `graylog2-server/pom.xml` for `junit-jupiter` vs `junit:junit`. Wire `useJUnitJupiter()` or `useJUnit()` (+ vintage engine) to match. Add a `libs.versions.toml` entry for `mockito` if the BOM does not supply a concrete version for the `mockitoAgent` configuration.

- [ ] **Step 2: Verify a single known unit test runs**

Pick one existing fast unit test (e.g. grep for a `*Test.java` with no external deps). Run:
```bash
./gradlew :graylog2-server:test --tests "<fully.qualified.TestName>"
```
Expected: SUCCESS, 1 test executed. Confirms the suite, engine, and Mockito agent are wired.

- [ ] **Step 3: Verify integration tests are excluded from `test`**

Run: `./gradlew :graylog2-server:test --tests "*IT" --dry-run` (or run `test` and inspect the report).
Expected: no `*IT` classes selected by the `test` task.

- [ ] **Step 4: Commit**

```bash
git add graylog2-server/build.gradle.kts gradle/libs.versions.toml
git commit --no-gpg-sign -m "build(gradle): unit test suite with Mockito agent"
```

---

### Task 9: Integration test suite (unit/IT separation)

Add the opt-in `integrationTest` suite and prove single-IT execution.

**Files:**
- Modify: `<root>/graylog2-server/build.gradle.kts`

**Interfaces:**
- Produces: an `integrationTest` task NOT wired into `check`, sharing main + test code, selecting `*IT`/`*IntegrationTest`.

- [ ] **Step 1: Register the `integrationTest` suite**

Inside `testing { suites { ... } }` add:
```kotlin
val integrationTest by registering(JvmTestSuite::class) {
    useJUnitJupiter()
    // Integration tests live alongside unit tests in src/test/java in the
    // current layout; reuse the main test source set's compiled classes + classpath
    // rather than a separate source dir, to avoid moving files (Maven coexistence).
    sources {
        java.setSrcDirs(listOf("src/test/java"))
        resources.setSrcDirs(listOf("src/test/resources"))
    }
    dependencies {
        implementation(project())
        // This suite reuses src/test/java directly (set above), so it needs the same
        // test libraries the unit `test` suite uses — re-declare them here (or via a
        // shared list of catalog refs). Do NOT use java-test-fixtures: the chosen
        // sharing mechanism is the testArtifacts configuration (Task 10), and that is
        // for cross-MODULE consumption, not for this same-module suite.
    }
    targets.all {
        testTask.configure {
            include("**/*IntegrationTest.class", "**/*IT.class")
            jvmArgs("-Djava.awt.headless=true")
            shouldRunAfter(test)
        }
    }
}
```

> If sharing `src/test/java` between the `test` and `integrationTest` suites causes double-compilation friction, the simpler boring alternative is to NOT create a separate source set and instead add a second `Test` task to the existing `test` source set that filters for `*IT`. Either way the requirement is: `check` excludes ITs; an `integrationTest` task runs them on demand. Use whichever compiles cleanly with the least ceremony.

- [ ] **Step 2: Ensure `check` does NOT depend on `integrationTest`**

By default the JVM Test Suite plugin only wires the built-in `test` suite into `check`. Do **not** add `tasks.check { dependsOn(integrationTest) }`. Verify in Step 3.

- [ ] **Step 3: Verify separation and single-IT run**

```bash
./gradlew :graylog2-server:check --dry-run
```
Expected: the task list includes `:graylog2-server:test` but NOT `:graylog2-server:integrationTest`.

Then run one integration test by name:
```bash
./gradlew :graylog2-server:integrationTest --tests "<fully.qualified.SomeIT>"
```
Expected: SUCCESS, only that IT runs, and (on a warm build) no recompilation of `:graylog2-server` occurs.

- [ ] **Step 4: Commit**

```bash
git add graylog2-server/build.gradle.kts
git commit --no-gpg-sign -m "build(gradle): opt-in integrationTest suite, unit/IT separation"
```

---

### Task 10: Share test classes via a tests-jar consumable configuration

Storage modules and `full-backend-tests` consume `graylog2-server`'s test classes. Expose them without moving files (Maven coexistence).

**Files:**
- Modify: `<root>/graylog2-server/build.gradle.kts`

**Interfaces:**
- Produces: a consumable configuration `testArtifacts` on `:graylog2-server` exposing a `*-tests.jar`. Consumed as `project(path = ":graylog2-server", configuration = "testArtifacts")`.

- [ ] **Step 1: Build and expose a tests jar**

Add to `graylog2-server/build.gradle.kts`:
```kotlin
val testJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets.test.get().output)
}
val testArtifacts by configurations.consumable("testArtifacts")
artifacts {
    add("testArtifacts", testJar)
}
```

> This mirrors Maven's `test-jar` exactly: it exposes the compiled test classes only. Consumers must re-declare any transitive test dependencies they need (the same constraint Maven imposes). When a consumer references `configuration = "testArtifacts"`, Gradle uses that configuration directly and skips attribute matching — boring and predictable.

- [ ] **Step 2: Verify the tests jar builds**

Run: `./gradlew :graylog2-server:testJar`
Expected: SUCCESS; `graylog2-server/build/libs/graylog2-server-*-tests.jar` exists and contains test `.class` files (`jar tf` shows e.g. test base classes).

- [ ] **Step 3: Commit**

```bash
git add graylog2-server/build.gradle.kts
git commit --no-gpg-sign -m "build(gradle): expose graylog2-server test classes via testArtifacts"
```

---

### Task 11: Storage plugin modules + `storage-plugin-conventions`

Add the three storage modules and their shared convention plugin.

**Files:**
- Create: `<root>/buildSrc/src/main/kotlin/graylog.storage-plugin-conventions.gradle.kts`
- Create: `<root>/graylog-storage-elasticsearch7/build.gradle.kts`
- Create: `<root>/graylog-storage-opensearch2/build.gradle.kts`
- Create: `<root>/graylog-storage-opensearch3/build.gradle.kts`
- Modify: `<root>/settings.gradle.kts` (register the three projects)

**Interfaces:**
- Consumes: `:graylog2-server` (compileOnly), `:graylog2-server` testArtifacts (testImplementation).
- Produces: three compilable storage plugin projects.

- [ ] **Step 1: Register the projects in `settings.gradle.kts`**

Append:
```kotlin
include(":graylog-storage-elasticsearch7")
include(":graylog-storage-opensearch2")
include(":graylog-storage-opensearch3")
// Project dirs match their names at repo root — no projectDir override needed.
```

- [ ] **Step 2: Write `graylog.storage-plugin-conventions`**

`<root>/buildSrc/src/main/kotlin/graylog.storage-plugin-conventions.gradle.kts`:
```kotlin
plugins {
    id("graylog.java-conventions")
    `jvm-test-suite`
}

dependencies {
    // Storage plugins compile against the server but it is provided at runtime
    // (they are loaded as plugins). compileOnly is the boring Maven-`provided` analog.
    "compileOnly"(project(":graylog2-server"))
    "testImplementation"(project(":graylog2-server"))
    "testImplementation"(project(path = ":graylog2-server", configuration = "testArtifacts"))
}
```

> The three modules use slightly different Maven scopes for the server dep (es7/os2 `compile`, os3 `provided`). For the prototype, `compileOnly` everywhere is correct because the server is always present at runtime via the combined classpath. Note this divergence in the results doc.

- [ ] **Step 3: Write each module's build file**

For each storage module, create `build.gradle.kts`:
```kotlin
plugins {
    id("graylog.storage-plugin-conventions")
}

val libs = the<org.gradle.accessors.dm.LibrariesForLibs>()

dependencies {
    // TODO-DURING-EXECUTION: translate the module-specific <dependency> entries from
    // graylog-storage-<name>/pom.xml using the Task 3 scope mapping. The server dep and
    // server test-jar are already provided by the convention plugin — do not repeat them.
}
```
Then translate each module's POM dependencies (the ES/OpenSearch client libraries, etc.).

- [ ] **Step 4: Verify each storage module compiles**

```bash
./gradlew :graylog-storage-opensearch2:compileJava
./gradlew :graylog-storage-opensearch3:compileJava
./gradlew :graylog-storage-elasticsearch7:compileJava
```
Expected: SUCCESS for each (resolve dependency gaps as in Task 6).

- [ ] **Step 5: Verify a storage module sees server test classes**

Run: `./gradlew :graylog-storage-opensearch2:compileTestJava`
Expected: SUCCESS — confirms `testArtifacts` consumption works.

- [ ] **Step 6: Commit**

```bash
git add settings.gradle.kts buildSrc/src/main/kotlin/graylog.storage-plugin-conventions.gradle.kts graylog-storage-*/build.gradle.kts gradle/libs.versions.toml
git commit --no-gpg-sign -m "build(gradle): storage plugin modules and conventions"
```

---

### Task 12: `full-backend-tests` module

The heavy backend test module — must compile and stay out of the default `check`.

**Files:**
- Create: `<root>/full-backend-tests/build.gradle.kts`
- Modify: `<root>/settings.gradle.kts`

**Interfaces:**
- Consumes: `:graylog2-server` + all three storage modules (compile and testArtifacts).
- Produces: a compilable `:full-backend-tests` whose heavy tests are opt-in.

- [ ] **Step 1: Register the project**

Append to `settings.gradle.kts`:
```kotlin
include(":full-backend-tests")
```

- [ ] **Step 2: Write the build file**

```kotlin
plugins {
    id("graylog.java-conventions")
    `jvm-test-suite`
}

val libs = the<org.gradle.accessors.dm.LibrariesForLibs>()

dependencies {
    implementation(project(":graylog2-server"))
    implementation(project(path = ":graylog2-server", configuration = "testArtifacts"))
    listOf(
        ":graylog-storage-elasticsearch7",
        ":graylog-storage-opensearch2",
        ":graylog-storage-opensearch3",
    ).forEach {
        implementation(project(it))
        implementation(project(path = it, configuration = "testArtifacts"))
    }
    // TODO-DURING-EXECUTION: add remaining test deps from full-backend-tests/pom.xml.
}

// The heavy backend tests use @FullBackendTest, not @Test, and must be opt-in.
testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter()
            targets.all { testTask.configure { jvmArgs("-Djava.awt.headless=true") } }
        }
    }
}
```

> Each storage module also needs a `testArtifacts` consumable configuration if `full-backend-tests` consumes their test classes (the POM shows it does). Add the same `testJar`/`testArtifacts` block from Task 10 to each storage module's build file, then reference `configuration = "testArtifacts"` above. Do this now if Step 3 reports missing test classes.

- [ ] **Step 3: Verify it compiles without running the heavy tests**

Run: `./gradlew :full-backend-tests:compileTestJava`
Expected: SUCCESS.

Run: `./gradlew :full-backend-tests:check --dry-run`
Expected: the heavy `@FullBackendTest` suites are not executed by a plain `check` (they require explicit invocation / tag filtering). If `check` would run them, gate them behind a JUnit tag filter (`excludeTags("full-backend-test")` on the `test` task) to match Maven's `-DexcludedGroups=full-backend-test`.

- [ ] **Step 4: Commit**

```bash
git add settings.gradle.kts full-backend-tests/build.gradle.kts graylog-storage-*/build.gradle.kts
git commit --no-gpg-sign -m "build(gradle): full-backend-tests module (opt-in heavy tests)"
```

---

### Task 13: Frontend build + bundle into the server jar

Drive Yarn/Webpack via the node-gradle plugin with up-to-date checking, and wire assets into `:graylog2-server` resources.

**Files:**
- Create: `<root>/graylog2-web-interface/build.gradle.kts`
- Modify: `<root>/settings.gradle.kts`
- Modify: `<root>/graylog2-server/build.gradle.kts` (consume web assets)

**Interfaces:**
- Consumes: `:graylog2-server:generateApiDefinition` output (`build/swagger`).
- Produces: `yarnBuild` task whose output (`target/web/build`) is bundled into the server jar at `web-interface/assets`.

- [ ] **Step 1: Register the frontend project**

Append to `settings.gradle.kts`:
```kotlin
include(":graylog2-web-interface")
```

- [ ] **Step 2: Write `graylog2-web-interface/build.gradle.kts`**

```kotlin
import com.github.gradle.node.yarn.task.YarnTask

plugins {
    id("com.github.node-gradle.node") version "7.1.0"
}

node {
    version.set("24.13.0")
    yarnVersion.set("1.22.22")
    download.set(true)
    distBaseUrl.set("https://graylog-ci-cache.s3.eu-west-1.amazonaws.com/downloads/node/")
}

// Generate API defs from the server's Swagger output. Mirrors:
//   yarn generate:apidefs  ==  ts-node src/generator/generate.ts ../graylog2-server/target/swagger target/api
val generateApiDefs by tasks.registering(YarnTask::class) {
    dependsOn(":graylog2-server:generateApiDefinition")
    args.set(listOf("generate:apidefs"))
    inputs.dir(project(":graylog2-server").layout.buildDirectory.dir("swagger"))
    inputs.files("src/generator/generate.ts")
    outputs.dir("target/api")
}

// Production webpack build. Mirrors:
//   yarn build  ==  cross-env disable_plugins=true webpack --config webpack.bundled.ts
val yarnBuild by tasks.registering(YarnTask::class) {
    dependsOn(tasks.named("yarn"), generateApiDefs) // 'yarn' = yarn install
    args.set(listOf("build"))
    // Up-to-date checking: skip the multi-minute webpack build when nothing changed.
    inputs.dir("src")
    inputs.files("package.json", "yarn.lock")
    inputs.files(fileTree("webpack").include("**/*.ts", "**/*.js"))
    inputs.dir("target/api")
    outputs.dir("target/web/build")
}

// Frontend unit tests, decoupled from Java tests. Mirrors: yarn test == jest --maxWorkers=50%
val yarnTest by tasks.registering(YarnTask::class) {
    dependsOn(tasks.named("yarn"))
    args.set(listOf("test"))
    inputs.dir("src")
    inputs.files("package.json", "yarn.lock")
    outputs.dir(layout.buildDirectory.dir("jest-marker")) // marker so reruns can be cached
}
```

> The node-gradle `yarn` task = `yarn install`. The web interface dir is `<root>/graylog2-web-interface`; the plugin runs yarn there by default. Note `generate:apidefs` reads `../graylog2-server/target/swagger` (Maven's path), but our Gradle task produces `graylog2-server/build/swagger`. Either (a) point the `generateApiDefs` input/working setup so the script reads `build/swagger`, or (b) copy `build/swagger` to `../graylog2-server/target/swagger` before running. Prefer (b) for the prototype (one boring `Copy` task) to avoid editing the TS generator's hardcoded path.

- [ ] **Step 3: Bundle assets into the server jar**

In `graylog2-server/build.gradle.kts`, make `processResources` include the built web assets at `web-interface/assets` (matching the Maven `copy-web-ui-assets` execution):
```kotlin
tasks.processResources {
    dependsOn(":graylog2-web-interface:yarnBuild")
    from(project(":graylog2-web-interface").file("target/web/build")) {
        into("web-interface/assets")
    }
}
```

> This couples the server jar to the frontend build. That is intended for a full `:graylog2-server:jar`, but it means a server-only compile should NOT trigger webpack. Confirm: `compileJava` does not depend on `processResources` for the web assets (it does not by default). Only `jar`/`processResources` pull the frontend. Keep it that way so backend-only inner loops stay fast.

- [ ] **Step 4: Verify the frontend builds and is cacheable**

```bash
./gradlew :graylog2-web-interface:yarnBuild
```
Expected: SUCCESS; `graylog2-web-interface/target/web/build/` populated. Run again:
```bash
./gradlew :graylog2-web-interface:yarnBuild
```
Expected: `UP-TO-DATE` (no webpack run).

- [ ] **Step 5: Verify assets land in the jar and a Java-only edit skips webpack**

```bash
./gradlew :graylog2-server:jar
jar tf graylog2-server/build/libs/graylog2-server-*.jar | grep web-interface/assets | head
```
Expected: lists `web-interface/assets/...` entries.

Touch a Java file, then:
```bash
touch graylog2-server/src/main/java/<some>/File.java
./gradlew :graylog2-server:compileJava
```
Expected: SUCCESS without any `yarn`/`yarnBuild` task running.

- [ ] **Step 6: Commit**

```bash
git add settings.gradle.kts graylog2-web-interface/build.gradle.kts graylog2-server/build.gradle.kts
git commit --no-gpg-sign -m "build(gradle): frontend build via node-gradle, bundle assets into server jar"
```

---

### Task 14: Build cache validation

Confirm the cross-run cache behavior the whole prototype is meant to demonstrate.

**Files:**
- Modify: `<root>/gradle.properties` (only if a tweak is needed — caching is already on from Task 1)

**Interfaces:**
- Produces: documented evidence that unchanged work is skipped across clean invocations.

- [ ] **Step 1: Warm the cache with a full build**

Run: `./gradlew build -x integrationTest`
Expected: SUCCESS (units + jar + frontend; ITs and heavy tests excluded).

- [ ] **Step 2: Prove cross-run reuse after a clean**

```bash
./gradlew clean
./gradlew build -x integrationTest --console=plain
```
Expected: a large share of tasks report `FROM-CACHE` (compile, test, codegen) rather than re-executing. Record the console summary.

- [ ] **Step 3: Prove a no-op rebuild is fully up to date**

Run: `./gradlew build -x integrationTest` again without changes.
Expected: `BUILD SUCCESSFUL ... N actionable tasks: N up-to-date` (0 executed).

- [ ] **Step 4: Commit any gradle.properties tweak**

```bash
git add gradle.properties
git commit --no-gpg-sign -m "build(gradle): confirm and tune build cache settings"
```

(Skip the commit if no file changed.)

---

### Task 15: Measurement runbook + results document

Capture the four-signal comparison vs Maven as a committed deliverable.

**Files:**
- Create: `<root>/docs/superpowers/specs/2026-06-28-gradle-prototype-results.md`
- Create: `<root>/scripts/gradle-prototype/measure.sh` (the runbook, re-runnable)

**Interfaces:**
- Consumes: everything built above.
- Produces: a results doc with concrete Maven-vs-Gradle numbers for each acceptance criterion.

- [ ] **Step 1: Write the measurement script**

Create `<root>/scripts/gradle-prototype/measure.sh` — a bash script that runs each scenario for both tools with `time` and tees output. It must cover, each timed twice (cold/warm):
1. Selective recompile: touch one `graylog2-server` `.java`; `mvn -o -pl graylog2-server test-compile` vs `./gradlew :graylog2-server:compileJava`; assert storage modules untouched by Gradle.
2. Single IT: one `*IT` by name under Maven (`-Dit.test=`) vs `./gradlew :graylog2-server:integrationTest --tests`.
3. Unit/IT separation: `./gradlew :graylog2-server:check --dry-run` shows no IT; Maven `verify` baseline time.
4. Cross-run cache: `./gradlew clean && ./gradlew build -x integrationTest` twice; capture FROM-CACHE/up-to-date counts.
5. Frontend skip: Java-only edit → confirm no webpack under Gradle; Maven baseline rebuilds web.

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root
log() { printf '\n=== %s ===\n' "$1"; }
# ... one timed block per scenario, writing wall-clock numbers to results.tmp ...
# Keep it boring: explicit `time` around each command, append results to a markdown table.
```
Fill in each scenario block concretely while implementing (the exact commands are listed in the spec's acceptance criteria and Tasks 8/9/13/14 above).

- [ ] **Step 2: Run it and record results**

Run: `bash scripts/gradle-prototype/measure.sh | tee /tmp/measure.out`
Expected: completes and prints a Maven-vs-Gradle table.

- [ ] **Step 3: Write the results document**

Create `docs/superpowers/specs/2026-06-28-gradle-prototype-results.md` with: a table of the five scenarios (Maven time, Gradle cold, Gradle warm), pass/fail vs each acceptance criterion, and a short "divergences found" section (dependency-scope differences, any codegen quirks, the `target/swagger` path copy, storage-scope note from Task 11).

- [ ] **Step 4: Commit**

```bash
git add scripts/gradle-prototype/measure.sh docs/superpowers/specs/2026-06-28-gradle-prototype-results.md
git commit --no-gpg-sign -m "docs(gradle): measurement runbook and prototype results"
```

---

## Self-Review

**Spec coverage:**
- Layout & coexistence (spec §1) → Task 1 (+ Global Constraints "never modify POMs").
- Module mapping (§2) → Tasks 1, 11, 12, 13 (settings registration); compileOnly/test edges in Tasks 11–12.
- Convention plugins (§3) → Tasks 2, 11.
- Version catalog + BOM platforms (§4) → Tasks 2, 3.
- Codegen (§5) → Tasks 4 (proto), 5 (antlr), 6 (annotation processors active), 7 (swagger).
- Frontend (§6) → Task 13.
- Test architecture (§7) → Tasks 8 (unit), 9 (IT split), 10 (test-jar sharing).
- Build cache & perf (§8) → Tasks 1 (properties), 14 (validation).
- Acceptance criteria/runbook (§10) → Task 15.
- Risks (§11): version drift (accepted, noted Task 3), JVM flags (Task 2 + Task 6 Step 2 check), resolution differences (Task 6/11 gap-resolution loops), codegen ordering (Task 6 Step 4, Task 13 Step 2 path note).

**Placeholder scan:** The two `TODO-DURING-EXECUTION` markers (Task 3 Step 2, Task 11 Step 3, Task 12 Step 2) are deliberate, bounded instructions to translate enumerated POM `<dependency>` blocks via the explicit scope-mapping table — not vague placeholders. They cannot be pre-expanded without inlining 100+ dependency coordinates; the source file and exact mapping are specified.

**Type/name consistency:** task/configuration names are consistent across tasks — `testArtifacts` (Tasks 10, 11, 12), `generateApiDefinition` (Task 7) consumed in Task 13, `generateApiDefs`/`yarnBuild`/`yarnTest` (Task 13), `mockitoAgent` (Task 8), suites `test`/`integrationTest` (Tasks 8, 9). Module dirs match `settings.gradle.kts` includes.

## Known decision points the implementer must confirm against the POMs (not placeholders — verifications)
- ANTLR `-visitor` flag (Task 5) — match the Maven `antlr4-maven-plugin` config.
- Test engine JUnit 5 vs 4 (Task 8) — match `graylog2-server/pom.xml`.
- OpAMP proto separation (Task 4) — simple combined source dir vs separate source set.
- `full-backend-test` tag gating (Task 12) — match `-DexcludedGroups=full-backend-test`.
