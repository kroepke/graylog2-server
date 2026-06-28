import org.gradle.api.attributes.Category
import org.gradle.api.attributes.LibraryElements
import org.gradle.api.attributes.Usage

plugins {
    id("graylog.java-conventions")
    `jvm-test-suite`
}

// ---------------------------------------------------------------------------
// serverProvided: resolves graylog2-server's FULL runtime classpath so storage
// plugins can compile against the server's API and all of its transitively
// required dependencies (guice, jackson, jakarta APIs, opentelemetry, etc.).
//
// Background: graylog2-server uses the `java` plugin (not `java-library`), so its
// `implementation` deps do NOT propagate to consumers' compile classpaths via the
// default apiElements variant. This mirrors Maven `compile` scope behaviour, where
// all transitive deps are available at compile time.
//
// Scope note: es7 and os2 POMs use `compile` for the server dep; os3 uses
// `provided`. For this prototype ALL three are treated as compile-only
// (server is always present at runtime; no need to bundle server classes).
// ---------------------------------------------------------------------------
val serverProvided by configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
    attributes {
        attribute(
            Usage.USAGE_ATTRIBUTE,
            objects.named(Usage::class.java, Usage.JAVA_RUNTIME)
        )
        attribute(
            Category.CATEGORY_ATTRIBUTE,
            objects.named(Category::class.java, Category.LIBRARY)
        )
        attribute(
            LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
            objects.named(LibraryElements::class.java, LibraryElements.JAR)
        )
    }
}

dependencies {
    "serverProvided"(project(":graylog2-server"))
    // testImplementation needs the server on the test runtime classpath too
    "testImplementation"(project(":graylog2-server"))
    "testImplementation"(project(path = ":graylog2-server", configuration = "testArtifacts"))
    // BOM platforms for test dependencies (hardcoded versions matching graylog-parent,
    // consistent with graylog.java-conventions which also hardcodes processor versions).
    // Keep these in sync with gradle/libs.versions.toml (buildSrc can't access the catalog easily).
    "testImplementation"(platform("org.junit:junit-bom:6.1.0"))
    "testImplementation"(platform("org.mockito:mockito-bom:5.23.0"))
    "testImplementation"(platform("org.testcontainers:testcontainers-bom:2.0.5"))
    // Jackson BOM for implementation deps (OS3 declares jackson-databind explicitly)
    "implementation"(platform("com.fasterxml.jackson:jackson-bom:2.22.0"))
}

// Add graylog2-server's full runtime classpath to compile classpaths (not runtime).
// This makes guice, jackson, jakarta APIs, etc. available for compilation without
// bundling them into the storage module's own artifact.
//
// Why afterEvaluate? Without it, the sourceSets accessor runs before the java plugin has
// registered its source sets, causing a "cannot find symbol" failure. The afterEvaluate
// form is intentional and is configuration-cache compatible (verified: second run reuses
// the cache cleanly with no problems reported). Do NOT replace with
// configurations.named("compileClasspath") { extendsFrom(serverProvided) } — extendsFrom
// inherits dependencies and re-resolves them using compileClasspath's own attributes
// (apiElements/compile variant), which does NOT carry implementation transitives,
// breaking compilation. The afterEvaluate approach correctly adds the already-resolved
// file collection and avoids re-resolution.
afterEvaluate {
    sourceSets["main"].compileClasspath += serverProvided
    sourceSets["test"].compileClasspath += serverProvided
}

// ---------------------------------------------------------------------------
// Test-classes JAR: exposes this module's compiled test classes as a consumable
// artifact so downstream modules (e.g., full-backend-tests Task 12) can depend
// on test helpers without duplicating source code.
// ---------------------------------------------------------------------------
val testJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}
val testArtifacts by configurations.consumable("testArtifacts")
artifacts { add("testArtifacts", testJar) }
