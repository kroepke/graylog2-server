import org.gradle.api.attributes.Category
import org.gradle.api.attributes.LibraryElements
import org.gradle.api.attributes.Usage

plugins {
    id("graylog.java-conventions")
    `jvm-test-suite`
}

val libs = the<org.gradle.accessors.dm.LibrariesForLibs>()

// ---------------------------------------------------------------------------
// backendProvided: resolves the full JAVA_RUNTIME classpath for graylog2-server
// and all three storage modules so their transitive implementation deps
// (guice, jackson, netty, log4j, etc.) are visible at test compile and runtime.
//
// Mirrors the serverProvided pattern in graylog.storage-plugin-conventions.
// JAVA_RUNTIME attributes force resolution of each project's runtimeElements
// variant, which carries implementation transitives absent from apiElements.
//
// Do NOT use extendsFrom — it re-resolves via apiElements and loses
// implementation transitives (verified in Task 11). Add the resolved
// file collection directly via afterEvaluate.
// ---------------------------------------------------------------------------
val backendProvided by configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage::class.java, Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category::class.java, Category.LIBRARY))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements::class.java, LibraryElements.JAR))
    }
}

dependencies {
    // --- backendProvided: resolve full runtime classpath of server + storage modules ---
    "backendProvided"(project(":graylog2-server"))
    listOf(
        ":graylog-storage-elasticsearch7",
        ":graylog-storage-opensearch2",
        ":graylog-storage-opensearch3",
    ).forEach { "backendProvided"(project(it)) }

    // --- test-class JARs from server + all storage modules ---
    testImplementation(project(path = ":graylog2-server", configuration = "testArtifacts"))
    listOf(
        ":graylog-storage-elasticsearch7",
        ":graylog-storage-opensearch2",
        ":graylog-storage-opensearch3",
    ).forEach { testImplementation(project(path = it, configuration = "testArtifacts")) }

    // --- BOM platforms (mirror graylog-parent + graylog-project-parent imports) ---
    testImplementation(platform(libs.bom.log4j))
    testImplementation(platform(libs.bom.jackson))
    testImplementation(platform(libs.bom.junit))
    testImplementation(platform(libs.bom.mockito))
    testImplementation(platform(libs.bom.testcontainers))

    // --- Logging (compile scope in Maven POM; versions from bom-log4j + slf4j version) ---
    testImplementation(libs.log4j.api)
    testImplementation(libs.log4j.core)
    testImplementation(libs.log4j.slf4j2.impl)
    testImplementation(libs.slf4j.jcl.over)
    testImplementation(libs.slf4j.log4j.over)
    testImplementation(libs.slf4j.api)

    // --- Guava (compile scope in Maven POM; version from libs.guava) ---
    testImplementation(libs.guava)

    // --- Date/time (compile scope in Maven POM) ---
    testImplementation(libs.assertj.joda.time)
    testImplementation(libs.joda.time)

    // --- Test / assertion libs (compile scope in Maven POM, no explicit <scope>) ---
    testImplementation(libs.mockito.core)
    testImplementation(libs.assertj.core)
    testImplementation(libs.awaitility)

    // --- REST-Assured (test scope in Maven POM; version restassured.version = 6.0.0) ---
    testImplementation(libs.rest.assured)
    testImplementation(libs.rest.assured.json.path)

    // --- Jackson (test scope in Maven POM; versions from bom-jackson platform above) ---
    testImplementation(libs.jackson.core)
    testImplementation(libs.jackson.databind)

    // --- Commons IO (test scope in Maven POM; commonsIo = 2.22.0 from catalog) ---
    testImplementation(libs.commons.io)

    // --- JUnit (test scope in Maven POM; version from bom-junit) ---
    testImplementation(libs.junit.jupiter)

    // --- Mockito JUnit Jupiter extension (test scope in Maven POM; from bom-mockito) ---
    testImplementation(libs.mockito.junit.jupiter)

    // --- Hamcrest (test scope in Maven POM; hamcrest.version = 3.0 from root POM) ---
    testImplementation(libs.hamcrest)

    // --- Testcontainers (test scope in Maven POM; versions from bom-testcontainers) ---
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.elasticsearch)

    // --- OpenSearch Testcontainers (test scope; opensearchTestcontainers = 4.1.0 from catalog) ---
    testImplementation(libs.opensearch.testcontainers)

    // --- GraalVM Polyglot API (test scope; graalvm.version = 25.0.3 from root POM) ---
    // Used by CustomizationConfigIT (org.graalvm.polyglot.Context / HostAccess).
    testImplementation(libs.graalvm.polyglot)
    // org.graalvm.js:js declared as type=pom in Maven POM — adds JS engine transitives.
    // Gradle resolves it as a POM-only dep pulling in its transitive deps.
    testImplementation(libs.graalvm.js)
}

// Add server+storage full runtime classpaths onto the test compile and runtime classpaths.
// afterEvaluate mirrors the approach in storage-plugin-conventions (see comments there).
afterEvaluate {
    sourceSets["test"].compileClasspath += backendProvided
    sourceSets["test"].runtimeClasspath += backendProvided
}

tasks.register<Test>("fullBackendTest") {
    description = "Runs the heavy @FullBackendTest integration tests (requires Docker)."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags("full-backend-test") }
    jvmArgs("-Djava.awt.headless=true")
}

// ---------------------------------------------------------------------------
// Tag exclusion: @FullBackendTest is a composed annotation defined as
// @Tag("full-backend-test") @Test in graylog2-server's test sources.
// Excluding this tag from the default test task ensures ./gradlew check
// does NOT run the heavy containerized tests. Mirrors Maven's
// -DexcludedGroups=full-backend-test flag used in CI.
// The tests remain opt-in: run with --tests or a separate task that
// re-enables the tag filter.
// ---------------------------------------------------------------------------
testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter()
            targets.all {
                testTask.configure {
                    useJUnitPlatform {
                        excludeTags("full-backend-test")
                    }
                    jvmArgs("-Djava.awt.headless=true")
                }
            }
        }
    }
}
