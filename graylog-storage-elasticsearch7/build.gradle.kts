plugins { id("graylog.storage-plugin-conventions") }

val libs = the<org.gradle.accessors.dm.LibrariesForLibs>()

dependencies {
    // =========================================================
    // guava: <scope>provided</scope> in Maven → compileOnly
    // Available transitively via the convention plugin's serverProvided configuration
    // (resolves server's JAVA_RUNTIME variant), but declared explicitly here so that
    // Gradle's dependency insight and locking show it as a direct compileOnly dep.
    // =========================================================
    compileOnly(libs.guava)

    // =========================================================
    // ES7 shaded client libraries (compile scope in Maven POM)
    // Source: graylog-shaded project, published to Maven Central
    // Version: elasticsearch.version = 7.9.1-0 (defined in this module's POM)
    // =========================================================
    implementation(libs.elasticsearch7.shaded)
    implementation(libs.elasticsearch7.sniffer.shaded)

    // =========================================================
    // Test dependencies
    // Versions managed by BOM platforms added in storage-plugin-conventions.
    // =========================================================
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit.jupiter)
    testImplementation(libs.assertj.core)
    testImplementation(libs.assertj.joda.time)
    testImplementation(libs.testcontainers.elasticsearch)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.junit.platform.reporting)
}
