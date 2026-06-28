plugins { id("graylog.storage-plugin-conventions") }

val libs = the<org.gradle.accessors.dm.LibrariesForLibs>()

dependencies {
    // =========================================================
    // guava: <scope>provided</scope> in Maven → compileOnly
    // =========================================================
    compileOnly(libs.guava)

    // =========================================================
    // OS2 shaded REST client libraries (compile scope in Maven POM)
    // commons-logging excluded (matches Maven POM <exclusions>)
    // Source: graylog-shaded project, published to Maven Central
    // Version: opensearch.shaded.version = 2.19.3-1 (from graylog-parent POM)
    // =========================================================
    implementation(libs.opensearch2.hlrc.shaded) {
        exclude(group = "commons-logging", module = "commons-logging")
    }
    implementation(libs.opensearch2.sniffer.shaded) {
        exclude(group = "commons-logging", module = "commons-logging")
    }

    // =========================================================
    // JJWT (version from graylog-parent dependencyManagement → jjwt.version = 0.13.0)
    // =========================================================
    implementation(libs.jjwt.api)
    implementation(libs.jjwt.impl)
    runtimeOnly(libs.jjwt.jackson)   // <scope>runtime</scope> in Maven POM

    // =========================================================
    // Test dependencies
    // Versions managed by BOM platforms added in storage-plugin-conventions.
    // =========================================================
    testImplementation(libs.opensearch.testcontainers)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit.jupiter)
    testImplementation(libs.assertj.core)
    testImplementation(libs.assertj.joda.time)
    testImplementation(libs.testcontainers.elasticsearch)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.junit.platform.reporting)
}
