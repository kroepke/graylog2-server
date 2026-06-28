plugins { id("graylog.storage-plugin-conventions") }

val libs = the<org.gradle.accessors.dm.LibrariesForLibs>()

dependencies {
    // =========================================================
    // guava: <scope>provided</scope> in Maven → compileOnly
    // Note: os3 POM uses provided for both guava AND graylog2-server;
    // server dep is already declared as compileOnly in the convention plugin.
    // =========================================================
    compileOnly(libs.guava)

    // =========================================================
    // OpenSearch Java client (official opensearch-project/opensearch-java client)
    // commons-logging excluded (matches Maven POM <exclusions>)
    // Version: opensearch.client.version = 3.2.0 (from graylog-parent POM)
    // =========================================================
    implementation(libs.opensearch.java.client) {
        exclude(group = "commons-logging", module = "commons-logging")
    }

    // =========================================================
    // Apache HttpComponents 5 (from graylog-parent dependencyManagement;
    // apache-httpclient5.version = 5.5.1)
    // =========================================================
    implementation(libs.apache.httpclient5)

    // =========================================================
    // JJWT (version from graylog-parent dependencyManagement → jjwt.version = 0.13.0)
    // =========================================================
    implementation(libs.jjwt.api)
    implementation(libs.jjwt.impl)
    runtimeOnly(libs.jjwt.jackson)   // <scope>runtime</scope> in Maven POM

    // =========================================================
    // Jackson (version from jackson-bom platform added in convention plugin)
    // =========================================================
    implementation(libs.jackson.databind)

    // =========================================================
    // Jakarta annotation API (explicit in os3 POM; version = 3.0.0 from catalog)
    // =========================================================
    implementation(libs.jakarta.annotation.api)

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
}
