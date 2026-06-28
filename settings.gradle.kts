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

include(":graylog-storage-elasticsearch7")
include(":graylog-storage-opensearch2")
include(":graylog-storage-opensearch3")
// Project dirs match their names at repo root — no projectDir override needed.

include(":full-backend-tests")

include(":graylog2-web-interface")
// projectDir matches the directory name by convention; the explicit line is kept
// for clarity alongside the other explicit overrides in this file.
project(":graylog2-web-interface").projectDir = file("graylog2-web-interface")
