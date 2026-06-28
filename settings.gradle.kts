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
