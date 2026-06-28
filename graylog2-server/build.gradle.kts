plugins {
    id("graylog.java-conventions")
    id("com.google.protobuf") version "0.9.4"
    antlr
    `jvm-test-suite`
}

// Mockito as a javaagent (Java 21 inline-mock requirement), resolved to a single jar file.
// isTransitive = false so only the agent jar itself is in the configuration — no deps pulled in.
// Version is pinned explicitly using the catalog version (same as bom-mockito) because BOM
// platforms don't apply version constraints when transitive resolution is disabled.
val mockitoAgent: Configuration by configurations.creating { isTransitive = false }

dependencies {
    // Pin same mockito-core version as bom-mockito (5.23.0 from graylog-parent mockito.version).
    // Using a literal version because the `libs` accessor is declared after this block in the script.
    mockitoAgent("org.mockito:mockito-core:5.23.0")
}

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter()
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

// ANTLR4: grammar is under src/main/antlr4 (Gradle plugin default is src/main/antlr).
sourceSets.main {
    antlr {
        setSrcDirs(listOf("src/main/antlr4"))
    }
}

tasks.generateGrammarSource {
    // Pass -package so generated files have the correct package declaration to match Maven output.
    // Gradle already sets -o to the per-grammar subdirectory based on the relative path from
    // src/main/antlr4, so ANTLR4 places files in the correct location without creating an
    // additional package subdirectory (ANTLR4 ignores -package for directory layout).
    arguments = arguments + listOf("-package", "org.graylog.plugins.pipelineprocessor.parser")
}

// Dedicated source set for OpAMP protos (mirrors Maven's separate <execution>).
// Setting the proto srcDir to src/main/proto/opamp/proto makes that directory the
// import root, so `import "anyvalue.proto"` in opamp.proto resolves correctly.
// NOTE: this source set exists ONLY to drive a separate protoc invocation with its
// own import root. Its generated Java is routed into the MAIN source set below so the
// `opamp.proto.*` classes are part of the graylog2-server main module (as in Maven).
val opamp = sourceSets.create("opamp") {
    (extensions.getByName("proto") as SourceDirectorySet).setSrcDirs(
        listOf(file("src/main/proto/opamp/proto"))
    )
}

// Exclude opamp/** from the main proto source set so those files are NOT passed to
// the main protoc invocation (they are compiled by the dedicated opamp task above).
sourceSets.main {
    (extensions.getByName("proto") as SourceDirectorySet).exclude("opamp/**")
    // Route the OpAMP-generated Java/gRPC sources into MAIN compilation so that
    // package `opamp.proto.*` resolves from main code. The protobuf plugin writes
    // these under build/generated/source/proto/opamp/{java,grpc}.
    java.srcDir(layout.buildDirectory.dir("generated/source/proto/opamp/java"))
    java.srcDir(layout.buildDirectory.dir("generated/source/proto/opamp/grpc"))
}

// Prevent the opamp source set from ALSO compiling its generated Java (it would
// otherwise produce competing class files via compileOpampJava). We keep only the
// proto-generation task (generateOpampProto) from that source set; the Java is
// compiled exclusively by compileJava (main). This avoids duplicate-class output.
tasks.named("compileOpampJava") { enabled = false }

// Generated OpAMP sources must exist before main compilation runs. Adding the
// generated dirs to main.java.srcDirs does not, by itself, create the task ordering,
// so wire compileJava explicitly to depend on the OpAMP proto generation task.
tasks.named("compileJava") {
    dependsOn("generateOpampProto")
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

// =========================================================
// Swagger/OpenAPI generation task
// Mirrors the Maven exec-maven-plugin execution generate-swagger-api-definition.
// =========================================================
val generateApiDefinition by tasks.registering(JavaExec::class) {
    dependsOn(tasks.compileJava, tasks.processResources)
    mainClass.set("org.graylog.api.GenerateApiDefinition")
    classpath = sourceSets.main.get().runtimeClasspath
    val outDir = layout.buildDirectory.dir("swagger")
    args(outDir.get().asFile.absolutePath, "org.graylog", "org.graylog2")
    // Declared I/O so Gradle can skip this task when nothing has changed.
    inputs.files(sourceSets.main.get().output)
    outputs.dir(outDir)
}

val libs = the<org.gradle.accessors.dm.LibrariesForLibs>()

dependencies {
    // =========================================================
    // BOM platforms — mirror graylog-parent + graylog-project-parent imports
    // =========================================================
    implementation(platform(libs.bom.log4j))
    implementation(platform(libs.bom.netty))
    implementation(platform(libs.bom.jackson))
    implementation(platform(libs.bom.grpc))
    implementation(platform(libs.bom.awssdk))
    implementation(platform(libs.bom.awssdk1))
    implementation(platform(libs.bom.protobuf))
    implementation(platform(libs.bom.okhttp))
    implementation(platform(libs.bom.metrics))
    implementation(platform(libs.bom.guice))
    implementation(platform(libs.bom.hk2))
    implementation(platform(libs.bom.jersey))
    implementation(platform(libs.bom.prometheus))
    implementation(platform(libs.bom.opentelemetry))
    implementation(platform(libs.bom.mcp))
    testImplementation(platform(libs.bom.junit))
    testImplementation(platform(libs.bom.mockito))
    testImplementation(platform(libs.bom.testcontainers))

    // =========================================================
    // OpenTelemetry
    // =========================================================
    implementation(libs.opentelemetry.api)
    implementation(libs.opentelemetry.instrumentation.annotations)

    // =========================================================
    // CLI
    // =========================================================
    implementation(libs.airline)

    // =========================================================
    // Google (Guava, Guice)
    // =========================================================
    implementation(libs.guava)
    implementation(libs.caffeine)
    implementation(libs.guava.retrying)
    implementation(libs.guice)
    implementation(libs.guice.assistedinject)

    // =========================================================
    // Security / Auth
    // =========================================================
    implementation(libs.shiro.core)
    implementation(libs.jjwt.api)
    implementation(libs.jjwt.impl)
    runtimeOnly(libs.jjwt.jackson)
    implementation(libs.jbcrypt)
    implementation(libs.bouncycastle.bcpkix)

    // =========================================================
    // Configuration (jadconfig is also wired as annotation processor
    // in the convention plugin — add as implementation for runtime)
    // =========================================================
    implementation(libs.jadconfig)

    // =========================================================
    // Jakarta / DI
    // =========================================================
    implementation(libs.jakarta.inject.api)
    implementation(libs.jakarta.annotation.api)
    implementation(libs.jakarta.validation.api)
    implementation(libs.jakarta.ws.rs.api)
    implementation(libs.javax.annotation.api)
    implementation(libs.javax.xml.bind.api)
    // provided scope in Maven: annotation-only jar; processor already in convention plugin
    compileOnly(libs.google.auto.value.annotations)

    // =========================================================
    // MongoDB
    // =========================================================
    implementation(libs.mongodb.driver.sync)
    implementation(libs.mongodb.driver.legacy)
    implementation(libs.mongojack)

    // =========================================================
    // OkHttp
    // =========================================================
    implementation(libs.okhttp.jvm)
    testImplementation(libs.okhttp.tls)
    testImplementation(libs.okhttp.mockwebserver3)

    // =========================================================
    // Jackson
    // =========================================================
    implementation(libs.jackson.core)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.annotations)
    implementation(libs.jackson.jakarta.rs.base)
    implementation(libs.jackson.jakarta.rs.json.provider)
    implementation(libs.jackson.datatype.guava)
    implementation(libs.jackson.datatype.jdk8)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.jackson.datatype.joda)
    implementation(libs.jackson.module.json.schema.jakarta)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.jackson.dataformat.xml)
    implementation(libs.jackson.dataformat.csv)

    // =========================================================
    // Apache POI
    // =========================================================
    implementation(libs.poi.ooxml)

    // =========================================================
    // Dropwizard Metrics
    // =========================================================
    implementation(libs.metrics.annotation)
    implementation(libs.metrics.core)
    implementation(libs.metrics.log4j2)
    implementation(libs.metrics.jvm)
    implementation(libs.metrics.jmx)
    implementation(libs.metrics.json)

    // =========================================================
    // Prometheus
    // =========================================================
    implementation(libs.prometheus.dropwizard)
    implementation(libs.prometheus.hotspot)
    implementation(libs.prometheus.httpserver)

    // =========================================================
    // Crypto
    // =========================================================
    implementation(libs.siv.mode)

    // =========================================================
    // Apache Commons
    // =========================================================
    implementation(libs.commons.email)
    implementation(libs.commons.validator)
    implementation(libs.commons.lang3)
    implementation(libs.commons.io)
    implementation(libs.commons.codec)
    implementation(libs.commons.net)
    implementation(libs.commons.csv)

    // =========================================================
    // Jersey
    // =========================================================
    implementation(libs.jersey.hk2)
    implementation(libs.jersey.bean.validation)
    implementation(libs.jersey.media.multipart)
    implementation(libs.jersey.container.grizzly2.http)

    // =========================================================
    // Grizzly / HK2
    // =========================================================
    implementation(libs.grizzly.websockets)
    implementation(libs.hk2.guice.bridge)
    implementation(libs.hk2.api)
    implementation(libs.hk2.locator)

    // =========================================================
    // Hibernate Validator
    // =========================================================
    implementation(libs.hibernate.validator)

    // =========================================================
    // Reflections / Classgraph
    // =========================================================
    implementation(libs.reflections)
    implementation(libs.classgraph)

    // =========================================================
    // CSV
    // =========================================================
    implementation(libs.opencsv)

    // =========================================================
    // Date / Time
    // =========================================================
    implementation(libs.natty)
    implementation(libs.joda.time)
    implementation(libs.threeten.extra)

    // =========================================================
    // Misc utilities
    // =========================================================
    implementation(libs.jsoup)
    implementation(libs.jmte)
    implementation(libs.disruptor)
    implementation(libs.uuid.repack)
    implementation(libs.ulid)
    implementation(libs.jool)
    implementation(libs.freemarker)
    implementation(libs.asciitable)
    implementation(libs.streamex)
    implementation(libs.stateless4j)
    implementation(libs.json.path.lib)
    implementation(libs.zstd.jni)

    // =========================================================
    // Version parsing
    // =========================================================
    implementation(libs.java.semver)
    implementation(libs.semver4j)

    // =========================================================
    // Graylog / custom artifacts
    // =========================================================
    implementation(libs.grok)
    implementation(libs.gelfclient)
    implementation(libs.os.platform.finder)
    implementation(libs.kafka09.shaded)
    implementation(libs.syslog4j)
    implementation(libs.cef.parser)

    // =========================================================
    // Swagger
    // =========================================================
    implementation(libs.swagger.jaxrs2.jakarta)
    implementation(libs.swagger.parser)

    // =========================================================
    // JSON Schema generator
    // =========================================================
    implementation(libs.jsonschema.generator)
    implementation(libs.jsonschema.module.jackson)
    implementation(libs.jsonschema.module.jakarta.validation)

    // =========================================================
    // Logging
    // =========================================================
    implementation(libs.log4j.api)
    implementation(libs.log4j.core)
    implementation(libs.log4j.slf4j2.impl)
    implementation(libs.log4j.jul)
    implementation(libs.slf4j.api)
    implementation(libs.slf4j.jcl.over)
    implementation(libs.slf4j.log4j.over)

    // =========================================================
    // Protobuf / gRPC
    // =========================================================
    implementation(libs.protobuf.java)
    implementation(libs.protobuf.java.util)
    implementation(libs.grpc.netty)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.stub)
    implementation(libs.grpc.alts)
    implementation(libs.grpc.services)

    // =========================================================
    // MCP
    // =========================================================
    implementation(libs.mcp.core)
    implementation(libs.mcp.json.jackson2)

    // =========================================================
    // Netty (versions from bom-netty)
    // =========================================================
    implementation(libs.netty.common)
    implementation(libs.netty.buffer)
    implementation(libs.netty.handler)
    implementation(libs.netty.codec)
    implementation(libs.netty.codec.dns)
    implementation(libs.netty.codec.http)
    implementation(libs.netty.resolver.dns)
    implementation(libs.netty.transport.classes.epoll)
    implementation(libs.netty.transport.classes.kqueue)
    // Classified Netty native transports — version from bom-netty
    implementation("io.netty:netty-transport-native-epoll") { artifact { classifier = "linux-x86_64" } }
    implementation("io.netty:netty-transport-native-epoll") { artifact { classifier = "linux-aarch_64" } }
    implementation("io.netty:netty-transport-native-kqueue") { artifact { classifier = "osx-x86_64" } }
    implementation("io.netty:netty-transport-native-kqueue") { artifact { classifier = "osx-aarch_64" } }
    // Classified BoringSSL — version separately managed (not in bom-netty)
    val tcnativeVersion = libs.versions.nettyTcnative.get()
    implementation("io.netty:netty-tcnative-boringssl-static:$tcnativeVersion:osx-x86_64")
    implementation("io.netty:netty-tcnative-boringssl-static:$tcnativeVersion:osx-aarch_64")
    implementation("io.netty:netty-tcnative-boringssl-static:$tcnativeVersion:linux-x86_64")
    implementation("io.netty:netty-tcnative-boringssl-static:$tcnativeVersion:linux-aarch_64")

    // =========================================================
    // HdrHistogram / OSHI
    // =========================================================
    implementation(libs.hdr.histogram)
    implementation(libs.oshi.core)

    // =========================================================
    // Messaging
    // =========================================================
    implementation(libs.amqp.client)
    implementation(libs.kafka.clients)
    implementation(libs.aws.msk.iam.auth)

    // =========================================================
    // Lucene
    // =========================================================
    implementation(libs.lucene.queryparser)
    implementation(libs.lucene.analysis.common)

    // =========================================================
    // AWS SDK v2
    // =========================================================
    implementation(libs.awssdk.s3)
    implementation(libs.awssdk.sqs)
    implementation(libs.awssdk.apache.client)
    implementation(libs.awssdk.netty.nio.client)
    implementation(libs.awssdk.cloudwatchlogs)
    implementation(libs.awssdk.iam)
    implementation(libs.awssdk.sts)

    // =========================================================
    // AWS Kinesis
    // =========================================================
    implementation(libs.aws.kinesis.client)

    // =========================================================
    // AWS SDK v1
    // =========================================================
    implementation(libs.awssdk1.s3)
    implementation(libs.awssdk1.sqs)
    implementation(libs.awssdk1.ec2)
    implementation(libs.awssdk1.sts)

    // =========================================================
    // GCS / Azure
    // =========================================================
    implementation(libs.google.cloud.storage)
    implementation(libs.azure.storage.blob)

    // =========================================================
    // Retrofit
    // =========================================================
    implementation(libs.retrofit)
    implementation(libs.retrofit.converter.jackson)

    // =========================================================
    // LDAP
    // =========================================================
    implementation(libs.unboundid.ldapsdk)

    // =========================================================
    // Geo IP
    // =========================================================
    implementation(libs.geoip2)

    // =========================================================
    // ANTLR4 tool (codegen) + runtime
    // =========================================================
    antlr(libs.antlr)
    implementation(libs.antlr4.runtime)

    // =========================================================
    // Cron / Rate limiting
    // =========================================================
    implementation(libs.cron.utils)
    implementation(libs.rate.limited.logger)

    // =========================================================
    // Test infrastructure
    // =========================================================

    // JUnit 5 (from bom-junit)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.junit.platform.reporting)

    // Mockito (from bom-mockito)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit.jupiter)

    // AssertJ
    testImplementation(libs.assertj.core)
    testImplementation(libs.assertj.joda.time)

    // Awaitility
    testImplementation(libs.awaitility)

    // Equalsverifier / JSONassert
    testImplementation(libs.equalsverifier)
    testImplementation(libs.jsonassert.lib)

    // REST-Assured
    testImplementation(libs.rest.assured)
    testImplementation(libs.rest.assured.json.path)

    // pkts (network packet parsing)
    testImplementation(libs.pkts.core)

    // Guice JUnit 5 extension
    testImplementation(libs.guice.extension)

    // Testcontainers (from bom-testcontainers)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.elasticsearch)
    testImplementation(libs.testcontainers.rabbitmq)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.mongodb)

    // OpenSearch Testcontainers (explicit version)
    testImplementation(libs.opensearch.testcontainers)

    // Apache Directory API
    testImplementation(libs.apache.directory.api)
    testImplementation(libs.apacheds.test.framework)
    testImplementation(libs.apacheds.ldap.client.test)

    // Jersey test framework (from bom-jersey)
    testImplementation(libs.jersey.test.framework.core)
    testImplementation(libs.jersey.test.framework.provider.grizzly2)
    testImplementation(libs.jersey.test.framework.provider.inmemory)

    // =========================================================
    // Test annotation processors (mirrors main convention plugin setup;
    // test sources use @AutoValue, @AutoService, and JadConfig annotations)
    // Versions match the convention plugin exactly (auto-service 1.1.1,
    // auto-value 1.11.1, jadconfig 1.1.0 — all from graylog-parent / POMs).
    // =========================================================
    testCompileOnly("com.google.auto.service:auto-service:1.1.1")
    testAnnotationProcessor("com.google.auto.service:auto-service:1.1.1")
    testCompileOnly("com.google.auto.value:auto-value-annotations:1.11.1")
    testAnnotationProcessor("com.google.auto.value:auto-value:1.11.1")
    testAnnotationProcessor("org.graylog:jadconfig:1.1.0")
    testAnnotationProcessor("com.google.errorprone:error_prone_core:2.50.0")
}

// =========================================================
// Integration test task — opt-in, NOT wired into `check`.
// Reuses the already-compiled test source set output so no
// second compilation of src/test/java is needed.
// Run with: ./gradlew :graylog2-server:integrationTest --tests "<fqn>"
// =========================================================
val integrationTest by tasks.registering(Test::class) {
    description = "Runs integration tests (*IT / *IntegrationTest)."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    include("**/*IntegrationTest.class", "**/*IT.class")
    jvmArgs("-Djava.awt.headless=true")
    shouldRunAfter(tasks.named("test"))
}
