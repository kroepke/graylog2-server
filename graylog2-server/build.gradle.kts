plugins {
    id("graylog.java-conventions")
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
    // ANTLR runtime
    // =========================================================
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
}
