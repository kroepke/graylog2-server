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

// Annotation processors declared as plain-string coordinates.
// LibrariesForLibs catalog access in buildSrc precompiled plugins requires non-trivial
// classpath wrangling (versionCatalogs in buildSrc/settings.gradle.kts + implementation(files(...))
// trick), which exceeds the brief's "couple of lines" threshold. Using plain strings instead
// (Option 2), which is always-works and explicitly endorsed by the plan.
dependencies {
    "compileOnly"("com.google.auto.service:auto-service:1.1.1")
    "annotationProcessor"("com.google.auto.service:auto-service:1.1.1")
    "compileOnly"("com.google.auto.value:auto-value:1.11.1")
    "annotationProcessor"("com.google.auto.value:auto-value:1.11.1")
    "compileOnly"("org.graylog:jadconfig:1.1.0") // jadconfig annotations
    "annotationProcessor"("org.graylog:jadconfig:1.1.0")
    // ErrorProne is wired as a processor path; full ErrorProne compiler integration
    // (the javac plugin) is out of scope for the prototype — processor path only.
    "annotationProcessor"("com.google.errorprone:error_prone_core:2.50.0")
}
