import com.github.gradle.node.task.NodeTask
import com.github.gradle.node.yarn.task.YarnTask

plugins {
    id("com.github.node-gradle.node") version "7.1.0"
}

node {
    version.set("24.13.0")
    yarnVersion.set("1.22.22")
    download.set(true)
    // S3 CI cache that mirrors nodejs.org/dist — same URL Maven's frontend-maven-plugin uses.
    distBaseUrl.set("https://graylog-ci-cache.s3.eu-west-1.amazonaws.com/downloads/node/")
}

// =========================================================
// Swagger bridge
//
// The yarn script 'generate:apidefs' is hardcoded to read from
// ../graylog2-server/target/swagger (Maven's output dir).  Rather than
// copying the Gradle swagger output into Maven's target/, we invoke
// ts-node directly with an explicit source path pointing at the Gradle
// build dir.  The generator script (generate.ts) accepts exactly two
// positional args: <source-dir> <dest-dir>, so no source changes are needed.
// =========================================================
val serverSwaggerDir: Provider<Directory> =
    project(":graylog2-server").layout.buildDirectory.dir("swagger")

// =========================================================
// generateApiDefs — mirrors 'yarn generate:apidefs' but with explicit paths.
// Reads:  :graylog2-server:generateApiDefinition  →  build/swagger
// Writes: graylog2-web-interface/target/api
// =========================================================
val generateApiDefs by tasks.registering(NodeTask::class) {
    dependsOn(":graylog2-server:generateApiDefinition", tasks.named("yarn"))

    // ts-node's bin.js is a plain Node.js script — NodeTask runs:
    //   <managed-node> node_modules/ts-node/dist/bin.js <args>
    script.set(file("node_modules/ts-node/dist/bin.js"))

    // provider { } defers resolution to execution time → configuration-cache safe.
    args.set(provider {
        listOf(
            "src/generator/generate.ts",
            serverSwaggerDir.get().asFile.absolutePath,
            layout.projectDirectory.dir("target/api").asFile.absolutePath,
        )
    })

    // Up-to-date inputs: the swagger JSON files and the generator TypeScript sources.
    inputs.dir(serverSwaggerDir)
    inputs.files(
        "src/generator/generate.ts",
        "src/generator/parse.ts",
        "src/generator/emit.ts",
        "src/generator/Api.ts",
    )
    // Output: generated TypeScript API client files.
    outputs.dir(layout.projectDirectory.dir("target/api"))
}

// =========================================================
// yarnBuild — production webpack build.  Mirrors: yarn build
//   cross-env disable_plugins=true webpack --config webpack.bundled.ts
//
// Fine-grained I/O declarations ensure a second run is UP-TO-DATE and
// a Java-only change does NOT trigger webpack.
// =========================================================
val yarnBuild by tasks.registering(YarnTask::class) {
    dependsOn(tasks.named("yarn"), generateApiDefs)
    args.set(listOf("build"))

    // Inputs: all source that webpack processes.
    inputs.dir(layout.projectDirectory.dir("src")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.files("package.json", "yarn.lock")
    inputs.files(fileTree(layout.projectDirectory.dir("webpack"))
        .include("**/*.ts", "**/*.js", "**/*.json"))
    inputs.dir(layout.projectDirectory.dir("target/api")).withPathSensitivity(PathSensitivity.RELATIVE)

    // Output: the webpack bundle written by 'yarn build'.
    outputs.dir(layout.projectDirectory.dir("target/web/build"))
}

// =========================================================
// yarnTest — Jest unit tests, decoupled from Java tests.
// Run: ./gradlew :graylog2-web-interface:yarnTest
// =========================================================
val yarnTest by tasks.registering(YarnTask::class) {
    dependsOn(tasks.named("yarn"))
    args.set(listOf("test"))
    inputs.dir(layout.projectDirectory.dir("src")).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.files("package.json", "yarn.lock")
    // Jest writes no meaningful output dir; use a marker dir so reruns can be incremental.
    outputs.dir(layout.buildDirectory.dir("jest-marker"))
}
