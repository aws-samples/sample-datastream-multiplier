import sbt.Keys.javacOptions

lazy val root = (project in file(".")).
  settings(
    name := "stream-multiplier-demo",
    version := "1.0",
    scalaVersion := "2.12.20",
    mainClass := Some("com.amazonaws.services.msf.DataMultiplierJob"),
    javacOptions ++= Seq("-source", "17", "-target", "17"),
    // Flink 2.x defaults to Java 17; compile Scala to the matching bytecode target
    scalacOptions ++= Seq("-release", "17")
  )

val jarName = "DataStreamGeneratorDemo-flink_2.3.jar"
val flinkVersion = "2.3.0"
val msfRuntimeVersion = "1.2.0"
val kinesisConnectorVersion = "6.0.1-2.0"
val log4jVersion = "2.24.3"
val fakerVersion = "2.7.0"
val jacksonVersion = "2.15.2"

libraryDependencies ++= Seq(
  // MSF Runtime
  "com.amazonaws" % "aws-kinesisanalytics-runtime" % msfRuntimeVersion,

  // Flink core - provided scope
  // Note: Flink 2.x removed the Scala APIs (flink-streaming-scala). This app uses
  // the Java DataStream APIs directly, so only the Java artifacts are needed.
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % "provided",
  "org.apache.flink" % "flink-connector-base" % flinkVersion % "provided",
  "org.apache.flink" % "flink-clients" % flinkVersion % "provided",

  // Kinesis source + sink connector - compile scope (MUST be included)
  // Flink 2.x: legacy flink-connector-kinesis is replaced by flink-connector-aws-kinesis-streams
  "org.apache.flink" % "flink-connector-aws-kinesis-streams" % kinesisConnectorVersion,

  // Data generation
  "net.datafaker" % "datafaker" % fakerVersion,

  // Jackson
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion,

  // Logging - provided
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion % "provided",
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion % "provided",
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion % "provided",

  // Tests
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.apache.flink" % "flink-test-utils" % flinkVersion % Test,
  "org.apache.flink" % "flink-test-utils-junit" % flinkVersion % Test,
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % Test classifier "tests",
)

artifactName := { (_: ScalaVersion, _: ModuleID, _: Artifact) => jarName }

assembly / assemblyJarName := jarName

// Managed Service for Apache Flink resolves the job entry point from the JAR
// manifest, so Main-Class must be set explicitly for the assembled artifact.
// The project-level mainClass setting above does not reach the assembly task,
// and without this the application fails to start with
// "Neither a 'Main-Class', nor a 'program-class' entry was found in the jar file".
assembly / mainClass := Some("com.amazonaws.services.msf.DataMultiplierJob")

assembly / packageOptions += Package.ManifestAttributes(
  "Main-Class" -> "com.amazonaws.services.msf.DataMultiplierJob"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "module-info.class" => MergeStrategy.discard
  case x if x.contains("module-info") => MergeStrategy.discard
  case "flink-application-properties-dev.json" => MergeStrategy.first
  case "faker-config.yaml" => MergeStrategy.first
  case x if x.endsWith(".json") => MergeStrategy.first
  case x if x.endsWith(".yaml") => MergeStrategy.first
  case x if x.endsWith(".yml") => MergeStrategy.first
  case _ => MergeStrategy.first
}

// CRITICAL: Removed scala-library exclusion - it's needed for your application
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp.filter { jar =>
    val name = jar.data.getName
    // Only exclude Flink runtime JARs (provided by the Flink 2.x runtime),
    // NOT scala-library or the kinesis connector.
    // flink-scala / flink-streaming-scala no longer exist in Flink 2.x.
    name.startsWith("flink-dist") ||
      name.startsWith("flink-streaming-java-") ||
      name.startsWith("flink-clients-") ||
      name.startsWith("flink-connector-base-") ||
      name.startsWith("log4j-")
  }
}