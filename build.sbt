// Dependencies version
val sparkVersion = "1.6.0-cdh5.13.0"
val scalaTestVersion = "3.2.0"
val scoptVersion = "3.3.0"
val jsqlParserVersion = "4.0"
val scalaMockVersion = "4.1.0"
val poiVersion = "3.17"

// Additional repositories
val clouderaRepoUrl = "https://repository.cloudera.com/artifactory/cloudera-repos/"

lazy val commonSettings = Seq(

  scalaVersion := "2.10.5",
  javacOptions ++= "-source" :: "1.7" ::
    "-target" :: "1.7" ::
    Nil,

  scalacOptions ++= "-encoding" :: "UTF-8" ::
    "-target:jvm-1.7" ::
    "-feature" :: "-language:implicitConversions" ::
    Nil,

  resolvers += "ClouderaRepo" at clouderaRepoUrl,

  // Dependencies
  libraryDependencies ++= "org.apache.spark" %% "spark-core" % sparkVersion % Provided ::
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided ::
    "org.scalactic" %% "scalactic" % scalaTestVersion ::
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test ::
    "org.scalamock" %% "scalamock" % scalaMockVersion % Test ::
    Nil
)

lazy val auroraDataload = (project in file("."))
  .settings(

    commonSettings,
    name := "aurora-dataload",
    version := "0.3.0",
    libraryDependencies ++= "org.apache.spark" %% "spark-hive" % sparkVersion % Provided ::
      "com.github.scopt" %% "scopt" % scoptVersion ::
      Nil,

    // Exclude .properties file from packaging
    (unmanagedResources in Compile) := (unmanagedResources in Compile)
      .value.filterNot(_.getName.endsWith(".properties")),

    // Output .jar name
    assemblyJarName in assembly := s"${name.value}-${version.value}.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x) }
  )
  .dependsOn(logging, excelParser, sqlParser)
  .aggregate(logging, excelParser, sqlParser)

lazy val sqlParser = (project in file("sql-parser"))
  .settings(

    commonSettings,
    name := "sql-parser",
    libraryDependencies ++= "com.github.jsqlparser" % "jsqlparser" % jsqlParserVersion ::
      Nil
  ).dependsOn(logging)

lazy val excelParser = (project in file("excel-parser"))
  .settings(

    commonSettings,
    name := "excel-parser",
    libraryDependencies ++= "org.apache.poi" % "poi" % poiVersion ::
      "org.apache.poi" % "poi-ooxml" % poiVersion ::
      Nil
  ).dependsOn(logging)

lazy val logging = (project in file("logging"))
  .settings(commonSettings)