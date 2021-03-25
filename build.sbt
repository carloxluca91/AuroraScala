// Dependencies version
val sparkVersion = "1.6.0-cdh5.13.0"
val scalaTestVersion = "3.2.0"
val scoptVersion = "3.3.0"
val jsqlParserVersion = "4.0"
val grizzledVersion = "1.3.0"
val poiVersion = "3.17"

// Additional repositories
val clouderaRepoUrl = "https://repository.cloudera.com/artifactory/cloudera-repos/"

lazy val commonSettings = Seq(

  javacOptions ++= "-source" :: "1.8" :: "-target" :: "1.7" :: Nil,
  scalacOptions ++= "-encoding" :: "UTF-8" :: Nil,
  scalaVersion := "2.10.5",
  resolvers += "ClouderaRepo" at clouderaRepoUrl,

  // Dependencies
  libraryDependencies ++= "org.apache.spark" %% "spark-core" % sparkVersion :: // % "provided" ::
    "org.apache.spark" %% "spark-sql" % sparkVersion :: // % "provided" ::
    "org.scalactic" %% "scalactic" % scalaTestVersion ::
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test" ::
    "org.clapper" %% "grizzled-slf4j" % grizzledVersion :: Nil
)

lazy val auroraDataload = (project in file("."))
  .settings(

    commonSettings,
    name := "aurora-dataload",
    version := "0.3.0",
    libraryDependencies ++= "org.apache.spark" %% "spark-hive" % sparkVersion % "provided" ::
      "com.github.scopt" %% "scopt" % scoptVersion :: Nil,

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
  .dependsOn(sqlParser, excelParser)

lazy val sqlParser = (project in file("sql-parser"))
  .settings(

    commonSettings,
    name := "sql-parser",
    libraryDependencies ++= "com.github.jsqlparser" % "jsqlparser" % jsqlParserVersion :: Nil
  )

lazy val excelParser = (project in file("excel-parser"))
  .settings(

    commonSettings,
    name := "excel-parser",
    libraryDependencies ++= "org.apache.poi" % "poi" % poiVersion ::
      "org.apache.poi" % "poi-ooxml" % poiVersion :: Nil
  )