val sparkVersion = "1.6.0-cdh5.13.0"
val scalaTestVersion = "3.2.0"
val scoptVersion = "3.3.0"
val clouderaRepoUrl = "https://repository.cloudera.com/artifactory/cloudera-repos/"

lazy val commonSettings = Seq(

  scalaVersion := "2.10.5",
  scalacOptions ++= "-encoding" :: "UTF-8" :: "-target:jvm-1.7" :: Nil,
  resolvers += "ClouderaRepo" at clouderaRepoUrl,

  // Dependencies
  libraryDependencies ++= "org.apache.spark" %% "spark-core" % sparkVersion % "provided" ::
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" ::
    "org.scalactic" %% "scalactic" % scalaTestVersion ::
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test" :: Nil
)

lazy val auroraDataload = (project in file("."))
  .settings(

    commonSettings,
    name := "aurora-dataload",
    version := "0.0.2",
    libraryDependencies ++= "com.github.scopt" %% "scopt" % scoptVersion :: Nil,

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