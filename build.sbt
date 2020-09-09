val sparkVersion = "2.2.3"
val scalaTestVersion = "3.0.0"
val scoptVersion = "3.3.0"
val mySqlConnectorVersion = "5.1.45"

lazy val auroraScala = (project in file("."))
  .settings(

    name := "aurora_scala",
    version := "0.0.1",
    scalaVersion := "2.11.8",
    scalacOptions ++= Seq(

      "-encoding", "UTF-8"),

    libraryDependencies ++= Seq(

      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "com.github.scopt" %% "scopt" % scoptVersion,
      "org.scalactic" %% "scalactic" % scalaTestVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "mysql" % "mysql-connector-java" % mySqlConnectorVersion % "provided"),

      (unmanagedResources in Compile) := (unmanagedResources in Compile)
      .value
      .filterNot(_.getName.endsWith(".properties")),

    assemblyJarName in assembly := s"${name.value}_${version.value}.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x) }
  )