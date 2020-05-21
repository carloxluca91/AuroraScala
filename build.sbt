val sparkVersion = "1.6.0"

lazy val auroraScala = (project in file("."))
  .settings(

    name := "aurora_scala",
    version := "1.0",
    scalaVersion := "2.10.5",
    scalacOptions ++= Seq(

      "-encoding", "UTF-8"
    ),

    libraryDependencies += ("org.apache.spark" %% "spark-core" % sparkVersion % "provided"),
    libraryDependencies += ("org.apache.spark" %% "spark-sql" % sparkVersion % "provided"),
    libraryDependencies += ("org.apache.spark" %% "spark-hive" % sparkVersion % "provided"),
    libraryDependencies += ("com.github.scopt" %% "scopt" % "3.3.0"),

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