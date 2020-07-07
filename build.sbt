val sparkVersion = "2.2.3"
val scalaTestVersion = "3.0.0"

lazy val auroraScala = (project in file("."))
  .settings(

    name := "aurora_scala",
    version := "0.0.1",
    scalaVersion := "2.11.8",
    scalacOptions ++= Seq(

      "-encoding", "UTF-8"
    ),

    resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases",

    libraryDependencies += ("org.apache.spark" %% "spark-core" % sparkVersion % "provided"),
    libraryDependencies += ("org.apache.spark" %% "spark-sql" % sparkVersion % "provided"),
    libraryDependencies += ("com.github.scopt" %% "scopt" % "3.3.0"),
    libraryDependencies += ("mysql" % "mysql-connector-java" % "5.1.47" % "provided"),
    libraryDependencies += ("org.scalactic" %% "scalactic" % scalaTestVersion),
    libraryDependencies += ("org.scalatest" %% "scalatest" % scalaTestVersion % "test"),

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