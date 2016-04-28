
lazy val root = (project in file(".")).
    settings(
        name := "Query",
        libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.2")
    )
