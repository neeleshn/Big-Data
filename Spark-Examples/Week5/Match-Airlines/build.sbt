
lazy val root = (project in file(".")).
    settings(
        name := "Carrier Match",
        libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.2")
    )
