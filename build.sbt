lazy val root = (project in file("."))
.settings(
  name := "SmartCityAnalytics",
  version := "0.1.0",
  scalaVersion := "2.12.18",

  libraryDependencies ++= Seq(
    // Dépendances Spark (core + SQL)
    "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
    "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided",

    // Typesafe Config pour gérer config.conf
    "com.typesafe" % "config" % "1.4.3"
  )
)