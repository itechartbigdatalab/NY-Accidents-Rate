name := "New-York-accident-rate_v.ananase"
version := "0.1"
scalaVersion := "2.12.8"

mainClass in (Compile, run) := Some("com.itechart.ny_accidents.application.Application")

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.9"

libraryDependencies += "org.postgresql" % "postgresql" % "42.1.4"
libraryDependencies += "com.vividsolutions" % "jts" % "1.13"
libraryDependencies += "com.typesafe.slick" %% "slick" % "3.3.0"
libraryDependencies += "com.github.tminglei" %% "slick-pg" % "0.15.3"
libraryDependencies += "com.github.tminglei" %% "slick-pg_play-json" % "0.15.3"
libraryDependencies += "com.github.tminglei" %% "slick-pg_jts" % "0.17.2"
libraryDependencies += "com.typesafe" % "config" % "1.3.3"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.8" % "provided"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.8" % "provided"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.9.8" % "provided"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

enablePlugins(FlywayPlugin)
flywayDriver := "org.postgresql.Driver"
flywayUrl := "jdbc:postgresql://localhost:5432/ny_data"
flywayUser := "postgres" // Replace with your postgres login & password
flywayPassword := "postgres"
flywayLocations := Seq("filesystem:" + (resourceDirectory in Compile).value + "/db/migration/")
