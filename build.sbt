name := "New-York-accident-rate_v.ananase"
version := "0.1"
scalaVersion := "2.12.8"

mainClass in (Compile, run) := Some("com.itechart.ny_accidents.Application")

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.0.9",
  "com.vividsolutions" % "jts" % "1.13",
  "com.typesafe.slick" %% "slick" % "3.2.1",
  "com.github.tminglei" %% "slick-pg" % "0.15.3",
  "com.github.tminglei" %% "slick-pg_play-json" % "0.15.3",
  "com.github.tminglei" %% "slick-pg_jts" % "0.15.3",
  "com.typesafe" % "config" % "1.2.1",
  "com.github.tototoshi" %% "scala-csv" % "1.3.5",
  "org.apache.spark" %% "spark-core" % "2.4.3" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided",
  "org.scalactic" %% "scalactic" % "3.0.5" % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.scalamock" %% "scalamock" % "4.1.0" % Test,
  "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test,
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.6.0",
  "net.codingwell" %% "scala-guice" % "4.2.3",
  "net.ruippeixotog" %% "scala-scraper" % "2.1.0",
  "redis.clients" % "jedis" % "2.9.0",
  "org.ehcache" % "ehcache" % "3.7.1",
  "javax.cache" % "cache-api" % "1.0.0",
  "com.typesafe.play" %% "play-json" % "2.7.3",
  "info.debatty" % "java-string-similarity" % "1.2.1",
  "org.slf4j" % "slf4j-api" % "1.7.26",
  "org.slf4j" % "slf4j-nop" % "1.7.26" % Test,
)
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1"
)

enablePlugins(ScoverageSbtPlugin)
enablePlugins(FlywayPlugin)
flywayDriver := "org.postgresql.Driver"
flywayUrl := "jdbc:postgresql://localhost:5432/ny_data"
flywayUser := "postgres" // Replace with your postgres login & password
flywayPassword := "postgres"
flywayLocations := Seq("filesystem:" + (resourceDirectory in Compile).value + "/db/migration/")
