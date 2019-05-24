addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.2.2")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")
addSbtPlugin("uk.co.randomcoding" % "sbt-git-hooks" % "0.2.0")

resolvers += "Flyway" at "https://davidmweber.github.io/flyway-sbt.repo"
addSbtPlugin("io.github.davidmweber" % "flyway-sbt" % "5.2.0")
