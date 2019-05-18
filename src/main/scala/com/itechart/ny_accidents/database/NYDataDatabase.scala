package com.itechart.ny_accidents.database

import com.typesafe.config.ConfigFactory
import slick.jdbc
import slick.jdbc.JdbcBackend

object NYDataDatabase {
  private lazy val config = ConfigFactory.parseResources("conf/district_database.conf")
  private lazy val dbUrl = config.getString("postgreConf.url")
  private lazy val dbDriver = config.getString("postgreConf.driver")
  private lazy val dbUser = config.getString("postgreConf.user")
  private lazy val dbPassword = config.getString("postgreConf.password")
  implicit val database: jdbc.JdbcBackend.DatabaseDef = JdbcBackend.Database.forURL(url = dbUrl,
    driver = dbDriver,
    user = dbUser,
    password = dbPassword)
}
