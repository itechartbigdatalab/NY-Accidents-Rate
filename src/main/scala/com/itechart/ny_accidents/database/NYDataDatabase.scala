package com.itechart.ny_accidents.database

import com.itechart.ny_accidents.constants.Configuration
import com.typesafe.config.ConfigFactory
import slick.jdbc
import slick.jdbc.JdbcBackend

object NYDataDatabase {
  implicit val database: jdbc.JdbcBackend.DatabaseDef = JdbcBackend.Database.forURL(
    url = Configuration.NY_DATA_DATABASE_URL,
    driver = Configuration.NY_DATA_DATABASE_DRIVER,
    user = Configuration.NY_DATA_DATABASE_USER,
    password = Configuration.NY_DATA_DATABASE_PASSWORD)
}
