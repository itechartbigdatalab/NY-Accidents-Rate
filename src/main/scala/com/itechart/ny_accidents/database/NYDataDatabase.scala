package com.itechart.ny_accidents.database

import slick.jdbc
import slick.jdbc.JdbcBackend

object NYDataDatabase {
  implicit val database: jdbc.JdbcBackend.Database = JdbcBackend.Database.forConfig("postgreConf")
}
