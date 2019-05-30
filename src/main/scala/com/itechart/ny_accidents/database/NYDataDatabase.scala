package com.itechart.ny_accidents.database

import com.itechart.ny_accidents.constants.Configuration._
import org.apache.spark.sql.{DataFrame, SaveMode}
import slick.jdbc
import slick.jdbc.JdbcBackend

object NYDataDatabase {
  implicit val database: jdbc.JdbcBackend.Database = JdbcBackend.Database.forConfig("postgreConf")

  def insertDataFrame(tableName: String, data: DataFrame): Unit = {
    data.write
      .format("jdbc")
      .option("url", NY_DATA_DATABASE_URL)
      .option("dbtable", tableName)
      .option("user", NY_DATA_DATABASE_USER)
      .option("password", NY_DATA_DATABASE_PASSWORD)
      .mode(SaveMode.Append)
      .save()
  }
}
