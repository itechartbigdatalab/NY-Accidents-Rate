package com.itechart.ny_accidents.database

import com.itechart.ny_accidents.entity.District
import com.itechart.ny_accidents.parse.DistrictsParser
import com.itechart.ny_accidents.spark.Spark
import com.typesafe.config.ConfigFactory


object DistrictsStorage {
  private lazy val pathToDataFolder = ConfigFactory.load("app.conf")
    .getString("file.nynta_path")

  lazy val districts: Seq[District] = new DistrictsParser()
    .parseCsv(pathToDataFolder, Spark.sparkSql)
    .filter(_.isDefined).map(_.get)
}