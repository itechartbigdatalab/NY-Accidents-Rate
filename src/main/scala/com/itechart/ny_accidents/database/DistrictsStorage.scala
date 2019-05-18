package com.itechart.ny_accidents.database

import com.itechart.ny_accidents.database.dao.DistrictsDAO
import com.itechart.ny_accidents.entity.District
import com.itechart.ny_accidents.parse.DistrictsParser
import com.itechart.ny_accidents.spark.Spark
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration


object DistrictsStorage {
  private lazy val pathToDataFolder = ConfigFactory.load("app.conf")
    .getString("file.nynta_path")

  lazy val districts: Seq[District] = Await.result(NYDataDatabase.database.run(new DistrictsDAO().all()), Duration.Inf)
}