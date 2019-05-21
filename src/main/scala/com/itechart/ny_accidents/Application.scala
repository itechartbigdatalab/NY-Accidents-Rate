package com.itechart.ny_accidents

import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.database.dao.cache.EhCacheDAO
import com.itechart.ny_accidents.entity.{Accident, MergedData}
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.spark.Spark
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD

import scala.concurrent.ExecutionContext

object Application extends App {
  sys.addShutdownHook(EhCacheDAO.close())

  private implicit val ec: ExecutionContext = ExecutionContext.global
  val filesConfig = ConfigFactory.load("app.conf")
  val pathToDataFolder = filesConfig.getString("file.inputPath")
  val inputFileAccidents = pathToDataFolder + filesConfig.getString("file.input.inputFileNYAccidents")
  val accidentsParser = new AccidentsParser

  val rawData = Spark.sc.parallelize(accidentsParser.readData(inputFileAccidents))
  println("Raw data read")
  val mergedData: RDD[MergedData] = MergeService
    .mergeAccidentsWithWeatherAndDistricts[Accident, MergedData](rawData, MergeService.mapper).cache()
  val count = mergedData.count()
  println("MERGED DATA SIZE: " + count)
}
