package com.itechart.ny_accidents

import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.database.dao.cache.EhCacheDAO
import com.itechart.ny_accidents.entity.{Accident, MergedData}
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.spark.Spark
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

object Application extends App {
  sys.addShutdownHook(EhCacheDAO.close())
  private lazy val logger = LoggerFactory.getLogger(getClass)

  private implicit val ec: ExecutionContext = ExecutionContext.global
  val filesConfig = ConfigFactory.load("app.conf")
  val pathToDataFolder = filesConfig.getString("file.inputPath")
  val inputFileAccidents = pathToDataFolder + filesConfig.getString("file.input.inputFileNYAccidents")
//  val inputFileAccidents = "/home/uladzimir/Downloads/NYPD_Motor_Vehicle_Collisions.csv"
  val accidentsParser = new AccidentsParser

  val rawData = Spark.sc.parallelize(accidentsParser.readData(inputFileAccidents)).cache()
  println("Raw data read")
  val mergedData: RDD[(MergedData, Boolean)] = MergeService
    .mergeAccidentsWithWeatherAndDistricts[Accident, (MergedData, Boolean)](rawData, MergeService.mapper).cache()
  val count = mergedData.count()

  val inCache = mergedData.filter(_._2 == true).count()
  val notInCache = mergedData.filter(_._2 == false).count()
  logger.info("====================STATS==================")
  logger.info("IN CACHE: " + inCache)
  logger.info("NOT IN CACHE: " + notInCache)
  logger.info("===========================================")
}
