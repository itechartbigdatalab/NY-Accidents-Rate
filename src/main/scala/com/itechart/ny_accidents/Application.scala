package com.itechart.ny_accidents

import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.spark.Spark
import com.typesafe.config.ConfigFactory

object Application extends App {

  val filesConfig = ConfigFactory.load("app.conf")
  val pathToDataFolder = filesConfig.getString("file.inputPath")
  val inputFileAccidents = pathToDataFolder + filesConfig.getString("file.input.inputFileNYAccidents")
  val accidentsParser = new AccidentsParser

  val raws = accidentsParser.readData(inputFileAccidents).cache()
  val mergedData = MergeService.mergeAccidentsWithWeatherAndDistricts(raws)

  println(mergedData.count() + "KISAKISA")
}
