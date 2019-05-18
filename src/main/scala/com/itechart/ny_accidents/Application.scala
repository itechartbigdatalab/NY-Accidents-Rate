package com.itechart.ny_accidents

import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.service.metric.WeatherMetric
import com.typesafe.config.ConfigFactory

object Application extends App {


  val filesConfig = ConfigFactory.load("app.conf")
  val pathToDataFolder = filesConfig.getString("file.inputPath")
  val inputFileAccidents = pathToDataFolder + filesConfig.getString("file.input.inputFileNYAccidents")
  val accidentsParser = new AccidentsParser

  val raws = accidentsParser.readData(inputFileAccidents)
  val mergedData = MergeService.mergeAccidentsWithWeatherAndDistricts(raws).cache()

  val dayOfWeek = WeatherMetric.countDayOfWeek(mergedData)
  val hourOfDay = WeatherMetric.countHours(mergedData)
  val lightPeriods = WeatherMetric.definePeriod(mergedData)

  println(mergedData.count)
  println()
  dayOfWeek.foreach(println)
  println()
  hourOfDay.foreach(println)
  println()
  lightPeriods.foreach(println)
  println()

}
