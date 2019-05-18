package com.itechart.ny_accidents

import com.itechart.ny_accidents.parse.{AccidentsParser, SunriseHtmlParser}
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.service.metric.WeatherMetric
import com.typesafe.config.ConfigFactory

object Application extends App {

  val sunriseHtmlParser = new SunriseHtmlParser
  sunriseHtmlParser.parseSunrisesSunsets

//  val filesConfig = ConfigFactory.load("app.conf")
//  val pathToDataFolder = filesConfig.getString("file.inputPath")
//  val inputFileAccidents = pathToDataFolder + filesConfig.getString("file.input.inputFileNYAccidents")
//  val accidentsParser = new AccidentsParser
//
//  val raws = accidentsParser.readData(inputFileAccidents)
//  val mergedData = MergeService.mergeAccidentsWithWeatherAndDistricts(raws).cache()
//
//  val dayOfWeek = WeatherMetric.countDayOfWeek(mergedData)
//  val hourOfDay = WeatherMetric.countHours(mergedData)
//
//  dayOfWeek.foreach(println)
//  hourOfDay.foreach(println)
//  println(mergedData.count)

//  dayOfWeek.foreach(println)
//  println(dayOfWeek.count())
//  println("KISAKISA")
//  print(hourOfDay.count())
//  hourOfDay.foreach(println)
}
