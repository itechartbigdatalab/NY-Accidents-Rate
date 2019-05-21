package com.itechart.ny_accidents

import com.itechart.ny_accidents.database.dao.MergedDataCacheDAO
import com.itechart.ny_accidents.entity.{Accident, MergedData}
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.service.metric.{Metrics, WeatherMetric}
import com.itechart.ny_accidents.spark.Spark
import com.itechart.ny_accidents.utils.FileWriterUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD

import scala.concurrent.ExecutionContext

object Application extends App {
  private implicit val ec: ExecutionContext = ExecutionContext.global
  val filesConfig = ConfigFactory.load("app.conf")
  val pathToDataFolder = filesConfig.getString("file.inputPath")
  val inputFileAccidents = pathToDataFolder + filesConfig.getString("file.input.inputFileNYAccidents")
  val accidentsParser = new AccidentsParser

  val rawData = Spark.sc.parallelize(accidentsParser.readData(inputFileAccidents))
  println("Raw data read")
  val mergedData: RDD[MergedData] = MergeService
    .mergeAccidentsWithWeatherAndDistricts[Accident, MergedData](rawData, MergeService.mapper).cache()


  println("DONE: " + mergedData.count())

//
//  val dayOfWeek: RDD[(String, Double)] = WeatherMetric.countDayOfWeek(mergedData)
//  val hourOfDay: RDD[(Int, Double)] = WeatherMetric.countHours(mergedData)
//  val period: RDD[(String, Double)] = WeatherMetric.definePeriod(mergedData)
//  val weatherPhenomenon: RDD[(String, Double)] = Metrics.getPhenomenonPercentage(mergedData)
//  val boroughPercentage: RDD[(String, Double)] = Metrics.getBoroughPercentage(mergedData)
//  val districtsPercentage: RDD[(String, Double)] = Metrics.getDistrictsPercentage(mergedData)
//
//  val report = new Reports()
//  val dayOfWeekReport = report.generateReportString[String, Double](dayOfWeek)
//  val hourOfDayReport = report.generateReportString[Int, Double](hourOfDay)
//  val periodReport = report.generateReportString[String, Double](period)
//  val weatherReport = report.generateReportString[String, Double](weatherPhenomenon)
//  val boroughReport = report.generateReportString[String, Double](boroughPercentage)
//  val districtsReport = report.generateReportString[String, Double](districtsPercentage)
//
//
//  FileWriterUtils.writeToCsv(dayOfWeekReport, "reports/day_of_week.csv")
//  FileWriterUtils.writeToCsv(hourOfDayReport, "reports/hour_of_day.csv")
//  FileWriterUtils.writeToCsv(periodReport, "reports/period.csv")
//  FileWriterUtils.writeToCsv(weatherReport, "reports/weather_phenomenon.csv")
//  FileWriterUtils.writeToCsv(boroughReport, "reports/borough.csv")
//  FileWriterUtils.writeToCsv(districtsReport, "reports/districts.csv")
}
