package com.itechart.ny_accidents

import com.itechart.ny_accidents.entity.{ReportAccident, ReportMergedData}
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.service.metric.{Metrics, WeatherMetric}
import com.itechart.ny_accidents.utils.FileWriterUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Application extends App {
  val raws = accidentsParser.readData(inputFileAccidents).cache()
  println("RAWS DATA READ")
  val splitData = MergeService.splitData(raws).cache()
  println("Split data: " + splitData.count())
  val mergeData: RDD[ReportMergedData] = MergeService
    .mergeAccidentsWithWeatherAndDistricts[ReportAccident, ReportMergedData](splitData, MergeService.splitDataMapper).cache()
  println("Merged data size: " + mergeData.count())

  val dayOfWeek: RDD[(String, Double)] = WeatherMetric.countDayOfWeek(mergeData)
  val hourOfDay: RDD[(Int, Double)] = WeatherMetric.countHours(mergeData)
  val period: RDD[(String, Double)] = WeatherMetric.definePeriod(mergeData)

  val weatherPhenomenon: RDD[(String, Double)] = Metrics.getPhenomenonPercentage(mergeData)
  val boroughPercentage: RDD[(String, Double)] = Metrics.getBoroughPercentage(mergeData)
  val districtsPercentage: RDD[(String, Double)] = Metrics.getDistrictsPercentage(mergeData)

  val report = new Reports()
  val dayOfWeekReport = report.generateReportString[String,Double](dayOfWeek)
  val hourOfDayReport = report.generateReportString[Int, Double](hourOfDay)
  val periodReport = report.generateReportString[String, Double](period)
  val weatherReport = report.generateReportString[String,Double](weatherPhenomenon)
  val boroughReport = report.generateReportString[String,Double](boroughPercentage)
  val districtsReport = report.generateReportString[String, Double](districtsPercentage)


  FileWriterUtils.writeToCsv(dayOfWeekReport, "reports/day_of_week.csv")
  FileWriterUtils.writeToCsv(hourOfDayReport, "reports/hour_of_day.csv")
  FileWriterUtils.writeToCsv(periodReport, "reports/period.csv")
  FileWriterUtils.writeToCsv(weatherReport, "reports/weather_phenomenon.csv")
  FileWriterUtils.writeToCsv(boroughReport, "reports/borough.csv")
  FileWriterUtils.writeToCsv(districtsReport, "reports/districts.csv")
}
