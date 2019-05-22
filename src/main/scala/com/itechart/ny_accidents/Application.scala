package com.itechart.ny_accidents

import com.google.inject.Guice
import com.itechart.ny_accidents.database.dao.cache.{EhCacheDAO, MergedDataCacheDAO}
import com.itechart.ny_accidents.entity.{Accident, MergedData, ReportAccident, ReportMergedData}
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.service.metric.{DistrictMetricService, TimeMetricService, WeatherMetricService}
import com.itechart.ny_accidents.utils.FileWriterUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD

object Application extends App {
  val filesConfig = ConfigFactory.load("app.conf")
  val pathToDataFolder = filesConfig.getString("file.inputPath")
  val inputFileAccidents = pathToDataFolder + filesConfig.getString("file.input.inputFileNYAccidents")

  val injector = Guice.createInjector(new GuiceModule)
  val accidentsParser = injector.getInstance(classOf[AccidentsParser])
  val mergeService = injector.getInstance(classOf[MergeService])
  val weatherMetricService = injector.getInstance(classOf[WeatherMetricService])
  val cacheService = injector.getInstance(classOf[MergedDataCacheDAO])

  val raws = accidentsParser.readData(inputFileAccidents).cache()
  println("RAWS DATA READ")

  val mergeData: RDD[MergedData] = mergeService
    .mergeAccidentsWithWeatherAndDistricts[Accident, MergedData](raws, mergeService.mapper).cache()
  println("Merged data size: " + mergeData.count())

  val dayOfWeek: RDD[(String, Int, Double)] = TimeMetricService.countDayOfWeek(mergeData)
  val hourOfDay: RDD[(Int, Int, Double)] = TimeMetricService.countHours(mergeData)
  val period: RDD[(String, Int, Double)] = weatherMetricService.definePeriod(mergeData)
  val weatherPhenomenon: RDD[(String, Int, Double)] = weatherMetricService.getPhenomenonPercentage(mergeData)
  val boroughPercentage: RDD[(String, Int, Double)] = DistrictMetricService.getBoroughPercentage(mergeData)
  val districtsPercentage: RDD[(String, Int, Double)] = DistrictMetricService.getDistrictsPercentage(mergeData)
  //    val districtsByBorough: RDD[(String, Map[String, Double])] = DistrictMetricService.getDistrictsPercentageByBorough(mergeData)

  val report = injector.getInstance(classOf[Reports])
  val dayOfWeekReport = report.generateReportStringFor3Fields[String, Int, Double](dayOfWeek)
  val hourOfDayReport = report.generateReportStringFor3Fields[Int, Int, Double](hourOfDay)
  val periodReport = report.generateReportStringFor3Fields[String, Int, Double](period)
  val weatherReport = report.generateReportStringFor3Fields[String, Int, Double](weatherPhenomenon)
  val boroughReport = report.generateReportStringFor3Fields[String, Int, Double](boroughPercentage)
  val districtsReport = report.generateReportStringFor3Fields[String, Int, Double](districtsPercentage)


  FileWriterUtils.writeToCsv(dayOfWeekReport, "reports/day_of_week.csv")
  FileWriterUtils.writeToCsv(hourOfDayReport, "reports/hour_of_day.csv")
  FileWriterUtils.writeToCsv(periodReport, "reports/period.csv")
  FileWriterUtils.writeToCsv(weatherReport, "reports/weather_phenomenon.csv")
  FileWriterUtils.writeToCsv(boroughReport, "reports/borough.csv")
  FileWriterUtils.writeToCsv(districtsReport, "reports/districts.csv")
}
