package com.itechart.ny_accidents

import com.google.inject.Guice
import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.ny_accidents.entity.{Accident, MergedData}
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.service.metric.{DayPeriodMetricService, DistrictMetricService, TimeMetricService, WeatherMetricService}
import com.itechart.ny_accidents.utils.FileWriterUtils
import org.apache.spark.rdd.RDD

object Application extends App {
  val injector = Guice.createInjector(new GuiceModule)
  val accidentsParser = injector.getInstance(classOf[AccidentsParser])
  val mergeService = injector.getInstance(classOf[MergeService])
  val weatherMetricService = injector.getInstance(classOf[WeatherMetricService])
  val cacheService = injector.getInstance(classOf[MergedDataCacheDAO])
  sys.addShutdownHook(cacheService.close)

  val raws = accidentsParser.readData(Configuration.DATA_FILE_PATH).cache()
  println("RAWS DATA READ")

  val mergeData: RDD[MergedData] = mergeService
    .mergeAccidentsWithWeatherAndDistricts[Accident, MergedData](raws, mergeService.mapper).cache()
  println("Merged data size: " + mergeData.count())

  val dayOfWeek: RDD[(String, Double)] = TimeMetricService.countDayOfWeek(mergeData)
  val hourOfDay: RDD[(Int, Double)] = TimeMetricService.countHours(mergeData)
  val period: RDD[(String, Double)] = weatherMetricService.definePeriod(mergeData)
  val weatherPhenomenon: RDD[(String, Double)] = weatherMetricService.getPhenomenonPercentage(mergeData)
  val boroughPercentage: RDD[(String, Double)] = DistrictMetricService.getBoroughPercentage(mergeData)
  val districtsPercentage: RDD[(String, Double)] = DistrictMetricService.getDistrictsPercentage(mergeData)
  //    val districtsByBorough: RDD[(String, Map[String, Double])] = DistrictMetricService.getDistrictsPercentageByBorough(mergeData)
  val dayPeriodHourFrequency:Map[String, Long] = DayPeriodMetricService.getFrequency(mergeData)

  val report = injector.getInstance(classOf[Reports])
  val dayOfWeekReport = report.generateReportString[String, Double](dayOfWeek)
  val hourOfDayReport = report.generateReportString[Int, Double](hourOfDay)
  val periodReport = report.generateReportString[String, Double](period)
  val weatherReport = report.generateReportString[String, Double](weatherPhenomenon)
  val boroughReport = report.generateReportString[String, Double](boroughPercentage)
  val districtsReport = report.generateReportString[String, Double](districtsPercentage)
  val frequencyReport = report.generateReportString(dayPeriodHourFrequency)


  FileWriterUtils.writeToCsv(dayOfWeekReport, "reports/day_of_week.csv")
  FileWriterUtils.writeToCsv(hourOfDayReport, "reports/hour_of_day.csv")
  FileWriterUtils.writeToCsv(periodReport, "reports/period.csv")
  FileWriterUtils.writeToCsv(weatherReport, "reports/weather_phenomenon.csv")
  FileWriterUtils.writeToCsv(boroughReport, "reports/borough.csv")
  FileWriterUtils.writeToCsv(districtsReport, "reports/districts.csv")
  FileWriterUtils.writeToCsv(frequencyReport, "reports/frequency.csv")
}
