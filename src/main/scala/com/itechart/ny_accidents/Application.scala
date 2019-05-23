package com.itechart.ny_accidents

import java.util.Date

import com.google.inject.Guice
import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.database.dao.PopulationStorage
import com.itechart.ny_accidents.database.dao.cache.{EhCacheDAO, MergedDataCacheDAO}
import com.itechart.ny_accidents.entity.{Accident, MergedData, ReportAccident, ReportMergedData}
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.service.{MergeService, PopulationService}
import com.itechart.ny_accidents.service.metric.{DistrictMetricService, PopulationMetricService, TimeMetricService, WeatherMetricService}
import com.itechart.ny_accidents.utils.{DateUtils, FileWriterUtils}
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD

object Application extends App {
  println("POPPATH: " + Configuration.POPULATION_FILE_PATH)
  println(PopulationStorage.populationMap.get(0))

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

  val dayOfWeek: RDD[(String, Int, Double)] = TimeMetricService.countDayOfWeek(mergeData)
  println("Day of week calculated")
  val hourOfDay: RDD[(Int, Int, Double)] = TimeMetricService.countHours(mergeData)
  println("Hour of day calculated")
  val period: RDD[(String, Int, Double)] = weatherMetricService.definePeriod(mergeData)
  println("Period calculated")
  val weatherPhenomenon: RDD[(String, Int, Double)] = weatherMetricService.getPhenomenonPercentage(mergeData)
  println("Weather phenomenon calculated")
  val boroughPercentage: RDD[(String, Int, Double)] = DistrictMetricService.getBoroughPercentage(mergeData)
  println("Borough percentage calculated" +
    "")
  val districtsPercentage: RDD[(String, Int, Double)] = DistrictMetricService.getDistrictsPercentage(mergeData)
  println("Districts percentage calculated")
  val populationToNumberOfAccidents: RDD[(String, Double)] = PopulationMetricService
    .getPopulationToNumberOfAccidentsRatio(mergeData)
  println("Population to number of accidents calculated")

  val report = injector.getInstance(classOf[Reports])
  val dayOfWeekReport = report.generateReportStringFor3Fields[String, Int, Double](dayOfWeek)
  println("Day of week report created")
  val hourOfDayReport = report.generateReportStringFor3Fields[Int, Int, Double](hourOfDay)
  println("Hour of day report created")
  val periodReport = report.generateReportStringFor3Fields[String, Int, Double](period)
  println("Period report created")
  val weatherReport = report.generateReportStringFor3Fields[String, Int, Double](weatherPhenomenon)
  println("Weather report created")
  val boroughReport = report.generateReportStringFor3Fields[String, Int, Double](boroughPercentage)
  println("Borough report created")
  val districtsReport = report.generateReportStringFor3Fields[String, Int, Double](districtsPercentage)
  println("Districts report created")
  val populationToNumberOfAccidentsReport = report
    .generateReportStringFor2Fields[String, Double](populationToNumberOfAccidents)
  println("Population to number of accidents report created")


  FileWriterUtils.writeToCsv(dayOfWeekReport, "reports/day_of_week.csv")
  FileWriterUtils.writeToCsv(hourOfDayReport, "reports/hour_of_day.csv")
  FileWriterUtils.writeToCsv(periodReport, "reports/period.csv")
  FileWriterUtils.writeToCsv(weatherReport, "reports/weather_phenomenon.csv")
  FileWriterUtils.writeToCsv(boroughReport, "reports/borough.csv")
  FileWriterUtils.writeToCsv(districtsReport, "reports/districts.csv")
  FileWriterUtils.writeToCsv(populationToNumberOfAccidentsReport,
    "reports/population_to_number_of_accidents_report.csv")
}