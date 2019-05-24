package com.itechart.ny_accidents

import com.google.inject.Guice
import com.itechart.ny_accidents.constants.Configuration

import com.itechart.ny_accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.ny_accidents.entity.{Accident, MergedData}
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.service.metric.{DistrictMetricService, TimeMetricService, WeatherMetricService}
import com.itechart.ny_accidents.utils.FileWriterUtils

import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object Application extends App {
  lazy val logger = LoggerFactory.getLogger(getClass)

  val injector = Guice.createInjector(new GuiceModule)
  val accidentsParser = injector.getInstance(classOf[AccidentsParser])
  val mergeService = injector.getInstance(classOf[MergeService])
  val weatherMetricService = WeatherMetricService
  val cacheService = injector.getInstance(classOf[MergedDataCacheDAO])
  sys.addShutdownHook(cacheService.close)


  val raws = accidentsParser.readData(Configuration.DATA_FILE_PATH).cache()
  logger.info("Raw data read")

  val mergeData: RDD[MergedData] = mergeService
    .mergeAccidentsWithWeatherAndDistricts[Accident, MergedData](raws, mergeService.mapper).cache()
  logger.info("Merged data size: " + mergeData.count())

  val dayOfWeek: RDD[(String, Int, Double)] = TimeMetricService.countDayOfWeek(mergeData)
  logger.info("Day of week calculated")
  val hourOfDay: RDD[(Int, Int, Double)] = TimeMetricService.countHours(mergeData)
  logger.info("Hour of day calculated")
  val period: RDD[(String, Int, Double)] = weatherMetricService.definePeriod(mergeData)
  logger.info("Period calculated")
  val weatherPhenomenon: RDD[(String, Int, Double)] = weatherMetricService.getPhenomenonPercentage(mergeData)
  logger.info("Weather phenomenon calculated")
  val boroughPercentage: RDD[(String, Int, Double)] = DistrictMetricService.getBoroughPercentage(mergeData)
  logger.info("Borough percentage calculated")
  val districtsPercentage: RDD[(String, Int, Double)] = DistrictMetricService.getDistrictsPercentage(mergeData)
  logger.info("Districts percentage calculated")
  
  val populationToNumberOfAccidents: RDD[(String, Double)] = PopulationMetricService
    .getPopulationToNumberOfAccidentsRatio(mergeData)
  logger.info("Population to number of accidents calculated")
  
    val accidentCountDuringPhenomenonPerHour: RDD[(String, Int, Double, Double)] =
    weatherMetricService.calculateAccidentCountDuringPhenomenonPerHour(mergeData, weatherPhenomenon)
  logger.info("Accidents count per hour for each phenomenon metric calculated")

  
  val report = injector.getInstance(classOf[Reports])
  val dayOfWeekReport = report.generateReportStringFor3Fields[String, Int, Double](dayOfWeek)

  FileWriterUtils.writeToCsv(dayOfWeekReport, "reports/day_of_week.csv")
  logger.info("Day of week report created")

  val hourOfDayReport = report.generateReportStringFor3Fields[Int, Int, Double](hourOfDay)
  FileWriterUtils.writeToCsv(hourOfDayReport, "reports/hour_of_day.csv")
  logger.info("Hour of day report created")

  val periodReport = report.generateReportStringFor3Fields[String, Int, Double](period)
  FileWriterUtils.writeToCsv(periodReport, "reports/period.csv")
  logger.info("Period report created")

  val weatherReport = report.generateReportStringFor3Fields[String, Int, Double](weatherPhenomenon)
  FileWriterUtils.writeToCsv(weatherReport, "reports/weather_phenomenon.csv")
  logger.info("Weather report created")

  val boroughReport = report.generateReportStringFor3Fields[String, Int, Double](boroughPercentage)
  FileWriterUtils.writeToCsv(boroughReport, "reports/borough.csv")
  logger.info("Borough report created")

  val districtsReport = report.generateReportStringFor3Fields[String, Int, Double](districtsPercentage)
  FileWriterUtils.writeToCsv(districtsReport, "reports/districts.csv")
  logger.info("Districts report created")

  val populationToNumberOfAccidentsReport = report
    .generateReportStringFor2Fields[String, Double](populationToNumberOfAccidents)
  FileWriterUtils.writeToCsv(populationToNumberOfAccidentsReport,
    "reports/population_to_number_of_accidents_report.csv")
  logger.info("Population to number of accidents report created")
  
   FileWriterUtils.writeToCsv(accidentsPhenomenonsReport, "reports/accidents_count_phenomenon_per_hour.csv")
    logger.info("Accidents count per hour for each phenomenon report created")


}