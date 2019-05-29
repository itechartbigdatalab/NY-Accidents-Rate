package com.itechart.ny_accidents

import com.google.inject.Guice
import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.constants.GeneralConstants._
import com.itechart.ny_accidents.database.dao.PopulationStorage
import com.itechart.ny_accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.ny_accidents.entity.{Accident, MergedData}
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.service.metric.{DistrictMetricService, PopulationMetricService, TimeMetricService, WeatherMetricService}
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
  val populationService = injector.getInstance(classOf[PopulationMetricService])
  val populationStorage = injector.getInstance(classOf[PopulationStorage])
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
  val populationToNumberOfAccidents: RDD[(String, Double, Double, Int)] = populationService
    .getPopulationToNumberOfAccidentsRatio(mergeData)
  logger.info("Population to number of accidents calculated")
  val accidentCountDuringPhenomenonPerHour: RDD[(String, Int, Double, Double)] =
    weatherMetricService.calculateAccidentCountDuringPhenomenonPerHour(mergeData, weatherPhenomenon)
  logger.info("Accidents count per hour for each phenomenon metric calculated")



  val report = injector.getInstance(classOf[Reports])
  val dayOfWeekReport = report.generateReportForTupleRDD[(String, Int, Double)](dayOfWeek, DAY_OF_WEEK_REPORT_HEADER)
  FileWriterUtils.writeToCsv(dayOfWeekReport, "reports/day_of_week.csv")
  logger.info("Day of week report created")

  val hourOfDayReport = report.generateReportForTupleRDD[(Int, Int, Double)](hourOfDay, HOUR_OF_DAY_REPORT_HEADER)
  FileWriterUtils.writeToCsv(hourOfDayReport, "reports/hour_of_day.csv")
  logger.info("Hour of day report created")

  val periodReport = report.generateReportForTupleRDD[(String, Int, Double)](period, DAY_OF_WEEK_REPORT_HEADER)
  FileWriterUtils.writeToCsv(periodReport, "reports/period.csv")
  logger.info("Period report created")

  val weatherReport = report.generateReportForTupleRDD[(String, Int, Double)](weatherPhenomenon, PHENOMENON_REPORT_HEADER)
  FileWriterUtils.writeToCsv(weatherReport, "reports/weather_phenomenon.csv")
  logger.info("Weather report created")

  val boroughReport = report.generateReportForTupleRDD[(String, Int, Double)](boroughPercentage, BOROUGH_REPORT_HEADER)
  FileWriterUtils.writeToCsv(boroughReport, "reports/borough.csv")
  logger.info("Borough report created")

  val districtsReport = report.generateReportForTupleRDD[(String, Int, Double)](districtsPercentage, DISTRICT_REPORT_HEADER)
  FileWriterUtils.writeToCsv(districtsReport, "reports/districts.csv")

  logger.info("Districts report created")

  val populationToNumberOfAccidentsReport = report
    .generateReportForTupleRDD[(String, Double, Double, Int)](populationToNumberOfAccidents, POPULATION_TO_ACCIDENTS_REPORT_HEADER)
  FileWriterUtils.writeToCsv(populationToNumberOfAccidentsReport,
    "reports/population_to_number_of_accidents_report.csv")
  logger.info("Population to number of accidents report created")

  val accidentCountDuringPhenomenonPerHourReport = report
    .generateReportForTupleRDD[(String, Int, Double, Double)](accidentCountDuringPhenomenonPerHour, ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_HEADER)
  FileWriterUtils.writeToCsv(accidentCountDuringPhenomenonPerHourReport,
    "reports/accidents_count_phenomenon_per_hour.csv")
  logger.info("Accidents count per hour for each phenomenon report created")

}
