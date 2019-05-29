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
import com.itechart.ny_accidents.constants.ReportsDatabase._
import com.itechart.ny_accidents.database.NYDataDatabase

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
    weatherMetricService.calculateAccidentCountDuringPhenomenonPerHour(mergeData)
  logger.info("Accidents count per hour for each phenomenon metric calculated")


  val reportGenerateDate = org.apache.spark.sql.functions.current_date()

  val report = injector.getInstance(classOf[Reports])
  val dayOfWeekReport = report.generateDataFrameReportForTupleRDD(dayOfWeek, DAY_OF_WEEK_REPORT_SCHEMA, reportGenerateDate)
  NYDataDatabase.insertDataFrame(DAY_OF_WEEK_REPORT_TABLE_NAME, dayOfWeekReport)
  logger.info("Day of week report created")

  val hourOfDayReport = report.generateDataFrameReportForTupleRDD(hourOfDay, HOUR_OF_DAY_REPORT_SCHEMA, reportGenerateDate)
  NYDataDatabase.insertDataFrame(HOUR_OF_DAY_REPORT_TABLE_NAME, hourOfDayReport)
  logger.info("Hour of day report created")

  val periodReport = report.generateDataFrameReportForTupleRDD(period, DAY_OF_WEEK_REPORT_SCHEMA, reportGenerateDate)
  NYDataDatabase.insertDataFrame(DAY_PERIOD_REPORT_TABLE_NAME, periodReport)
  logger.info("Period report created")

  val weatherReport = report.generateDataFrameReportForTupleRDD(weatherPhenomenon, PHENOMENON_REPORT_SCHEMA, reportGenerateDate)
  NYDataDatabase.insertDataFrame(PHENOMENON_REPORT_TABLE_NAME, weatherReport)
  logger.info("Weather report created")

  val boroughReport = report.generateDataFrameReportForTupleRDD(boroughPercentage, BOROUGH_REPORT_SCHEMA, reportGenerateDate)
  NYDataDatabase.insertDataFrame(BOROUGH_REPORT_TABLE_NAME, boroughReport)
  logger.info("Borough report created")

  val districtsReport = report.generateDataFrameReportForTupleRDD(districtsPercentage, DISTRICT_REPORT_SCHEMA, reportGenerateDate)
  println("REPORT: " + districtsReport)
  NYDataDatabase.insertDataFrame(DISTRICT_REPORT_TABLE_NAME, districtsReport)
  logger.info("Districts report created")

  val populationToNumberOfAccidentsReport = report
    .generateDataFrameReportForTupleRDD(populationToNumberOfAccidents, POPULATION_TO_ACCIDENTS_REPORT_SCHEMA, reportGenerateDate)
  NYDataDatabase.insertDataFrame(POPULATION_TO_ACCIDENTS_REPORT_TABLE_NAME, populationToNumberOfAccidentsReport)
  logger.info("Population to number of accidents report created")

  val accidentCountDuringPhenomenonPerHourReport = report
    .generateDataFrameReportForTupleRDD(accidentCountDuringPhenomenonPerHour, ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_SCHEMA, reportGenerateDate)
  NYDataDatabase.insertDataFrame(ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_TABLE_NAME, accidentCountDuringPhenomenonPerHourReport)
  logger.info("Accidents count per hour for each phenomenon report created")
}