package com.itechart.ny_accidents

import com.itechart.ny_accidents.constants.Injector.injector
import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.database.NYDataDatabase
import com.itechart.ny_accidents.database.dao.PopulationStorage
import com.itechart.ny_accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.ny_accidents.entity.{Accident, MergedData}
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.report.generators._
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.service.metric.{PopulationMetricService, WeatherMetricService}
import com.itechart.ny_accidents.constants.ReportsDatabase._
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object Application extends App {
  lazy val logger = LoggerFactory.getLogger(getClass)

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

  val creationDate = org.apache.spark.sql.functions.current_date()
  val reports = injector.getInstance(classOf[Reports])

  val dayOfWeekReport = new DayOfWeekReportGenerator().apply(mergeData, reports, creationDate)
  val hourOfDayReport = new HourOfDayReportGenerator().apply(mergeData, reports, creationDate)
  val periodReport = new PeriodReportGenerator().apply(mergeData, reports, creationDate)
  val weatherReport = new WeatherReportGenerator().apply(mergeData, reports, creationDate)
  val boroughReport = new BoroughReportGenerator().apply(mergeData, reports, creationDate)
  val districtReport = new DistrictReportGenerator().apply(mergeData, reports, creationDate)
  val populationToNumberOfAccidentsReport = new PopulationToNumberOfAccidentsReportGenerator(populationService)
    .apply(mergeData, reports, creationDate)
  val accidentCountDuringPhenomenonPerHourReport = new AccidentCountDuringPhenomenonPerHourReportGenerator()
    .apply(mergeData, reports, creationDate)

  NYDataDatabase.insertDataFrame(DAY_OF_WEEK_REPORT_TABLE_NAME, dayOfWeekReport)
  NYDataDatabase.insertDataFrame(HOUR_OF_DAY_REPORT_TABLE_NAME, hourOfDayReport)
  NYDataDatabase.insertDataFrame(DAY_PERIOD_REPORT_TABLE_NAME, periodReport)
  NYDataDatabase.insertDataFrame(PHENOMENON_REPORT_TABLE_NAME, weatherReport)
  NYDataDatabase.insertDataFrame(BOROUGH_REPORT_TABLE_NAME, boroughReport)
  NYDataDatabase.insertDataFrame(DISTRICT_REPORT_TABLE_NAME, districtReport)
  NYDataDatabase.insertDataFrame(POPULATION_TO_ACCIDENTS_REPORT_TABLE_NAME, populationToNumberOfAccidentsReport)
  NYDataDatabase.insertDataFrame(ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_TABLE_NAME, accidentCountDuringPhenomenonPerHourReport)

}