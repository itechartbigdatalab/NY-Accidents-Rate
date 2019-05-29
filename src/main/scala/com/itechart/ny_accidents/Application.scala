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

  val rawData = accidentsParser.readData(Configuration.DATA_FILE_PATH).cache()
  logger.info("Raw data read")

  val mergeData: RDD[MergedData] = mergeService
    .mergeAccidentsWithWeatherAndDistricts[Accident, MergedData](rawData, mergeService.mapper).cache()
  logger.info("Merged data size: " + mergeData.count())

  val creationDate = org.apache.spark.sql.functions.current_date()
  val reports = injector.getInstance(classOf[Reports])

  val reportSeq = Seq(
    new DayOfWeekReportGenerator(),
    new HourOfDayReportGenerator(),
    new PeriodReportGenerator(),
    new WeatherReportGenerator(),
    new BoroughReportGenerator(),
    new DistrictReportGenerator(),
    new PopulationToNumberOfAccidentsReportGenerator(populationService),
    new AccidentCountDuringPhenomenonPerHourReportGenerator()
  )

  reportSeq.foreach(report => NYDataDatabase.insertDataFrame(report.tableName, report.apply(mergeData, reports, creationDate)))
}