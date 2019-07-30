package com.itechart.accidents

import com.itechart.accidents.constants.Configuration
import com.itechart.accidents.constants.Injector.injector
import com.itechart.accidents.database.DistrictsStorage
import com.itechart.accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.accidents.database.dao.DistrictsDAO
import com.itechart.accidents.entity._
import com.itechart.accidents.integration.ny.database.dao.PopulationStorage
import com.itechart.accidents.integration.ny.parse.NYAccidentsParser
import com.itechart.accidents.integration.ny.service.{NYMergeService, WeatherMappingService}
import com.itechart.accidents.report.Reports
import com.itechart.accidents.report.generators._
import com.itechart.accidents.service.metric.{DayPeriodMetricService, PopulationMetricService, WeatherMetricService}
import com.itechart.accidents.service.DistrictsService
import com.itechart.accidents.utils.FileWriterUtils
import org.apache.spark.sql.Dataset
import org.slf4j.LoggerFactory


object Application extends App {
  lazy val logger = LoggerFactory.getLogger(getClass)

  val weatherMetricService = WeatherMetricService
  val dayPeriodMetricService = DayPeriodMetricService
  val cacheService = injector.getInstance(classOf[MergedDataCacheDAO])
  val populationService = injector.getInstance(classOf[PopulationMetricService])
  val populationStorage = injector.getInstance(classOf[PopulationStorage])
  val weatherMappingService = injector.getInstance(classOf[WeatherMappingService])
  val districtService = injector.getInstance(classOf[DistrictsService])
  val districtDao = injector.getInstance(classOf[DistrictsDAO])
  val districtsStorage = injector.getInstance(classOf[DistrictsStorage])
  val reports = injector.getInstance(classOf[Reports])


  val accidents: Dataset[AccidentSparkFormat] = NYAccidentsParser.readData(Configuration.DATA_FILE_PATH).cache()
  logger.info("Raw data read - " + accidents.count())
  val mergedData = NYMergeService.mergeData(accidents).cache()

  logger.info("Merged data length: " + mergedData.count)

  val creationDate = org.apache.spark.sql.functions.current_date()
  val reportSeq = Seq(
    new DayOfWeekReportGenerator(),
    new HourOfDayReportGenerator(),
    new PeriodReportGenerator(),
    new WeatherReportGenerator(),
    new BoroughReportGenerator(),
    new DistrictReportGenerator(),
    new PopulationToNumberOfAccidentsReportGenerator(populationService),
    new AccidentCountDuringPhenomenonPerHourReportGenerator(),
    new FrequencyReportGenerator(),
    new DetailedDistrictReportGenerator()

  )

  reportSeq.foreach(report => FileWriterUtils.writeToCsv(report.apply(mergedData, reports), s"reports/${report.tableName}"))

}
