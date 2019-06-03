package com.itechart.ny_accidents

import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.constants.Injector.injector
import com.itechart.ny_accidents.database.{DistrictsStorage, NYDataDatabase}
import com.itechart.ny_accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.ny_accidents.database.dao.{DistrictsDAO, PopulationStorage}
import com.itechart.ny_accidents.entity._
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.report.generators.{AccidentCountDuringPhenomenonPerHourReportGenerator, BoroughReportGenerator, DayOfWeekReportGenerator, DistrictReportGenerator, HourOfDayReportGenerator, PeriodReportGenerator, PopulationToNumberOfAccidentsReportGenerator, WeatherReportGenerator}
import com.itechart.ny_accidents.service.metric.{PopulationMetricService, WeatherMetricService}
import com.itechart.ny_accidents.service.{DistrictsService, MergeService, WeatherMappingService}
import com.itechart.ny_accidents.utils.FileWriterUtils
import org.apache.spark.sql.Dataset
import org.slf4j.LoggerFactory


object Application extends App {
  lazy val logger = LoggerFactory.getLogger(getClass)

  val accidentsParser = AccidentsParser
  val weatherMetricService = WeatherMetricService
  val cacheService = injector.getInstance(classOf[MergedDataCacheDAO])
  val populationService = injector.getInstance(classOf[PopulationMetricService])
  val populationStorage = injector.getInstance(classOf[PopulationStorage])
  val weatherMappingService = injector.getInstance(classOf[WeatherMappingService])
  val districtService = injector.getInstance(classOf[DistrictsService])
  val districtDao = injector.getInstance(classOf[DistrictsDAO])
  val districtsStorage = injector.getInstance(classOf[DistrictsStorage])
  val reports = injector.getInstance(classOf[Reports])

  val accidents: Dataset[AccidentWithoutOptionAndLocalDate] = accidentsParser.readData(Configuration.DATA_FILE_PATH).cache()
  logger.info("Raw data read - " + accidents.count())
  val mergedData = MergeService.mergeData(accidents).cache()

  println("Merged data length: " + mergedData.count)

  val creationDate = org.apache.spark.sql.functions.current_date()
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

  reportSeq.foreach(report => FileWriterUtils.writeToCsv(report.apply(mergedData, reports), report.tableName))
  reportSeq.foreach(report => NYDataDatabase.insertDataFrame(report.tableName, report.apply(mergedData, reports, creationDate)))

}
