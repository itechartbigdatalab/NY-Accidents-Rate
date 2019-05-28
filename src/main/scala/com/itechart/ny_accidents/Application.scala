package com.itechart.ny_accidents

import com.google.inject.Guice
import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.constants.GeneralConstants._
import com.itechart.ny_accidents.constants.ReportsDatabaseSchemas._
import com.itechart.ny_accidents.database.dao.PopulationStorage
import com.itechart.ny_accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.ny_accidents.entity.{Accident, MergedData}
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.service.metric.{PopulationMetricService, TimeMetricService, WeatherMetricService}
import com.itechart.ny_accidents.utils.FileWriterUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameWriter, Row}
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

  val report = injector.getInstance(classOf[Reports])
  val dayOfWeekReport = report.generateDataFrameReportForTupleRDD[(String, Int, Double)](dayOfWeek, DAY_OF_WEEK_REPORT_SCHEMA)
  //  FileWriterUtils.writeToCsv(dayOfWeekReport, "reports/day_of_week.csv")
  logger.info("Day of week report created")
  dayOfWeekReport.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/ny_data")
    .option("dbtable", "asd")
    .option("user", "postgres")
    .option("password", "postgres")
    .save()
}
