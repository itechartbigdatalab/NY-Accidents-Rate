package com.itechart.ny_accidents

import com.google.inject.Guice
import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.ny_accidents.entity.{Accident, MergedData}
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.service.MergeService
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object YearMetricApp extends App {
  lazy val logger = LoggerFactory.getLogger(getClass)

  val injector = Guice.createInjector(new GuiceModule)
  val accidentsParser = injector.getInstance(classOf[AccidentsParser])
  val mergeService = injector.getInstance(classOf[MergeService])
  val cacheService = injector.getInstance(classOf[MergedDataCacheDAO])
  sys.addShutdownHook(cacheService.close)

  val firstYearRows = accidentsParser.readData(Configuration.FIRST_YEAR_FILE_PATH).cache()
  val secondYearRows = accidentsParser.readData(Configuration.SECOND_YEAR_FILE_PATH).cache()
  logger.info("Raw data read")

  val firstYearMergedData: RDD[MergedData] = mergeService
    .mergeAccidentsWithWeatherAndDistricts[Accident, MergedData](firstYearRows, mergeService.withoutWeatherMapper).cache()
  logger.info("Merged data size: " + firstYearMergedData.count())
  val secondYearMergedData: RDD[MergedData] = mergeService
    .mergeAccidentsWithWeatherAndDistricts[Accident, MergedData](secondYearRows, mergeService.withoutWeatherMapper).cache()
  logger.info("Merged data size: " + secondYearMergedData.count())

}
