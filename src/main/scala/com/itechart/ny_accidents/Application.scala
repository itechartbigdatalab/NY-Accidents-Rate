package com.itechart.ny_accidents

import com.itechart.ny_accidents.constants.{Configuration, Injector}
import com.itechart.ny_accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.ny_accidents.entity.{Accident, MergedData}
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.report.ReportsGenerator
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.service.metric.PopulationMetricService
import com.itechart.ny_accidents.utils.FileWriterUtils
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import com.itechart.ny_accidents.constants.Injector.injector

object Application extends App {
  lazy val logger = LoggerFactory.getLogger(getClass)

  val accidentsParser = injector.getInstance(classOf[AccidentsParser])
  val mergeService = injector.getInstance(classOf[MergeService])
  val cacheService = injector.getInstance(classOf[MergedDataCacheDAO])
  val populationService = injector.getInstance(classOf[PopulationMetricService])
  sys.addShutdownHook(cacheService.close)

  val rawData = accidentsParser.readData(Configuration.DATA_FILE_PATH).cache()
  val mergeData: RDD[MergedData] = mergeService
    .mergeAccidentsWithWeatherAndDistricts[Accident, MergedData](rawData, mergeService.mapper).cache()

  val reportsGenerator = new ReportsGenerator(populationService)
  val (dayOfWeek, hourOfDay, period, phenomenon, borough, districts, population, accidents) = reportsGenerator.calculateMetrics(mergeData)

  val reports = reportsGenerator.generateStringReports(dayOfWeek, hourOfDay, period, phenomenon,
    borough, districts, population, accidents)

  reports.map(report => ("reports/" + report._1, report._2))
    .foreach(report => FileWriterUtils.writeToCsv(report._2, report._1))

}
