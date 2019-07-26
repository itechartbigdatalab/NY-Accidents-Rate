package com.itechart.accidents

import com.google.inject.Guice
import com.itechart.accidents.constants.Configuration
import com.itechart.accidents.constants.GeneralConstants.YEAR_DIFFERENCE_HEADER
import com.itechart.accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.accidents.entity.MergedData
import com.itechart.accidents.integration.ny.parse.NYAccidentsParser
import com.itechart.accidents.integration.ny.service.NYMergeService
import com.itechart.accidents.report.Reports
import com.itechart.accidents.service.metric.YearDifferenceMetricService
import com.itechart.accidents.utils.FileWriterUtils
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object YearMetricApp extends App {
  lazy val logger = LoggerFactory.getLogger(getClass)

  val injector = Guice.createInjector(new GuiceModule)

  val cacheService = injector.getInstance(classOf[MergedDataCacheDAO])
  val report = injector.getInstance(classOf[Reports])
  val yearDifferenceService = injector.getInstance(classOf[YearDifferenceMetricService])
  sys.addShutdownHook(cacheService.close)

  val firstYearRows = NYAccidentsParser.readData(Configuration.FIRST_YEAR_FILE_PATH).cache()
  val secondYearRows = NYAccidentsParser.readData(Configuration.SECOND_YEAR_FILE_PATH).cache()
  logger.info("Raw data read")

  val firstYearMergedData: RDD[MergedData] = NYMergeService.mergeData(firstYearRows).cache()
  logger.info("Merged data size: " + firstYearMergedData.count())
  val secondYearMergedData: RDD[MergedData] = NYMergeService.mergeData(secondYearRows).cache()
  logger.info("Merged data size: " + secondYearMergedData.count())

  val yearDifference = yearDifferenceService.calculateDifferenceBetweenAccidentCount(firstYearMergedData, secondYearMergedData).cache()
  val boroughReport = report.generateReportForTupleRDD[(String, Int)](yearDifference, YEAR_DIFFERENCE_HEADER)
  FileWriterUtils.writeToCsv(boroughReport, "reports/year_difference.csv")
  logger.info("Year Difference Header report created")

}
