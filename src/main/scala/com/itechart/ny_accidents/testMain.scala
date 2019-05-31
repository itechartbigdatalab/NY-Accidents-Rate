package com.itechart.ny_accidents

import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.constants.Injector.injector
import com.itechart.ny_accidents.database.dao.PopulationStorage
import com.itechart.ny_accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.ny_accidents.entity.AccidentWithoutOption
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.service.metric.{PopulationMetricService, WeatherMetricService}
import com.itechart.ny_accidents.service.{MergeService, WeatherMappingService}
import com.itechart.ny_accidents.spark.Spark.superEncoder
import com.itechart.ny_accidents.utils.DateUtils
import org.apache.spark.sql.Dataset
import org.slf4j.LoggerFactory


object testMain extends App {
  lazy val logger = LoggerFactory.getLogger(getClass)

  val accidentsParser = AccidentsParser
  val mergeService = injector.getInstance(classOf[MergeService])
  val weatherMetricService = WeatherMetricService
  val cacheService = injector.getInstance(classOf[MergedDataCacheDAO])
  val populationService = injector.getInstance(classOf[PopulationMetricService])
  val populationStorage = injector.getInstance(classOf[PopulationStorage])
  val weatherMappingService = injector.getInstance(classOf[WeatherMappingService])
  sys.addShutdownHook(cacheService.close)

  val rawData = accidentsParser.readData(Configuration.DATA_FILE_PATH).cache()
  logger.info("Raw data read - " + rawData.count())

  val rawData2: Dataset[((Int, Long), AccidentWithoutOption)] = rawData
    .filter(_.latitude != 0)
    .filter(_.longitude != 0)
    .filter(_.dateTimeMillis != 0)
    .map(accident => {
      val stationId = weatherMappingService.getNearestStationId(accident.latitude, accident.longitude)
      val hash = DateUtils.hashByDate(accident.dateTimeMillis)
      ((stationId, hash), accident)
    }).as("accident")
//    .cache()

  val weather = weatherMappingService.weather.as("weather")

  val mergedData = rawData2.joinWith(weather, rawData2("_1") === weather("_1"), "inner")

  println(rawData2.printSchema())
  println("KISAKISAKISAKISA")
  println(rawData.printSchema())
  println("KISAKISAKISAKISA")
  println(weather.printSchema())
  println("Kisa")
  //  val mergedData = data.join(weatherMappingService.weather).cache()
  //
  //  println("Kisa " + mergedData.count())
  //  println("Kisa " + mergedData.count())

  //  val mergeData: RDD[MergedData] = mergeService
  //    .mergeAccidentsWithWeatherAndDistricts[Accident, MergedData](rawData, mergeService.withoutWeatherMapper).cache()

  //  logger.info("Merged data size: " + mergeData.count())

//  val creationDate = org.apache.spark.sql.functions.current_date()
//  val reports = injector.getInstance(classOf[Reports])
//
//  val reportSeq = Seq(
//    new DayOfWeekReportGenerator(),
//    new HourOfDayReportGenerator(),
//    new PeriodReportGenerator(),
//    new WeatherReportGenerator(),
//    new BoroughReportGenerator(),
//    new DistrictReportGenerator(),
//    new PopulationToNumberOfAccidentsReportGenerator(populationService),
//    new AccidentCountDuringPhenomenonPerHourReportGenerator()
//  )

  //  reportSeq.foreach(report => NYDataDatabase.insertDataFrame(report.tableName, report.apply(mergeData, reports, creationDate)))
}
