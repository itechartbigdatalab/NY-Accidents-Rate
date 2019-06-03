package com.itechart.ny_accidents

import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.constants.Injector.injector
import com.itechart.ny_accidents.database.dao.{DistrictsDAO, PopulationStorage}
import com.itechart.ny_accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.ny_accidents.entity.{AccidentWithoutOptionAndLocalDate, DistrictWithoutGeometry, MergedData, MergedDataDataSets, WeatherForAccident}
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.service.metric.{PopulationMetricService, WeatherMetricService}
import com.itechart.ny_accidents.service.{DistrictsService, MergeService, WeatherMappingService}
//import com.itechart.ny_accidents.spark.Spark.superEncoder
import com.itechart.ny_accidents.utils.DateUtils
import org.apache.spark.sql.Dataset
import org.slf4j.LoggerFactory


object TestMain extends App {
  lazy val logger = LoggerFactory.getLogger(getClass)

  val accidentsParser = AccidentsParser
  val mergeService = injector.getInstance(classOf[MergeService])
  val weatherMetricService = WeatherMetricService
  val cacheService = injector.getInstance(classOf[MergedDataCacheDAO])
  val populationService = injector.getInstance(classOf[PopulationMetricService])
  val populationStorage = injector.getInstance(classOf[PopulationStorage])
  val weatherMappingService = injector.getInstance(classOf[WeatherMappingService])
  val districtService = injector.getInstance(classOf[DistrictsService])
  val districtDao = injector.getInstance(classOf[DistrictsDAO])
  sys.addShutdownHook(cacheService.close)

  val accidents = accidentsParser.readData(Configuration.DATA_FILE_PATH).cache()
  logger.info("Raw data read - " + accidents.count())

  import com.itechart.ny_accidents.spark.Spark.sparkSql.implicits._
  val accidentsWithIds: Dataset[((Int, Long), AccidentWithoutOptionAndLocalDate)] = accidents
    .filter(_.latitude.isDefined)
    .filter(_.longitude.isDefined)
    .filter(_.dateTimeMillis.isDefined)
    .map(accident => {
      val stationId = weatherMappingService.getNearestStationId(accident.latitude.get, accident.longitude.get)
      val hash = DateUtils.hashByDate(accident.dateTimeMillis.get)
      ((stationId, hash), accident)
    }).as("accident")

  val weather = weatherMappingService.weather.as("weather").cache()
  val districts = districtDao.districtsDatasetWithoutGeometry.cache()

  val mergedData = accidentsWithIds.joinWith(weather, accidentsWithIds("_1") === weather("_1"), "inner")
      .map(t => (t._1._2, t._2._2))
      .map(t => (districtService.getDistrictName(t._1.latitude.get, t._1.longitude.get), t._1, t._2)).cache()

  val fullMergedData = mergedData.joinWith(districts, mergedData("_1") === districts("_1"), "inner")
      .map(t => (t._1._2, t._1._3, t._2._2))
      .map(t => MergedDataDataSets(t._1, Option(t._3), Option(t._2)))

  print(fullMergedData.count())
  println("Kisa")

}
