package com.itechart.ny_accidents

import java.time.LocalDateTime

import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.constants.Injector.injector
import com.itechart.ny_accidents.database.NYDataDatabase
import com.itechart.ny_accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.ny_accidents.database.dao.{DistrictsDAO, PopulationStorage}
import com.itechart.ny_accidents.entity._
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.report.generators._
import com.itechart.ny_accidents.service.metric.{PopulationMetricService, WeatherMetricService}
import com.itechart.ny_accidents.service.{DistrictsService, MergeService, WeatherMappingService}
import com.itechart.ny_accidents.utils.DateUtils
import org.apache.spark.rdd.RDD
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
  val reports = injector.getInstance(classOf[Reports])
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

  val mergedWithWeather = accidentsWithIds.joinWith(weather, accidentsWithIds("_1") === weather("_1"), "inner")
    .map(t => (t._1._2, t._2._2))
    .map(t => (districtService.getDistrictName(t._1.latitude.get, t._1.longitude.get), t._1, t._2)).cache()

  val mergedWithWeatherAndDistricts = mergedWithWeather.joinWith(districts, mergedWithWeather("_1") === districts("_1"), "inner")
    .map(t => (t._1._2, t._1._3, t._2._2))
    .map(t => MergedDataDataSets(t._1, t._3, t._2))

  val mergedData: RDD[MergedData] = mergedWithWeatherAndDistricts.rdd.map(mergedData => {
    val localDateTime: LocalDateTime = DateUtils.getLocalDateFromMillis(mergedData.accident.dateTimeMillis.get)
    val oldAccident = mergedData.accident
    MergedData(
      Accident(
        oldAccident.uniqueKey,
        Option(localDateTime),
        oldAccident.dateTimeMillis,
        oldAccident.borough,
        oldAccident.latitude,
        oldAccident.longitude,
        oldAccident.onStreet,
        oldAccident.crossStreet,
        oldAccident.offStreet,
        oldAccident.personsInjured,
        oldAccident.personsKilled,
        oldAccident.pedestriansInjured,
        oldAccident.pedestriansKilled,
        oldAccident.cyclistInjured,
        oldAccident.cyclistKilled,
        oldAccident.motoristInjured,
        oldAccident.motoristKilled,
        oldAccident.contributingFactors,
        oldAccident.vehicleType
      ),
      Some(District(
        mergedData.district.boroughName,
        mergedData.district.districtName,
        null
      )),
      Option(mergedData.weather)
    )
  }).cache()
  println("Kisa")

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

  reportSeq.foreach(report => NYDataDatabase.insertDataFrame(report.tableName, report.apply(mergedData, reports, creationDate)))

}
