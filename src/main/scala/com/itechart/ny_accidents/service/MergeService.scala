package com.itechart.ny_accidents.service

import java.time.LocalDateTime

import com.itechart.ny_accidents.constants.Injector.injector
import com.itechart.ny_accidents.database.DistrictsStorage
import com.itechart.ny_accidents.entity._
import com.itechart.ny_accidents.utils.DateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.slf4j.{Logger, LoggerFactory}

object MergeService {
  private lazy val weatherService = injector.getInstance(classOf[WeatherMappingService])
  private lazy val districtsService = injector.getInstance(classOf[DistrictsService])
  private lazy val districtsStorage = injector.getInstance(classOf[DistrictsStorage])

  private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  import com.itechart.ny_accidents.spark.Spark.sparkSql.implicits._


  def mergeData(data: Dataset[AccidentSparkFormat]): RDD[MergedData] = {
    val accidentWithIds = addIdToAccident(data)
    val mergedDataDataSets = mergeAccidentsWithWeatherAndDistricts(accidentWithIds)

    mergedDataDataSets.rdd.map(mergedDataMapper)
  }

  private def addIdToAccident(data: Dataset[AccidentSparkFormat]):
  Dataset[((Int, Long), AccidentSparkFormat)] = {
    data.filter(_.latitude.isDefined)
      .filter(_.longitude.isDefined)
      .filter(_.dateTimeMillis.isDefined)
      .map(accident => {
        val stationId = weatherService.getNearestStationId(accident.latitude.get, accident.longitude.get)
        val hash = DateUtils.hashByDate(accident.dateTimeMillis.get)
        ((stationId, hash), accident)
      }).as("accident")
  }

  private def mergeAccidentsWithWeatherAndDistricts(accidentsWithIds: Dataset[((Int, Long), AccidentSparkFormat)]):
  Dataset[MergedDataDataSets] = {
    val weather = weatherService.weather.as("weather").cache()
    val districts = districtsStorage.districtsDataSet.map(district => (district.districtName, district))

    val accidentsWithWeather = accidentsWithIds.joinWith(weather, accidentsWithIds("_1") === weather("_1"), "inner")
      .map { case (accident, weatherForAccident) => (accident._2, weatherForAccident._2) }
      .map { case (accident, weatherForAccident) =>
        (districtsService.getDistrictName(accident.latitude.get, accident.longitude.get), accident, weatherForAccident)
      }.cache()

    accidentsWithWeather.joinWith(districts, accidentsWithWeather("_1") === districts("_1"), "inner")
      .map { case (accidentAndWeather, district) =>
        (accidentAndWeather._2, accidentAndWeather._3, district._2)
      }
      .map { case (accident, weatherForAccident, district) => MergedDataDataSets(accident, district, weatherForAccident) }
  }

  private def mergedDataMapper(data: MergedDataDataSets): MergedData = {
    val localDateTime: LocalDateTime = DateUtils.getLocalDateFromMillis(data.accident.dateTimeMillis.get)
    val oldAccident = data.accident
    val oldDistrict = data.district

    val accident = Accident(oldAccident.uniqueKey,
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
    )

    val district = District(oldDistrict.districtName, oldDistrict.boroughName)
    val weather = data.weather

    MergedData(accident, Option(district), Option(weather))
  }

}