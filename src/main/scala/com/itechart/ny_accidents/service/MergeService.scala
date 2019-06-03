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


  def mergeData(data: Dataset[AccidentWithoutOptionAndLocalDate]): RDD[MergedData] = {
    val accidentWithIds = addIdToAccident(data)
    val mergedDataDataSets = mergeAccidentsWithWeatherAndDistricts(accidentWithIds)

    mergedDataDataSets.rdd.map(mergedDataMapper)
  }

  private def addIdToAccident(data: Dataset[AccidentWithoutOptionAndLocalDate]):
  Dataset[((Int, Long), AccidentWithoutOptionAndLocalDate)] = {
    data.filter(_.latitude.isDefined)
      .filter(_.longitude.isDefined)
      .filter(_.dateTimeMillis.isDefined)
      .map(accident => {
        val stationId = weatherService.getNearestStationId(accident.latitude.get, accident.longitude.get)
        val hash = DateUtils.hashByDate(accident.dateTimeMillis.get)
        ((stationId, hash), accident)
      }).as("accident")
  }

  private def mergeAccidentsWithWeatherAndDistricts(accidentsWithIds: Dataset[((Int, Long), AccidentWithoutOptionAndLocalDate)]):
  Dataset[MergedDataDataSets] = {
    val weather = weatherService.weather.as("weather").cache()
    val districts = districtsStorage.districtsDataSet.map(district => (district.districtName, district))

    val accidentsWithWeather = accidentsWithIds.joinWith(weather, accidentsWithIds("_1") === weather("_1"), "inner")
      .map(t => (t._1._2, t._2._2))
      .map(t => (districtsService.getDistrictName(t._1.latitude.get, t._1.longitude.get), t._1, t._2)).cache()
    accidentsWithWeather.joinWith(districts, accidentsWithWeather("_1") === districts("_1"), "inner")
      .map(t => (t._1._2, t._1._3, t._2._2))
      .map(t => MergedDataDataSets(t._1, t._3, t._2))
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