package com.itechart.ny_accidents.service

import com.google.inject.{Inject, Singleton}
import com.itechart.ny_accidents.database.DistrictsStorage
import com.itechart.ny_accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.ny_accidents.entity.{Accident, MergedData, ReportAccident}
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.ClassTag

@Singleton
class MergeService @Inject()(weatherService: WeatherMappingService,
                             districtsService: DistrictsService,
                             districtsStorage: DistrictsStorage,
                             cacheService: MergedDataCacheDAO) {
  private var counter = 0
  private lazy val logger: Logger = LoggerFactory.getLogger(getClass)


  def mergeAccidentsWithWeatherAndDistricts[A, B](accidents: RDD[A], fun: A => B)(implicit tag: ClassTag[B]): RDD[B] = {
    accidents.map(fun)
  }

  def fullMergeMapper(value: Accident): MergedData = {
    if (counter % 1000 == 0) logger.info("COUNTER: " + counter)
    counter += 1

    value.uniqueKey match {
      case Some(pk) =>
        cacheService.readMergedDataFromCache(pk) match {
          case Some(data) => data
          case None =>
            println("Not in cache")
            val data = fullMergeData(value)
            cacheService.cacheMergedData(data)
            data
        }
      case None => fullMergeData(value)
    }
  }

  def withoutWeatherMapper(value: Accident): MergedData = {
    if (counter % 1000 == 0) logger.info("COUNTER: " + counter)
    counter += 1

    value.uniqueKey match {
      case Some(pk) =>
        cacheService.readMergedDataFromCache(pk) match {
          case Some(data) => data
          case None =>
            println("Not in cache")
            val data = mergeWithoutWeather(value)
            cacheService.cacheMergedData(data)
            data
        }
      case None => mergeWithoutWeather(value)
    }
  }

  private def mergeWithoutWeather(value: Accident): MergedData = {
    (value.latitude, value.longitude) match {
      case (Some(latitude), Some(longitude)) =>
        val district = districtsService.getDistrict(latitude, longitude, districtsStorage.districts)
        MergedData(value, district, None)
      case _ => MergedData(value, None, None)
    }
  }

  private def fullMergeData(value: Accident): MergedData = {
    (value.latitude, value.longitude) match {
      case (Some(latitude), Some(longitude)) =>
        val district = districtsService.getDistrict(latitude, longitude, districtsStorage.districts)
        value.dateTimeMillis match {
          case Some(mills) =>
            val weather = weatherService.findWeatherByTimeAndCoordinates(mills, latitude, longitude)
            MergedData(value, district, weather)
          case _ => MergedData(value, district, None)
        }
      case _ => MergedData(value, None, None)
    }
  }

  def splitData(accidents: RDD[Accident]): RDD[ReportAccident] = {
    accidents.map(accident => {
      ReportAccident(accident.localDateTime, accident.dateTimeMillis, accident.latitude, accident.longitude)
    })
  }

}