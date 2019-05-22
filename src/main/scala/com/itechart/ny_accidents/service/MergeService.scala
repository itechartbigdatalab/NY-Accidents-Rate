package com.itechart.ny_accidents.service

import com.google.inject.{Inject, Singleton}
import com.itechart.ny_accidents.database.DistrictsStorage
import com.itechart.ny_accidents.database.dao.cache.{EhCacheDAO, MergedDataCacheDAO}
import com.itechart.ny_accidents.entity.{Accident, MergedData, ReportAccident, ReportMergedData}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

@Singleton
class MergeService @Inject()(weatherService: WeatherMappingService,
                             districtsService: DistrictsService,
                             districtsStorage: DistrictsStorage,
                             cacheService: MergedDataCacheDAO) {
  private var counter = 0

  def mergeAccidentsWithWeatherAndDistricts[A, B](accidents: RDD[A], fun: A => B)(implicit tag: ClassTag[B]): RDD[B] = {
    accidents.map(fun)
  }

  def mapper(value: Accident): MergedData = {
    if(counter % 1000 == 0)
      println("COUNTER: " + counter)
    counter += 1

    value.uniqueKey match {
      case Some(pk) =>
        cacheService.readMergedDataFromCache(pk) match {
          case Some(data) => data
          case None =>
            val data = createMergedData(value)
            cacheService.cacheMergedData(data)
            data
        }
      case None => createMergedData(value)
    }

  }

  private def createMergedData(value: Accident): MergedData = {
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
      ReportAccident(accident.dateTime, accident.dateTimeMillis, accident.latitude, accident.longitude)
    })
  }

  // todo remove counter!
  def splitDataMapper(value: ReportAccident): ReportMergedData = {
    println("COUNTER: " + counter)
    counter += 1

    (value.latitude, value.longitude) match {
      case (Some(latitude), Some(longitude)) =>
        val district = districtsService.getDistrict(latitude, longitude, districtsStorage.districts)
        value.dateTimeMillis match {
          case Some(mills) =>
            val weather = weatherService.findWeatherByTimeAndCoordinates(mills, latitude, longitude)
            ReportMergedData(value, district, weather)
          case _ => ReportMergedData(value, district, None)
        }
      case _ => ReportMergedData(value, None, None)
    }
  }
}