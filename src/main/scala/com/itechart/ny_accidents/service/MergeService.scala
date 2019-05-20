package com.itechart.ny_accidents.service

import com.itechart.ny_accidents.database.DistrictsStorage
import com.itechart.ny_accidents.entity.{Accident, MergedData, ReportAccident, ReportMergedData}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object MergeService {
  private var counter = 0
  private val ds = new DistrictsService
  private val service = new WeatherMappingService()

  def mergeAccidentsWithWeatherAndDistricts[A,B](accidents: RDD[A], fun: A => B)(implicit tag: ClassTag[B]): RDD[B] = {
    accidents.map(obj => {

      fun(obj)
    })
  }

  def mapper(value: Accident): MergedData = {

    (value.latitude, value.longitude) match {
      case (Some(latitude), Some(longitude)) =>
        val district = ds.getDistrict(latitude, longitude, DistrictsStorage.districts)
        value.dateTimeMillis match {
          case Some(mills) =>
            val weather = service.findWeatherByTimeAndCoordinates(mills, latitude, longitude)
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
        val district = ds.getDistrict(latitude, longitude, DistrictsStorage.districts)
        value.dateTimeMillis match {
          case Some(mills) =>
            val weather = service.findWeatherByTimeAndCoordinates(mills, latitude, longitude)
            ReportMergedData(value, district, weather)
          case _ => ReportMergedData(value, district, None)
        }
      case _ => ReportMergedData(value, None, None)
    }
  }
}