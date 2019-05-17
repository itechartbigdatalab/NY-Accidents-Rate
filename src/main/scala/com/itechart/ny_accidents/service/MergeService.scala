package com.itechart.ny_accidents.service

import com.itechart.ny_accidents.database.DistrictsStorage
import com.itechart.ny_accidents.entity.{Accident, MergedData}
import org.apache.spark.rdd.RDD

object MergeService {

  private val ds = new DistrictsService
  private val service = new WeatherMappingService()

  def mergeAccidentsWithWeatherAndDistricts(accidents: RDD[Accident]): RDD[MergedData] = {
    accidents.map(mapper)
  }

  private def mapper(value: Accident): MergedData = {

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
}