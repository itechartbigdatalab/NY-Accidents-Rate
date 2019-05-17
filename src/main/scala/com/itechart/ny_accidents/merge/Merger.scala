package com.itechart.ny_accidents.merge

import com.itechart.ny_accidents.database.DistrictsStorage
import com.itechart.ny_accidents.districts.controller.DistrictsService
import com.itechart.ny_accidents.entity.{Accident, MergedData}
import com.itechart.ny_accidents.weather.WeatherMappingService
import org.apache.spark.rdd.RDD

object Merger {

  private val ds = new DistrictsService
  private val service = new WeatherMappingService()

  def apply(accidents: RDD[Accident]): RDD[MergedData] = {
    accidents.map(mapper)
  }

  private def mapper(value: Accident): MergedData = {

    if (value.longitude.isDefined && value.latitude.isDefined) {
      val district = ds.getDistrict(value.latitude.getOrElse(0), value.longitude.getOrElse(0), DistrictsStorage.data)
      if (value.dateTimeMillis.isDefined) {
        val weather = service.findWeatherByTimeAndCoordinates(value.dateTimeMillis.getOrElse(0), value.latitude.getOrElse(0), value.longitude.getOrElse(0))
        MergedData(value, district, weather)
      }
      else
        MergedData(value, district, None)
    }
    else
      MergedData(value, None, None)
  }
}
