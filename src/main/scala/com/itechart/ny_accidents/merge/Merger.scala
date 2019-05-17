package com.itechart.ny_accidents.merge

import java.time.LocalDateTime

import com.itechart.ny_accidents.GeneralConstants
import com.itechart.ny_accidents.database.DistrictsStorage
import com.itechart.ny_accidents.districts.controller.{DistrictsDatabase, DistrictsService}
import com.itechart.ny_accidents.districts.database.DistrictsDao
import com.itechart.ny_accidents.entity.{AccidentsNY, MergedData}
import com.itechart.ny_accidents.utils.{DateUtils, PostgisUtils}
import com.itechart.ny_accidents.weather.WeatherMappingService
import org.codehaus.jackson.map.ext.JodaDeserializers.LocalDateTimeDeserializer

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Merger {

  def apply(accidents: Seq[AccidentsNY.RawAccidentsNY]):  Seq[MergedData] ={
     accidents.map(mapper)
  }

  private def mapper(value: AccidentsNY.RawAccidentsNY): MergedData ={
    val ds = new DistrictsService
    val district = ds.getDistrict(value.latitude.getOrElse(0), value.longitude.getOrElse(0), DistrictsStorage.data )
    val service = new WeatherMappingService()
    val weather = service.findWeatherByTimeAndCoordinates(value.datetimeunix), value.latitude.getOrElse(0), value.longitude.getOrElse(0))
    MergedData(value, district, weather)
  }
}
