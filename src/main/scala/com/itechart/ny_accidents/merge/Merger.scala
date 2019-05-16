package com.itechart.ny_accidents.merge

import java.time.format.DateTimeFormatter

import com.itechart.ny_accidents.districts.controller.DistrictsDatabase
import com.itechart.ny_accidents.districts.database.DistrictsDao
import com.itechart.ny_accidents.entity.{AccidentsNY, MergedData}
import com.itechart.ny_accidents.utils.{DateUtils, PostgisUtils}
import com.itechart.ny_accidents.weather.WeatherMappingService
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Merger {

  def apply(accidents: RDD[AccidentsNY.RawAccidentsNY], sc: SparkContext):  RDD[MergedData] ={

    val q = accidents.map(mapper)
      q

  }

  // TODO Replace Districts Dao by DistrictsService!
  private def mapper(value: AccidentsNY.RawAccidentsNY): MergedData ={
    val dao = new DistrictsDao
    val fut = DistrictsDatabase.database.run(dao.getByCoordinates(PostgisUtils.createPoint(value.latitude.getOrElse(0), value.longitude.getOrElse(0))))
    val district = Await.result(fut, Duration.Inf)
    val service = new WeatherMappingService()
    val weather = service.findWeatherByTimeAndCoordinates(DateUtils.parseDate(value.date.toString ,DateTimeFormatter.ofPattern("MM/d/yyyy")).getOrElse(0), value.latitude.getOrElse(0), value.longitude.getOrElse(0))
    MergedData(value, district, weather)
  }
}
