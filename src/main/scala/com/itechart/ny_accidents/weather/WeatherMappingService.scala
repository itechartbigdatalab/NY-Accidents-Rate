package com.itechart.ny_accidents.weather

import com.itechart.ny_accidents.districts.controller.DistrictsDatabase
import com.itechart.ny_accidents.entity.{WeatherEntity, WeatherForAccident}
import com.itechart.ny_accidents.utils.PostgisUtils

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class WeatherMappingService {

  val weatherDAO = new WeatherDAO()

  private lazy val allStations = Await.result(DistrictsDatabase.database.run(weatherDAO.allStations()), Duration.Inf)
  private lazy val allWeather = Await.result(
    DistrictsDatabase.database.run(weatherDAO.allWeather()), Duration.Inf
  ).sortBy(weather => weather.localDateTime)


  def findWeatherByTimeAndCoordinates(time: Long, lat: Double, lon: Double): WeatherForAccident = {
    val id = getNearestStationId(lat, lon)
    val currentStationWeather = allWeather
      .filter(_.id == id)

    val farWeather = currentStationWeather
      .filter(_.localDateTime >= time)
      .minBy(_.localDateTime)

    val lessWeather = currentStationWeather
      .filter(_.localDateTime < time)
      .maxBy(_.localDateTime)

    val currentWeather = nearestWeather(farWeather, lessWeather, time)

    WeatherForAccident(
      currentWeather.temperature,
      currentWeather.pressure,
      currentWeather.humidity,
      currentWeather.phenomenon,
      currentWeather.windSpeed,
      currentWeather.visibility
    )
  }

  def nearestWeather(weatherEntity1: WeatherEntity, weatherEntity2: WeatherEntity, time: Long): WeatherEntity = {
    if (Math.abs(weatherEntity1.localDateTime - time) > Math.abs(weatherEntity2.localDateTime - time)) {
      return weatherEntity1
    }
    weatherEntity2
  }

  private def getNearestStationId(lat: Double, lon: Double): Int = {
    allStations
      .map(station => (station.geom.distance(PostgisUtils.createPoint(lat, lon)), station.id))
      .minBy(_._1)
      ._2
  }

}
