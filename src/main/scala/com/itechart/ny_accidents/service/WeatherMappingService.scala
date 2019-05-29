package com.itechart.ny_accidents.service

import com.google.inject.{Inject, Singleton}
import com.itechart.ny_accidents.constants.GeneralConstants
import com.itechart.ny_accidents.constants.GeneralConstants.{FIRST_STATION_ID, LAST_STATION_ID}
import com.itechart.ny_accidents.database.NYDataDatabase
import com.itechart.ny_accidents.database.dao.WeatherDAO
import com.itechart.ny_accidents.entity.{WeatherEntity, WeatherForAccident}
import com.itechart.ny_accidents.parse.WeatherParser
import com.itechart.ny_accidents.utils.{DateUtils, PostgisUtils}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try

@Singleton
class WeatherMappingService @Inject()(weatherDAO: WeatherDAO, weatherParser: WeatherParser) {

  private val allStations = Await.result(NYDataDatabase.database.run(weatherDAO.allStations()), Duration.Inf)
  // map under have such structure -> Map[stationId, Map[TimeHash, Seq[WeatherEntity]]]
  private val allWeather: Map[Int, Map[Long, Seq[WeatherEntity]]] = Await.result(
    NYDataDatabase.database.run(weatherDAO.allWeather()), Duration.Inf)
    .sortBy(weather => weather.dateTime)
    .groupBy(weather => weather.stationId)
    .mapValues(weathers => {
      weathers.map(weather => (DateUtils.hashByDate(weather.dateTime), weather))
        .groupBy(_._1)
        .mapValues(seq => seq.map(_._2))
    })


  def findWeatherByTimeAndCoordinates(accidentTime: Long, lat: Double, lon: Double): Option[WeatherForAccident] = {
    val stationId = getNearestStationId(lat, lon)
    // we take weather for current hour, for future one and for previous, because if we have got 10:58 accident and
    // weather time for 10 o'clock is 10:05, and the next weather is 11:05, it's will be more accurately to take 11:05 weather
    Seq(
      Try(allWeather(stationId)(DateUtils.hashByDate(accidentTime))).toOption,
      Try(allWeather(stationId)(DateUtils.hashByDate(DateUtils.addHour(accidentTime)))).toOption,
      Try(allWeather(stationId)(DateUtils.hashByDate(DateUtils.subtractHour(accidentTime)))).toOption
    ).filter(_.isDefined).map(_.get).reduceOption(_ ++ _) match {
      case Some(value) => Some(findBestMatchWeather(value, accidentTime))
      case _ => None
    }
  }

  def getWeatherByStationsBetweenDates(earlyDate: Long, laterDate: Long): Map[Int, Seq[WeatherEntity]] = {
    val earlyDateHash = DateUtils.hashByDate(earlyDate)
    val laterDateHash = DateUtils.hashByDate(laterDate)
    val intermediateDatesHashes = earlyDateHash to laterDateHash by GeneralConstants.HASH_DIFFERENCE

    (FIRST_STATION_ID to LAST_STATION_ID)
      .map(stationId => {
        val weatherMap = intermediateDatesHashes
          .map(allWeather(stationId).get)
          .filter(_.isDefined)
          .flatMap(_.get)
          .sortBy(_.dateTime)
        (stationId, weatherMap)
      })
      .toMap
  }

  private def findBestMatchWeather(hashedWeatherForPeriod: Seq[WeatherEntity], accidentTime: Long): WeatherForAccident = {
    val farWeather = Try(
      hashedWeatherForPeriod
        .filter(_.dateTime >= accidentTime)
        .minBy(_.dateTime)
    ).toOption

    val lessWeather = Try(
      hashedWeatherForPeriod
        .filter(_.dateTime < accidentTime)
        .maxBy(_.dateTime)
    ).toOption

    val currentWeather = nearestWeather(farWeather, lessWeather, accidentTime)

    WeatherForAccident(
      currentWeather.temperature,
      currentWeather.pressure,
      currentWeather.humidity,
      currentWeather.phenomenon,
      currentWeather.windSpeed,
      currentWeather.visibility
    )
  }

  private def nearestWeather(farWeather: Option[WeatherEntity], lessWeather: Option[WeatherEntity], time: Long): WeatherEntity = {
    (farWeather, lessWeather) match {
      case (Some(far), Some(less)) =>
        if (Math.abs(far.dateTime - time) < Math.abs(less.dateTime - time)) {
          far
        } else {
          less
        }
      case (None, Some(less)) => less
      case (Some(far), None) => far
    }
  }

  private def getNearestStationId(lat: Double, lon: Double): Int = {
    allStations
      .map(station => (station.geom.distance(PostgisUtils.createPoint(lat, lon)), station.id))
      .minBy(_._1)
      ._2
  }

}
