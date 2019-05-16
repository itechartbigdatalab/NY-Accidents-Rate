package com.itechart.ny_accidents.weather

import com.itechart.ny_accidents.districts.controller.ExtendedPostgresDriver.api._
import com.itechart.ny_accidents.entity.{WeatherEntity, WeatherStation}
import com.vividsolutions.jts.geom.Point
import slick.dbio.Effect
import slick.jdbc.{JdbcProfile, PostgresProfile}
import slick.sql.{FixedSqlAction, SqlStreamingAction}

class WeatherDAO(val profile: JdbcProfile = PostgresProfile) {

  private lazy val WEATHER_TABLE_NAME = "weather"
  private lazy val STATION_TABLE_NAME = "station"
  private lazy val weatherQuery = TableQuery[WeatherEntities]
  private lazy val stationQuery = TableQuery[Stations]

  private class WeatherEntities(tag: Tag) extends Table[WeatherEntity](tag, WEATHER_TABLE_NAME) {
    val id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    val stationId = column[Int]("station_fk")
    val localDateTime = column[Long]("datetime")
    val temperature = column[Double]("temperature")
    val pressure = column[Double]("pressure")
    val humidity = column[Double]("humidity")
    val windSpeed = column[Double]("windspeed")
    val phenomenon = column[String]("phenomenon")
    val visibility = column[Double]("visibility")

    def * = (id, stationId, localDateTime, temperature, pressure, humidity, windSpeed, phenomenon, visibility) <>
      (WeatherEntity.tupled, WeatherEntity.unapply)
  }

  private class Stations(tag: Tag) extends Table[WeatherStation](tag, STATION_TABLE_NAME) {
    val stationId = column[Int]("id", O.PrimaryKey)
    val name = column[String]("name")
    val point = column[Point]("geom")

    def * = (stationId, name, point) <> (WeatherStation.tupled, WeatherStation.unapply)
  }

  var schetchik = 0
  def weatherByCoordinatesAndTime(accidentTime: Long, lat: Double, lon: Double): DBIO[Vector[(Double, Double, Double, String, Double, Double)]] = {

    val retval = sql"""SELECT * from findByCoordinatesAndTime($accidentTime, $lat, $lon)""".as[(Double, Double, Double, String, Double, Double)]

    schetchik += 1
    println(schetchik)
    retval
  }

  def allWeather(): DBIO[Seq[WeatherEntity]] = weatherQuery.result

  def allStations(): DBIO[Seq[WeatherStation]] = stationQuery.result

  def insert(weather: WeatherEntity): DBIO[Int] = weatherQuery += weather

  def insert(weathers: Seq[WeatherEntity]): FixedSqlAction[Option[Int], NoStream, Effect.Write] = weatherQuery ++= weathers

  def insert(station: WeatherStation): DBIO[Int] = stationQuery += station


}
