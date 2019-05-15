package com.itechart.ny_accidents.weather

import com.itechart.ny_accidents.districts.controller.ExtendedPostgresDriver.api._
import com.itechart.ny_accidents.entity.{WeatherEntity, WeatherStation}
import com.vividsolutions.jts.geom.Point
import slick.jdbc.{JdbcProfile, PostgresProfile}

class WeatherDAO(val profile: JdbcProfile = PostgresProfile) {

  private lazy val WEATHER_TABLE_NAME = "weather"
  private lazy val STATION_TABLE_NAME = "station"
  private lazy val weatherQuery = TableQuery[WeatherEntities]
  private lazy val stationQuery = TableQuery[Stations]

  private class WeatherEntities(tag: Tag) extends Table[WeatherEntity](tag, WEATHER_TABLE_NAME) {
    val id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    val stationId = column[Int]("station_fk")
    val localDateTime = column[Long]("datetime")
    val temperature = column[Int]("temperature")
    val pressure = column[Double]("pressure")
    val humidity = column[Int]("humidity")
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


  def insert(weather: WeatherEntity): DBIO[Int] = weatherQuery += weather

  def insert(station: WeatherStation): DBIO[Int] = stationQuery += station

}
