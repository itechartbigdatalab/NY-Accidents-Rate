package com.itechart.ny_accidents.weather

import com.itechart.ny_accidents.weather.WeatherDAO.WeatherEntities
import com.itechart.ny_accidents.weather.entity.WeatherEntity
import slick.lifted.{TableQuery, Tag}
import slick.model.Table

object WeatherDAO extends TableQuery(new WeatherEntities(_)) {

  private lazy val TABLE_NAME = "weather"

  private class WeatherEntities(tag: Tag) extends Table[WeatherEntity](tag, "WEATHER_ENTITIES") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def localDateTime = column[Long]("DateTime")
    def temperature = column[Int]("Temperature")
    def pressure = column[Double]("Pressure")
    def humidity = column[Int]("Humidity")
    def windSpeed = column[Double]("windSpeed")
    def phenomenon = column[String]("Phenomenon")
    def visibility = column[Double]("Visibility")

    def * = (id, localDateTime, temperature, pressure, humidity, windSpeed, phenomenon, visibility) <> (WeatherEntity.tupled, WeatherEntity.unapply)
  }


  def insert(weather: WeatherEntity) = {

  }
}
