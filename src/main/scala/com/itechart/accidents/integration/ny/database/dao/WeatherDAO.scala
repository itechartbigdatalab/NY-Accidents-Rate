package com.itechart.accidents.integration.ny.database.dao

import com.google.inject.Singleton
import com.itechart.accidents.constants.Configuration.{NY_DATA_DATABASE_PASSWORD, NY_DATA_DATABASE_URL, NY_DATA_DATABASE_USER}
import com.itechart.accidents.database.ExtendedPostgresDriver.api._
import com.itechart.accidents.entity.{WeatherEntity, WeatherStation}
import com.itechart.accidents.spark.Spark.sparkSql
import com.vividsolutions.jts.geom.Point
import org.apache.spark.sql.{Dataset, Row}
import slick.dbio.Effect
import slick.jdbc.{JdbcProfile, PostgresProfile}
import slick.sql.FixedSqlAction

@Singleton
class WeatherDAO() {

  val profile: JdbcProfile = PostgresProfile

  private lazy val WEATHER_TABLE_NAME = "weather"
  private lazy val STATION_TABLE_NAME = "station"
  private lazy val weatherQuery = TableQuery[WeatherEntities]
  private lazy val stationQuery = TableQuery[Stations]

  def loadAllWeatherSpark(): Dataset[Row] = {
    sparkSql.read
      .format("jdbc")
      .option("url", NY_DATA_DATABASE_URL)
      .option("dbtable", WEATHER_TABLE_NAME)
      .option("user", NY_DATA_DATABASE_USER)
      .option("password", NY_DATA_DATABASE_PASSWORD)
      .load()
  }


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

  def allWeather(): DBIO[Seq[WeatherEntity]] = weatherQuery.result

  def allStations(): DBIO[Seq[WeatherStation]] = stationQuery.result

  def insert(weather: WeatherEntity): DBIO[Int] = weatherQuery += weather

  def insert(weathers: Seq[WeatherEntity]): FixedSqlAction[Option[Int], NoStream, Effect.Write] = weatherQuery ++= weathers

  def insert(station: WeatherStation): DBIO[Int] = stationQuery += station

}
