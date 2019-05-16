package com.itechart.ny_accidents.weather

import com.itechart.ny_accidents.districts.controller.{DistrictsDatabase, ExtendedPostgresDriver}
import com.itechart.ny_accidents.entity.WeatherForAccident
import com.itechart.ny_accidents.weather.parser.WeatherParser
import slick.dbio.{DBIOAction, Effect, NoStream}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

object Application extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

  //  val DATA = "/home/aliaksandr/Downloads/Linden.csv"
  val weatherDAO = new WeatherDAO()
  //  val parser = new WeatherParser()
  //  val list = parser.parseCsv(DATA)
  //  println(list.size)
  //  val future = DistrictsDatabase.database.run(weatherDAO.insert(list))

  val allStations = Await.result(DistrictsDatabase.database.run(weatherDAO.allStations()), Duration.Inf)
  val allWeather = Await.result(DistrictsDatabase.database.run(weatherDAO.allWeather()), Duration.Inf)

  println(allWeather.size)
  //  val dbioList: DBIOAction[Vector[Vector[(Double, Double, Double, String, Double, Double)]], NoStream, Effect.All] =
  //    ExtendedPostgresDriver.api.DBIO.sequence(List.range(1, 1500000)
  //      .map(_ => weatherDAO.weatherByCoordinatesAndTime(1348628100000L, 40.0, -72.0)).toVector)
  //
  //  val dbioFuture: Future[Vector[Vector[(Double, Double, Double, String, Double, Double)]]] = DistrictsDatabase.database.run(dbioList)
  //
  //  Await.result(dbioFuture, Duration.Inf).head.head <> (WeatherForAccident.tupled, WeatherForAccident.unapply)

}
