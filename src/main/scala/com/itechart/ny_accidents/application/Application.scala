package com.itechart.ny_accidents.application

import com.itechart.ny_accidents.districts.controller.DistrictsDatabase
import com.itechart.ny_accidents.districts.database.DistrictsDao
import com.itechart.ny_accidents.districts.parser.DistrictsParser
import com.itechart.ny_accidents.utils.PostgisUtils
import slick.dbio.DBIO

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

object Application extends App {
  implicit val ec:ExecutionContext = ExecutionContext.global
  val districtsParser = new DistrictsParser
  val districts = districtsParser.parseCsv("/home/uladzimir/ny_accidents_resources/nynt.csv")

  districts.map(dist => (dist.boroughName, dist.districtName)).foreach(println)

  val districtsDao = new DistrictsDao
  val insertFuture: Future[Seq[Int]] = Future.sequence(districts.map(districtsDao.insert).map(DistrictsDatabase.database.run))

  Await.result(insertFuture, Duration.Inf)
}
