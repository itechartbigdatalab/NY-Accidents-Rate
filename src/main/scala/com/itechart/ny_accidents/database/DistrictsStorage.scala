package com.itechart.ny_accidents.database

import com.google.inject.{Inject, Singleton}
import com.itechart.ny_accidents.database.dao.DistrictsDAO
import com.itechart.ny_accidents.entity.District
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

@Singleton
class DistrictsStorage @Inject()(districtsDAO: DistrictsDAO) {
  lazy val districts: Seq[District] = Await.result(
    NYDataDatabase.database.run(districtsDAO.alldb()), Duration.Inf)
}