package com.itechart.accidents.database

import com.google.inject.{Inject, Singleton}
import com.itechart.accidents.database.dao.DistrictsDAO
import com.itechart.accidents.entity.{District, DistrictWithGeometry}
import com.itechart.accidents.spark.Spark
import org.apache.spark.sql.Dataset

import scala.concurrent.Await
import scala.concurrent.duration.Duration

@Singleton
class DistrictsStorage @Inject()(districtsDAO: DistrictsDAO) {
  lazy val districtsWithGeometry: Seq[DistrictWithGeometry] = Await.result(
    DataDatabase.database.run(districtsDAO.all()), Duration.Inf)

  lazy val districts: Seq[District] = districtsWithGeometry.map(dWithGeometry => District(dWithGeometry.districtName, dWithGeometry.boroughName))

  import Spark.sparkSql.implicits._
  val districtsDataSet: Dataset[District] = Spark.sparkSql.createDataFrame(districts)
    .as[District]
    .cache()
}