package com.itechart.ny_accidents.merge

import com.itechart.ny_accidents.districts.controller.DistrictsDatabase
import com.itechart.ny_accidents.districts.database.DistrictsDao
import com.itechart.ny_accidents.entity.{AccidentsNY, MergedData}
import com.itechart.ny_accidents.utils.PostgisUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Merger {

  def apply(accidents: RDD[AccidentsNY.RawAccidentsNY], sc: SparkContext):  RDD[MergedData] ={

    val q = accidents.map(mapper)
      q

  }

  private def mapper(value: AccidentsNY.RawAccidentsNY): MergedData ={
    val dao = new DistrictsDao
    val fut = DistrictsDatabase.database.run(dao.getByCoordinates(PostgisUtils.createPoint(value.latitude.getOrElse(0), value.longitude.getOrElse(0))))
    val district = Await.result(fut, Duration.Inf)
    MergedData(value, district)
  }
}
