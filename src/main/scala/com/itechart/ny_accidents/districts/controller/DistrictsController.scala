package com.itechart.ny_accidents.districts.controller

import com.itechart.ny_accidents.districts.database.MongoDistrictsDao
import com.itechart.ny_accidents.entity.{District, DistrictMongo}
import com.itechart.ny_accidents.utils.PostgisUtils
import com.mongodb.client.model.geojson.Position

import scala.concurrent.{ExecutionContext, Future}

class DistrictsController {
  private implicit val ec = ExecutionContext.global

  def getDistrict(latitude: Double, longitude: Double): Future[Option[DistrictMongo]] = {
    val position = new Position(latitude, longitude)
    val document = MongoDistrictsDao.getByCoordinates(position)
    document.map {
      case Some(value) =>
        Some(DistrictMongo(value("nta_name").toString, value("borough_name").toString))
      case _ =>
        None
    }
  }

  def getDistrict(latitude: Double, longitude: Double, districts: Seq[District]): Option[District] = {
    val point = PostgisUtils.createPoint(latitude, longitude)
    districts.find(_.geometry.contains(point))
  }
}
