package com.itechart.ny_accidents.service

import com.google.inject.Singleton
import com.itechart.ny_accidents.entity.{District, DistrictMongo}
import com.itechart.ny_accidents.utils.{PostgisUtils, StringUtils}
import com.mongodb.client.model.geojson.Position
import com.itechart.ny_accidents.database.dao.MongoDistrictsDAO

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

@Singleton
class DistrictsService {
  private implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  private lazy val MINIMUM_ACCEPTABLE_VALUE = 70.0

  @Deprecated
  // Will be deleted, do not use it!
  def getDistrict(latitude: Double, longitude: Double): Future[Option[DistrictMongo]] = {
    val mongoDao = new MongoDistrictsDAO

    val position = new Position(latitude, longitude)
    val document = mongoDao.getByCoordinates(position)
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

  def getDistrict(districtName: String, districts: Seq[District]): Option[District] = {
    districts.find(_.districtName.equalsIgnoreCase(districtName)) match {
      case Some(value) => Some(value)
      case None =>
        Try(districts.map(dist =>
          (dist,
            StringUtils.getLineMatchPercentage(
              dist.districtName, districtName
            ))).maxBy(_._2)._1).toOption
    }
  }
}
