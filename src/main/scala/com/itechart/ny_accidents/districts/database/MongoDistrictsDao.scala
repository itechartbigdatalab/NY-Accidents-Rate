package com.itechart.ny_accidents.districts.database

import com.itechart.ny_accidents.database.NyDataDatabaseMongo
import com.mongodb.client.model.geojson.{Point, Position}
import org.mongodb.scala.Document
import org.mongodb.scala.model.Filters

import scala.concurrent.Future

object MongoDistrictsDao {
  private final val COLLECTION_NAME = "districts"
  private lazy val collection = NyDataDatabaseMongo.database.getCollection(COLLECTION_NAME)

  def getByCoordinates(position: Position): Future[Option[Document]] = {
    collection.find(Filters.geoIntersects("geom", new Point(position))).headOption()
  }


}
