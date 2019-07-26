package com.itechart.accidents.database.dao

import com.itechart.accidents.database.DataDatabaseMongo
import com.mongodb.client.model.geojson.Point
import org.mongodb.scala.Document
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.geojson.Position

import scala.concurrent.Future

@Deprecated
class MongoDistrictsDAO {
  private final val COLLECTION_NAME = "districts"
  private lazy val collection = DataDatabaseMongo.database.getCollection(COLLECTION_NAME)

  def getByCoordinates(position: Position): Future[Option[Document]] = {
    collection.find(Filters.geoIntersects("geom", new Point(position))).headOption()
  }


}
