package com.itechart.ny_accidents.database

import org.mongodb.scala.{MongoClient, MongoDatabase}

object NyDataDatabaseMongo {
  private final val MONGO_HOST = "mongodb://localhost:27017"
  private final val DATABASE = "ny_data"

  private lazy val client: MongoClient = MongoClient(MONGO_HOST + "/?waitQueueMultiple=100")

  lazy val database: MongoDatabase = client.getDatabase(DATABASE)
}
