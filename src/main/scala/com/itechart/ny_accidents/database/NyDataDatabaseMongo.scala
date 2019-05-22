package com.itechart.ny_accidents.database

import com.itechart.ny_accidents.constants.Configuration
import org.mongodb.scala.{MongoClient, MongoDatabase}

@Deprecated
object NyDataDatabaseMongo {
  private final val DATABASE = "ny_data"

  private lazy val client: MongoClient = MongoClient(Configuration.MONGO_HOST
    + "/?waitQueueMultiple=" + Configuration.WAIT_QUEUE_SIZE_MONGO)

  lazy val database: MongoDatabase = client.getDatabase(DATABASE)
}
