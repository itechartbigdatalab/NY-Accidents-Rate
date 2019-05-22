package com.itechart.ny_accidents.constants

import com.typesafe.config.ConfigFactory

object Configuration {
  private lazy val appConf = ConfigFactory.load("application.conf")
  private lazy val cacheConf = ConfigFactory.load("conf/cache.conf")
  private lazy val databaseConf = ConfigFactory.load("conf/ny_data_database.conf")

  lazy val CACHE_NAME: String = cacheConf.getString("cache.name")
  lazy val CACHE_PATH: String = s"/home/${System.getProperty("user.name")}/${CACHE_NAME}"
  lazy val CACHE_DISK_SIZE: Int = cacheConf.getInt("cache.disk_size_gb")
  lazy val CACHE_HEAP_SIZE: Int = cacheConf.getInt("cache.heap_size_units")
  lazy val CACHE_OFF_HEAP_SIZE: Int = cacheConf.getInt("cache.off_head_size_gb")
  lazy val DATA_FILE_PATH: String = appConf.getString("file.inputPath") +
    appConf.getString("file.input.inputFileNYAccidents")

  lazy val NY_DATA_DATABASE_URL: String = databaseConf.getString("postgreConf.url")
  lazy val NY_DATA_DATABASE_DRIVER: String = databaseConf.getString("postgreConf.driver")
  lazy val NY_DATA_DATABASE_USER: String = databaseConf.getString("postgreConf.user")
  lazy val NY_DATA_DATABASE_PASSWORD: String = databaseConf.getString("postgreConf.password")
  lazy val NY_DATA_DATABASE_CONNECTION_POOL: String = databaseConf.getString("postgreConf.connectionPool")
  lazy val NY_DATA_DATABASE_KEEP_ALIVE_CONNECTION: Boolean = databaseConf.getBoolean("postgreConf.keepAliveConnection")

  lazy val NYNTA_PATH: String = appConf.getString("file.nynta_path")

  lazy val REDIS_HOST: String = "127.0.0.1"
  lazy val REDIS_PORT: Int = 6379
  lazy val REDIS_POOL_SIZE: Int = 128

  lazy val MONGO_HOST: String = "mongodb://localhost:27017"
  lazy val WAIT_QUEUE_SIZE_MONGO: Int = 100
}