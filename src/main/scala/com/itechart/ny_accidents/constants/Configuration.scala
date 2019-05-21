package com.itechart.ny_accidents.constants

import com.typesafe.config.ConfigFactory

object Configuration {
  private val appConf = ConfigFactory.load("app.conf")
  private val cacheConf = ConfigFactory.load("conf/cache.conf")

  val CACHE_NAME: String = cacheConf.getString("cache.name")
  val CACHE_PATH: String = "/home/" +
    System.getProperty("user.name") + "/" + CACHE_NAME
  val CACHE_DISK_SIZE: Int = cacheConf.getInt("cache.disk_size")
}
