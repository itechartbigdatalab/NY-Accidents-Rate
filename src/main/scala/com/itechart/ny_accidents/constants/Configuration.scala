package com.itechart.ny_accidents.constants

import com.typesafe.config.ConfigFactory

object Configuration {
  private val appConf = ConfigFactory.load("app.conf")
  private val cacheConf = ConfigFactory.load("conf/cache.conf")

  val POPULATION_DATASET_PATH = appConf.getString("file.population_dataset")

  val CACHE_NAME: String = cacheConf.getString("cache.name")
  val CACHE_PATH: String = "/home/" +
    System.getProperty("user.name") + "/" + CACHE_NAME
  val CACHE_DISK_SIZE: Int = cacheConf.getInt("cache.disk_size_gb")
  val CACHE_HEAP_SIZE: Int = cacheConf.getInt("cache.heap_size_units")
  val CACHE_OFF_HEAP_SIZE: Int = cacheConf.getInt("cache.off_head_size_gb")
}