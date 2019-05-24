package com.itechart.ny_accidents

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, LogManager, Logger}


object TestSparkApi {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("spark").setLevel(Level.OFF)
  LogManager.getRootLogger.setLevel(Level.OFF)

  lazy val spark: SparkContext = {
    new SparkContext(
      new SparkConf()
        .setAppName("Simple Application")
        .setMaster("local")
//      .set("spark.eventLog.enabled", "true")
//      .set("spark.eventLog.dir", "../tmp/logs")
    )
  }

  spark.setLogLevel("ERROR")

}

