package com.itechart.accidents

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, LogManager, Logger}


object TestSparkApi {
  lazy val spark: SparkContext = {
    new SparkContext(
      new SparkConf()
        .setAppName("Simple Application")
        .setMaster("local")
//      .set("spark.eventLog.enabled", "true")
//      .set("spark.eventLog.dir", "../tmp/logs")
    )
  }

}

