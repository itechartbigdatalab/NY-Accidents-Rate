package com.itechart.ny_accidents.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Spark {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf: SparkConf = new SparkConf().setAppName("ny_spark").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sparkSql: SparkSession = SparkSession.builder
    .master("local")
    .appName("ny_sql")
    .getOrCreate

  sc.setLogLevel("OFF")
}
