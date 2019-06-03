package com.itechart.ny_accidents.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Spark {
  val conf: SparkConf = new SparkConf().setAppName("ny_spark").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sparkSql: SparkSession = SparkSession.builder
    .master("local")
    .appName("ny_sql")
    .getOrCreate()
//  implicit val districtEncoder: Encoder[AccidentWithoutOption] = Encoders.kryo[AccidentWithoutOption]
//  implicit val superEncoder: Encoder[((Int, Long), AccidentWithoutOption)] = Encoders.kryo[((Int, Long), AccidentWithoutOption)]

}
