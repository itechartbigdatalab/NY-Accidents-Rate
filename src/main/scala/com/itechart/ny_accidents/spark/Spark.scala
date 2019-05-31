package com.itechart.ny_accidents.spark

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Spark {
  val conf: SparkConf = new SparkConf()
    .setAppName("ny_spark")
    .setMaster("local[*]")
    .registerKryoClasses(Array(classOf[Geometry]))

  val sc = new SparkContext(conf)
  val sparkSql: SparkSession = SparkSession.builder
    .master("local")
    .config(conf)
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .getOrCreate()

}
