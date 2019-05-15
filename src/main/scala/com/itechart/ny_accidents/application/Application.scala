package com.itechart.ny_accidents.application


import com.itechart.ny_accidents.utils.CsvReader
import com.itechart.ny_accidents.utils.AccidentsUtils._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



object Application extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("main").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("OFF")
  val filesConfig = ConfigFactory.load("app.conf")
  val pathToDataFolder = filesConfig.getString("file.inputPath")
  val inputFileAccidents = pathToDataFolder + filesConfig.getString("file.input.inputFileNYAccidents")
  val raw = sc.parallelize(CsvReader.readData(inputFileAccidents))

  println(raw.count())
}
