package com.itechart.ny_accidents.application



import com.itechart.ny_accidents.merge.Merger
import com.itechart.ny_accidents.utils.CsvReader
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}



object Application extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("spark").setLevel(Level.OFF)
  val conf = new SparkConf().setAppName("main").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("OFF")
  val filesConfig = ConfigFactory.load("app.conf")
  val pathToDataFolder = filesConfig.getString("file.inputPath")
  val inputFileAccidents = pathToDataFolder + filesConfig.getString("file.input.inputFileNYAccidents")
  val raws = sc.parallelize(CsvReader.readData(inputFileAccidents))
  val mergedData = Merger(raws, sc)
  println(mergedData.count())
  println(raws.count())


}
