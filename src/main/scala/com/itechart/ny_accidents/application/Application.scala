package com.itechart.ny_accidents.application

import com.itechart.ny_accidents.utils.CsvReader
import com.itechart.ny_accidents.utils.AccidentsUtils._
import com.typesafe.config.ConfigFactory

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



object Application extends App {

  val conf = new SparkConf().setAppName("main").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val filesConfig = ConfigFactory.load("app.conf")
  val pathToDataFolder = filesConfig.getString("file.inputPath")
  val inputFileAccidents = pathToDataFolder + filesConfig.getString("file.input.inputFileNYAccidents")
  val raw = sc.parallelize(CsvReader.readData(inputFileAccidents))

}
