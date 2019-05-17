package com.itechart.ny_accidents.application

import com.itechart.ny_accidents.entity.MergedData
import com.itechart.ny_accidents.merge.Merger
import com.itechart.ny_accidents.spark.Spark
import com.itechart.ny_accidents.utils.CsvReader
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD


object Application extends App {

  val filesConfig = ConfigFactory.load("app.conf")
  val pathToDataFolder = filesConfig.getString("file.inputPath")
  val inputFileAccidents = pathToDataFolder + filesConfig.getString("file.input.inputFileNYAccidents")

  val raws = CsvReader.readData(inputFileAccidents)
  val sc = Spark.sc
  val mergedData = sc.parallelize(Merger(raws))
  val q = mergedData.take(8)
  println(mergedData.count())
  println(raws.size)
  println(q)

  println(raws.size + "KISAKISA")

  //  val raws = Spark.sc.parallelize(CsvReader.readData(inputFileAccidents))
  //  val mergedData: RDD[MergedData] = Merger(raws)
  //  //  val q = mergedData.take(8)
  //  println(mergedData.count())
  //  println(q)
}
