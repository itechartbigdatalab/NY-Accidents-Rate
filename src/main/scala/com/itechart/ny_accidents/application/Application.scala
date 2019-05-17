package com.itechart.ny_accidents.application

import com.itechart.ny_accidents.merge.Merger
import com.itechart.ny_accidents.utils.CsvReader
import com.typesafe.config.ConfigFactory


object Application extends App {

  val filesConfig = ConfigFactory.load("app.conf")
  val pathToDataFolder = filesConfig.getString("file.inputPath")
  val inputFileAccidents = pathToDataFolder + filesConfig.getString("file.input.inputFileNYAccidents")
  val raw = CsvReader.readData(inputFileAccidents)
  val mergedData = Merger(raw)
  val q = mergedData.take(8)
  println(mergedData.size)
  //raw.foreach(println)
  println(raw.size)
  println(q)
}
