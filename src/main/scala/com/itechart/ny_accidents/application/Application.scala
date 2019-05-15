package com.itechart.ny_accidents.application

import com.itechart.ny_accidents.utils.CsvReader
import com.itechart.ny_accidents.utils.AccidentsUtils._
import com.typesafe.config.ConfigFactory
import java.time.LocalDate
import java.time.format.DateTimeFormatter


object Application extends App {

  val formatter = DateTimeFormatter.ofPattern("d/MM/yyyy")
  val date = "16/08/2016"
  val localDate = LocalDate.parse(date, formatter)

  val filesConfig = ConfigFactory.load("app.conf")
  val pathToDataFolder = filesConfig.getString("file.inputPath")
  val inputFileAccidents=pathToDataFolder+filesConfig.getString("file.input.inputFileNYAccidents")
  //val raw = CsvReader.readData(inputFileAccidents).map(accidentsMapper)
  
}
