package com.itechart.ny_accidents.utils

import com.github.tototoshi.csv.CSVReader
import com.itechart.ny_accidents.entity.AccidentsNY.RawAccidentsNY
import com.itechart.ny_accidents.utils.AccidentsUtils.accidentsMapper

object CsvReader {
  def readData(fileName: String): Seq[RawAccidentsNY] = {
    val reader = CSVReader.open(fileName)
    val dataFromFile = reader.allWithHeaders()
    reader.close()
    dataFromFile.map(accidentsMapper)
  }
}
