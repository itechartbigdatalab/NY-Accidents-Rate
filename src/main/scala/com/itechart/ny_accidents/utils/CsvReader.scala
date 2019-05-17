package com.itechart.ny_accidents.utils

import com.github.tototoshi.csv.CSVReader
import com.itechart.ny_accidents.entity.AccidentsNY.RawAccidentsNY
import com.itechart.ny_accidents.spark.Spark
import com.itechart.ny_accidents.utils.AccidentsUtils.accidentsMapper
import org.apache.spark.sql.Row

object CsvReader {
  def readData(fileName: String): Seq[RawAccidentsNY] = {

    val csvAccidentsData: Array[Row] = Spark.sparkSql.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(fileName)
      .collect()

    csvAccidentsData.map(accidentsMapper)


//
//    val reader = CSVReader.open(fileName)
//    val dataFromFile: List[Map[String, String]] = reader.allWithHeaders()
//    reader.close()
//    dataFromFile.map(accidentsMapper)
  }
}
