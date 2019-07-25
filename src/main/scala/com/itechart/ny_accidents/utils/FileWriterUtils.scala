package com.itechart.ny_accidents.utils

import com.github.tototoshi.csv.CSVWriter
import com.itechart.ny_accidents.spark.Spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object FileWriterUtils {
  def writeToCsv(data: Seq[Seq[String]], path: String): Unit = {
    if(!path.endsWith(".csv")) {
      writeToCsv(data, path + ".csv")
    } else {
      CSVWriter.open(path).writeAll(data)
    }
  }

  def createNewCsv(path: String, rows: Array[Row], schema: StructType): Unit = {
    val dataFrame = Spark.sparkSql.createDataFrame(Spark.sc.parallelize(rows), schema)
    dataFrame.coalesce(1).write
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
      .option("header", "true")
      .csv(path)
  }
}
