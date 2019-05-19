package com.itechart.ny_accidents.utils

import java.io.{BufferedWriter, FileWriter}

import com.github.tototoshi.csv.CSVWriter

object FileWriterUtils {
  def writeToCsv(data: Seq[Seq[String]], path: String): Unit = {
    CSVWriter.open(path).writeAll(data)
  }
}
