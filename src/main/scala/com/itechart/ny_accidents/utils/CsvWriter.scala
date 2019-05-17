package com.itechart.ny_accidents.utils

import com.github.tototoshi.csv.CSVWriter

import scala.collection.immutable.HashMap
import scala.concurrent.{ExecutionContext, Future}

object CsvWriter {

  case class Answer(description: String, answer: Any)
  implicit val ec: ExecutionContext = ExecutionContext.global

  def writePlantsInEachContinent(answer: HashMap[String, Int], outputFile: String , header: List[String] = List("Continent", "NumberOfPlants")): Future[Boolean] = {
    val writer = CSVWriter.open(outputFile)
    writer.writeRow(header)
    writer.writeAll(answer.toSeq.map(value => List(value._1, value._2)))
    writer.close()
    Future(true)
  }

  def writeGeneralStat(answer: Seq[Answer], outputFile: String , header: List[String] = List("Description", "Answer")): Future[Boolean]  = {
    val writer = CSVWriter.open(outputFile)
    writer.writeRow(header)
    writer.writeAll(answer.map(value=>List(value.description, value.answer)))
    writer.close()
    Future(true)
  }


}
