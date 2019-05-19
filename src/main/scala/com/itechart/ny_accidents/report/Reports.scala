package com.itechart.ny_accidents.report

class Reports {
  def generateReportString(data: Map[Any, Any]): Seq[Seq[String]] = {
    data.map(obj => Seq(obj._1.toString, obj._2.toString)).toSeq
  }
}
