package com.itechart.ny_accidents.report

import org.apache.spark.rdd.RDD

class Reports {
  def generateReportString(data: Map[Any, Any]): Seq[Seq[String]] = {
    data.map(obj => Seq(obj._1.toString, obj._2.toString)).toSeq
  }

  def generateReportString[A,B](data: RDD[(A, B)]): Seq[Seq[String]] = {
    data.map(obj => Seq(obj._1.toString, obj._2.toString)).collect().toSeq
  }

}
