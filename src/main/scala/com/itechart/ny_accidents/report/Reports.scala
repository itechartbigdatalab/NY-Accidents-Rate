package com.itechart.ny_accidents.report

import com.google.inject.Singleton
import org.apache.spark.rdd.RDD

@Singleton
class Reports {

  def generateReportString(data: Map[String, Long]): Seq[Seq[String]] = {
    data.map(obj => Seq(obj._1.toString, obj._2.toString)).toSeq
  }

  def generateReportString[A,B](data: RDD[(A, B)]): Seq[Seq[String]] = {
    data.map(obj => Seq(obj._1.toString, obj._2.toString)).collect().toSeq
  }

}
