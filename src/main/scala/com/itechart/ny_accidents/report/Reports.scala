package com.itechart.ny_accidents.report

import com.google.inject.Singleton
import org.apache.spark.rdd.RDD

@Singleton
class Reports {

  def generateReportString(data: Map[Any, Any]): Seq[Seq[String]] = {
    data.map(obj => Seq(obj._1.toString, obj._2.toString)).toSeq
  }

  def generateReportStringFor3Fields[A,B,C](data: RDD[(A, B, C)]): Seq[Seq[String]] = {
    data.map(obj => Seq(obj._1.toString, obj._2.toString, obj._3.toString)).collect().toSeq
  }

  def generateReportStringFor2Fields[A,B](data: RDD[(A, B)]): Seq[Seq[String]] = {
    data.map(obj => Seq(obj._1.toString, obj._2.toString)).collect().toSeq
  }

}
