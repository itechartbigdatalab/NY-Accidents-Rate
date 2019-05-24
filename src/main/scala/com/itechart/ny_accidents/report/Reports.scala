package com.itechart.ny_accidents.report

import com.google.inject.Singleton
import org.apache.spark.rdd.RDD

@Singleton
class Reports {

  def generateReportForTupleRDD[A <: Product](data: RDD[A], header: Seq[String]): Seq[Seq[String]] = {
    header +: data.collect().map(obj => obj.productIterator.toSeq.map(_.toString))
  }

}
