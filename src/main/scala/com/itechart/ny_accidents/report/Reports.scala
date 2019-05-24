package com.itechart.ny_accidents.report

import com.google.inject.Singleton
import org.apache.spark.rdd.RDD

@Singleton
class Reports {

  def generateReportForTupleRDD[A <: Product](data: RDD[A]): Seq[Seq[String]] = {
    data.collect().map(obj => obj.productIterator.toSeq.map(_.toString))
  }

}
