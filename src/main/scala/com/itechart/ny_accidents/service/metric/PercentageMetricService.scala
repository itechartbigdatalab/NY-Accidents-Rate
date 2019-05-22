package com.itechart.ny_accidents.service.metric

import org.apache.spark.rdd.RDD

class PercentageMetricService {

  protected def calculatePercentage[A, B](data: RDD[(B, Iterable[A])], dataLength: Long): RDD[(B, Int, Double)] = {
    val statsMap: RDD[(B, Int)] = data.map(tuple => (tuple._1, tuple._2.size))
    statsMap.map(tuple => (tuple._1, tuple._2, (tuple._2.toDouble / dataLength.toDouble) * 100.0))
  }

}
