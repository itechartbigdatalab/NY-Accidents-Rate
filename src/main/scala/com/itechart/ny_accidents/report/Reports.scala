package com.itechart.ny_accidents.report

import com.google.inject.Singleton
import com.itechart.ny_accidents.spark.Spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

@Singleton
class Reports {
  private lazy val PK_DATAFRAME_NAME = "id"

  def generateReportForTupleRDD[A <: Product](data: RDD[A], header: Seq[String]): Seq[Seq[String]] = {
    header +: data.collect().map(obj => obj.productIterator.toSeq.map(_.toString))
  }

  def generateDataFrameReportForTupleRDD[A <: Product](data: RDD[A], schema: StructType): DataFrame = {
    val rdd = data.map(Row.fromTuple)
    Spark.sparkSql.createDataFrame(rdd, schema)
      .withColumn(PK_DATAFRAME_NAME,monotonically_increasing_id())
  }
}
