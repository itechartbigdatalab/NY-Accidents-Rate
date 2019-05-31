package com.itechart.ny_accidents.database.dao

import com.google.inject.Singleton
import com.itechart.ny_accidents.constants.Configuration._
import com.itechart.ny_accidents.entity.District
import com.itechart.ny_accidents.spark.Spark
import com.itechart.ny_accidents.utils.PostgisUtils
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.{Dataset, Encoder, Encoders}

@Singleton
class DistrictsDAO() {
  private lazy val DB_TABLE_NAME = "district"
  import Spark.sparkSql.implicits._

  Spark.sparkSql.read
    .format("jdbc")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("driver", NY_DATA_DATABASE_DRIVER)
    .option("url", NY_DATA_DATABASE_URL)
    .option("user", NY_DATA_DATABASE_USER)
    .option("password", NY_DATA_DATABASE_PASSWORD)
    .option("dbtable", s"(SELECT nta_name, borough_name, st_astext(geom) as geom FROM $DB_TABLE_NAME) as $DB_TABLE_NAME")
    .load()
    .withColumnRenamed("nta_name", "districtName")
    .withColumnRenamed("borough_name", "boroughName")
    .withColumnRenamed("geom", "geometry")
    .createOrReplaceTempView(DB_TABLE_NAME)

  implicit val districtEncoder: Encoder[District] = Encoders.kryo[District]
  implicit val geometryEncoder: Encoder[Geometry] = Encoders.kryo[Geometry]

  lazy val districtsDataset: Dataset[District] = Spark.sparkSql.sql(
    s"SELECT districtName, boroughName, geometry FROM $DB_TABLE_NAME")
    .as[(String, String, String)]
    .map { case (districtName, boroughName, wktGeometry) =>
      District(districtName, boroughName, PostgisUtils.getGeometryFromText(wktGeometry))
    }

  // TODO Collect will be removed when we migrate all code to spark sql.
  // Now it's just to support old code
  def all(): Seq[District] = districtsDataset.collect()

}
