package com.itechart.ny_accidents.database.dao

import com.google.inject.Singleton
import com.itechart.ny_accidents.entity.DistrictWithGeometry
import com.vividsolutions.jts.geom.Geometry
import slick.jdbc.{JdbcProfile, PostgresProfile}
import slick.lifted

@Singleton
class DistrictsDAO() {
//  private lazy val DB_TABLE_NAME = "district"
//
//  import Spark.sparkSql.implicits._
//
//  Spark.sparkSql.read
//    .format("jdbc")
//    .option("header", "true")
//    .option("inferSchema", "true")
//    .option("driver", NY_DATA_DATABASE_DRIVER)
//    .option("url", NY_DATA_DATABASE_URL)
//    .option("user", NY_DATA_DATABASE_USER)
//    .option("password", NY_DATA_DATABASE_PASSWORD)
//    .option("dbtable", s"(SELECT nta_name, borough_name, st_astext(geom) as geom FROM $DB_TABLE_NAME) as $DB_TABLE_NAME")
//    .load()
//    .createOrReplaceTempView(DB_TABLE_NAME)
//
//  lazy val districtsDatasetWithoutGeometry: Dataset[(String, DistrictWithoutGeometry)] = Spark.sparkSql.sql(
//    s"SELECT nta_name, borough_name FROM $DB_TABLE_NAME")
//    .as[(String, String)]
//    .map { case (districtName, boroughName) =>
//      (districtName, DistrictWithoutGeometry(districtName, boroughName))
//    }
//
  val profile: JdbcProfile = PostgresProfile

  private lazy val TABLE_NAME = "district"
  private lazy val query = lifted.TableQuery[DistrictsTable]

  import com.itechart.ny_accidents.database.ExtendedPostgresDriver.api._
  private class DistrictsTable(tag: Tag) extends Table[DistrictWithGeometry](tag, TABLE_NAME) {
    val districtName = column[String]("nta_name", O.PrimaryKey, O.Length(60))
    val boroughName = column[String]("borough_name", O.Length(60))
    val geometry = column[Geometry]("geom")

    def * = (districtName, boroughName, geometry) <> (DistrictWithGeometry.tupled, DistrictWithGeometry.unapply)
  }

  def all(): DBIO[Seq[DistrictWithGeometry]] = query.result
}
