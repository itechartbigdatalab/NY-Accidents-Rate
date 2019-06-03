package com.itechart.ny_accidents.database.dao

import com.google.inject.Singleton
import com.itechart.ny_accidents.constants.Configuration._
import com.itechart.ny_accidents.entity.{District, DistrictWithoutGeometry}
import com.itechart.ny_accidents.spark.Spark
import com.itechart.ny_accidents.utils.PostgisUtils
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.{Dataset, Encoder, Encoders}

import com.vividsolutions.jts.geom.{Geometry, Point}
import slick.jdbc.{JdbcProfile, PostgresProfile}
import slick.lifted

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
    .createOrReplaceTempView(DB_TABLE_NAME)

  implicit val districtEncoder: Encoder[District] = Encoders.kryo[District]
  implicit val geometryEncoder: Encoder[Geometry] = Encoders.kryo[Geometry]

//  lazy val districtsDataset: Dataset[(String, District)] = Spark.sparkSql.sql(
//    s"SELECT nta_name, borough_name, geom FROM $DB_TABLE_NAME")
//    .as[(String, String, String)]
//    .map { case (districtName, boroughName, wktGeometry) =>
//      (districtName, District(districtName, boroughName, PostgisUtils.getGeometryFromText(wktGeometry)))
//    }

  lazy val districtsDatasetWithoutGeometry: Dataset[(String, DistrictWithoutGeometry)] = Spark.sparkSql.sql(
    s"SELECT nta_name, borough_name, geom FROM $DB_TABLE_NAME")
    .as[(String, String, String)]
    .map { case (districtName, boroughName, _) =>
      (districtName, DistrictWithoutGeometry(districtName, boroughName))
    }


  // TODO Collect will be removed when we migrate all code to spark sql.
  // Now it's just to support old code
//  def all(): Seq[District] = districtsDataset.map(_._2).collect()


  val profile: JdbcProfile = PostgresProfile

  private lazy val TABLE_NAME = "district"
  private lazy val query = lifted.TableQuery[DistrictsTable]

  import com.itechart.ny_accidents.database.ExtendedPostgresDriver.api._
  private class DistrictsTable(tag: Tag) extends Table[District](tag, TABLE_NAME) {
    val districtName = column[String]("nta_name", O.PrimaryKey, O.Length(60))
    val boroughName = column[String]("borough_name", O.Length(60))
    val geometry = column[Geometry]("geom")

    def * = (districtName, boroughName, geometry) <> (District.tupled, District.unapply)
  }

  def alldb(): DBIO[Seq[District]] = query.result

  def insert(dist: District): DBIO[Int] = query += dist

  def getByCoordinates(point: Point): DBIO[Option[District]] = query.filter(_.geometry.contains(point)).result.headOption

}
