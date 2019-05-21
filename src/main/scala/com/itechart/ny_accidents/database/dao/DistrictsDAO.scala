package com.itechart.ny_accidents.database.dao

import com.itechart.ny_accidents.entity.District
import com.vividsolutions.jts.geom.{Geometry, Point}
import slick.jdbc.{JdbcProfile, PostgresProfile}
import slick.lifted

@Deprecated
class DistrictsDAO() {

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

  def all(): DBIO[Seq[District]] = query.result

  def insert(dist: District): DBIO[Int] = query += dist

  def getByCoordinates(point: Point): DBIO[Option[District]] = query.filter(_.geometry.contains(point)).result.headOption
}
