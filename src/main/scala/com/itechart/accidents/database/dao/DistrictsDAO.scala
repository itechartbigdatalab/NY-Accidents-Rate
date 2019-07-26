package com.itechart.accidents.database.dao

import com.google.inject.Singleton
import com.itechart.accidents.entity.DistrictWithGeometry
import com.vividsolutions.jts.geom.Geometry
import slick.jdbc.{JdbcProfile, PostgresProfile}
import slick.lifted

@Singleton
class DistrictsDAO() {

  val profile: JdbcProfile = PostgresProfile

  private lazy val TABLE_NAME = "district"
  private lazy val query = lifted.TableQuery[DistrictsTable]

  import com.itechart.accidents.database.ExtendedPostgresDriver.api._
  private class DistrictsTable(tag: Tag) extends Table[DistrictWithGeometry](tag, TABLE_NAME) {
    val districtName = column[String]("nta_name", O.PrimaryKey, O.Length(60))
    val boroughName = column[String]("borough_name", O.Length(60))
    val geometry = column[Geometry]("geom")

    def * = (districtName, boroughName, geometry) <> (DistrictWithGeometry.tupled, DistrictWithGeometry.unapply)
  }

  def all(): DBIO[Seq[DistrictWithGeometry]] = query.result
}
