package com.itechart.ny_accidents.districts.database

import com.itechart.ny_accidents.entity.District
import com.vividsolutions.jts.geom.{Geometry, MultiPolygon}
import slick.dbio.Effect
import slick.jdbc.{JdbcProfile, PostgresProfile}
import slick.sql.FixedSqlAction

class DistrictsDao(val profile: JdbcProfile = PostgresProfile) {
  private lazy val TABLE_NAME = "district"
  import com.itechart.ny_accidents.districts.controller.ExtendedPostgresDriver.api._
  private lazy val query = TableQuery[DistrictsTable]

  private class DistrictsTable(tag: Tag) extends Table[District](tag, TABLE_NAME) {
    val districtName = column[String]("nta_name", O.PrimaryKey, O.Length(60))
    val boroughName = column[String]("borough_name", O.Length(60))
    val geometry = column[Geometry]("geom")

    def * = (districtName, boroughName, geometry) <> (District.tupled, District.unapply)
  }

  def all(): DBIO[Seq[District]] = query.result

  def insert(dist: District): DBIO[Int] = query += dist

}
