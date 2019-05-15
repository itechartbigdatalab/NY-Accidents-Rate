package com.itechart.ny_accidents.districts.controller

import com.github.tminglei.slickpg.{ExPostgresProfile, PgArraySupport, PgEnumSupport, PgPostGISSupport}

trait ExtendedPostgresDriver extends ExPostgresProfile
  with PgArraySupport
  with PgEnumSupport
  with PgPostGISSupport {

  override val api = new API with ArrayImplicits
    with PostGISImplicits {}
}

object ExtendedPostgresDriver extends ExtendedPostgresDriver