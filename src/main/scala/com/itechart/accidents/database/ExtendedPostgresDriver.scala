package com.itechart.accidents.database

import com.github.tminglei.slickpg.{ExPostgresProfile, PgArraySupport, PgEnumSupport, PgPostGISSupport}

trait ExtendedPostgresDriver extends ExPostgresProfile
  with PgArraySupport
  with PgEnumSupport
  with PgPostGISSupport {

  override val api: API with ArrayImplicits with PostGISImplicits = new API with ArrayImplicits
    with PostGISImplicits {}
}

object ExtendedPostgresDriver extends ExtendedPostgresDriver