package com.itechart.ny_accidents

import com.google.inject.AbstractModule
import com.itechart.ny_accidents.database.dao.cache.{EhCacheDAO, MergedDataCacheDAO}
import slick.jdbc.{JdbcProfile, PostgresProfile}

class GuiceModule extends AbstractModule {

  override def configure(): Unit = {
//    bind(classOf[JdbcProfile]).to(classOf[PostgresProfile])
    bind(classOf[MergedDataCacheDAO]).to(classOf[EhCacheDAO])
    super.configure()
  }

}
