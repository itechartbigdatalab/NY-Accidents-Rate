package com.itechart.ny_accidents

import com.google.inject.AbstractModule
import slick.jdbc.{JdbcProfile, PostgresProfile}

class GuiceModule extends AbstractModule {

  override def configure(): Unit = {
//    bind(classOf[JdbcProfile]).to(classOf[PostgresProfile])
    super.configure()
  }

}
