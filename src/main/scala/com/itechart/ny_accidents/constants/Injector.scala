package com.itechart.ny_accidents.constants

import com.google.inject.{Guice, Injector}
import com.itechart.ny_accidents.GuiceModule

object Injector {
  lazy val injector: Injector = Guice.createInjector(new GuiceModule)
}
