package com.itechart.accidents.constants

import com.google.inject.{Guice, Injector}
import com.itechart.accidents.GuiceModule

object Injector {
  lazy val injector: Injector = Guice.createInjector(new GuiceModule)
}
