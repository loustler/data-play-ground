package io.loustler.dpg.testing

import com.typesafe.config.{ Config, ConfigFactory }
import io.loustler.dpg.config.AppConfig
import org.scalatest.enablers.Emptiness
import org.scalatest.{ Inside, Inspectors, OptionValues }
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.Whenever

trait TestingSpec extends Matchers with OptionValues with Inside with Inspectors with Whenever {
  def loadConfig(): Config = ConfigFactory.load("test")

  def loadAppConfig(): AppConfig = AppConfig.load(loadConfig())

  implicit def MapEmptiness[K, V]: Emptiness[Map[K, V]] =
    new Emptiness[Map[K, V]] {
      override def isEmpty(thing: Map[K, V]): Boolean = thing.isEmpty
    }
}
