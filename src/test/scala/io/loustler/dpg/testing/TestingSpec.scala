package io.loustler.dpg.testing

import com.typesafe.config.{ Config, ConfigFactory }
import io.loustler.dpg.config.AppConfig
import org.scalatest.{ Inside, Inspectors, OptionValues }
import org.scalatest.matchers.should.Matchers

trait TestingSpec extends Matchers with OptionValues with Inside with Inspectors {
  def loadConfig: Config = ConfigFactory.load("test")

  def loadAppConfig: AppConfig = AppConfig.load(loadConfig)
}
