package io.loustler.dpg.config

import com.typesafe.config.ConfigFactory
import io.loustler.dpg.testing.FunSuite

final class AppConfigTest extends FunSuite {
  test("Success load app config using test conf") {
    val config = loadConfig()

    val appConfig = AppConfig.load(config)

    appConfig.storage.dataLake.name should not be empty
    appConfig.aws.accessKey should not be empty
    appConfig.aws.secretAccessKey should not be empty
  }

  test("Success load app config using reference conf") {
    val config = ConfigFactory.load()

    val appConfig = AppConfig.load(config)

    appConfig.storage.dataLake.name should not be empty
    appConfig.aws.accessKey should not be empty
    appConfig.aws.secretAccessKey should not be empty
  }
}
