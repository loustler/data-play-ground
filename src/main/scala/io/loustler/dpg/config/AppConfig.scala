package io.loustler.dpg.config

import com.typesafe.config.Config
import pureconfig._
import pureconfig.generic.auto._

final case class AppConfig(storage: StorageConfig)

object AppConfig {
  def load(config: Config): AppConfig = ConfigSource.fromConfig(config).at("play-ground").loadOrThrow[AppConfig]
}
