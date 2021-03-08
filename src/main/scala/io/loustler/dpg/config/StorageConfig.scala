package io.loustler.dpg.config

import io.loustler.dpg.config.StorageConfig.DataLakeConfig

final case class StorageConfig(dataLake: DataLakeConfig)

object StorageConfig {

  final case class DataLakeConfig(name: String, endpoint: Option[String]) {
    val path = s"s3a://$name"
  }

  final case class DataWarehouseConfig(name: String)
}
