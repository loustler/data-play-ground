package io.loustler.dpg.config

import com.typesafe.config.ConfigValueFactory
import io.loustler.dpg.testing.FunSuite

import scala.collection.JavaConverters._

final class AwsConfigTest extends FunSuite {
  test("Success read config whether endpoint is provide or not") {
    val regionValue     = "ap-northeast-2"
    val accessKey       = "my-access-key"
    val secretAccessKey = "my-secret-key"

    val configMap: Map[String, String] = Map(
      "region"            -> regionValue,
      "access-key"        -> accessKey,
      "secret-access-key" -> secretAccessKey
    )

    val configValue = ConfigValueFactory.fromMap(configMap.asJava)

    val config = AwsConfig.ConfigReaderForAwsConfig.from(configValue)

    config.map(_.region.id()) should ===(Right(regionValue))
    config.map(_.accessKey) should ===(Right(accessKey))
    config.map(_.secretAccessKey) should ===(Right(secretAccessKey))
    config.map(_.endpoint) should ===(Right(None))
  }

  test("Success read config when endpoint is provide") {
    val regionValue     = "ap-northeast-2"
    val accessKey       = "my-access-key"
    val secretAccessKey = "my-secret-key"
    val endpoint        = "http://localhost:4566"

    val configMap: Map[String, String] = Map(
      "region"            -> regionValue,
      "access-key"        -> accessKey,
      "secret-access-key" -> secretAccessKey,
      "endpoint"          -> endpoint
    )

    val configValue = ConfigValueFactory.fromMap(configMap.asJava)

    val config = AwsConfig.ConfigReaderForAwsConfig.from(configValue)

    config.map(_.region.id()) should ===(Right(regionValue))
    config.map(_.accessKey) should ===(Right(accessKey))
    config.map(_.secretAccessKey) should ===(Right(secretAccessKey))
    config.map(_.endpoint) should ===(Right(Some(endpoint)))
  }

  test("Throw IllegalArgumentException when region is not provided") {
    val regionValue     = ""
    val accessKey       = "my-access-key"
    val secretAccessKey = "my-secret-key"

    val configMap: Map[String, String] = Map(
      "region"            -> regionValue,
      "access-key"        -> accessKey,
      "secret-access-key" -> secretAccessKey
    )

    val configValue = ConfigValueFactory.fromMap(configMap.asJava)

    assertThrows[IllegalArgumentException](AwsConfig.ConfigReaderForAwsConfig.from(configValue))
  }

  test("Success read config whether access key is provide or not") {
    val regionValue     = "ap-northeast-2"
    val accessKey       = ""
    val secretAccessKey = "my-secret-key"

    val configMap: Map[String, String] = Map(
      "region"            -> regionValue,
      "access-key"        -> accessKey,
      "secret-access-key" -> secretAccessKey
    )

    val configValue = ConfigValueFactory.fromMap(configMap.asJava)

    val config = AwsConfig.ConfigReaderForAwsConfig.from(configValue)

    config.map(_.region.id()) should ===(Right(regionValue))
    config.map(_.accessKey) should ===(Right(accessKey))
    config.map(_.secretAccessKey) should ===(Right(secretAccessKey))
    config.map(_.endpoint) should ===(Right(None))
  }

  test("Success read config whether secret access key is provide or not") {
    val regionValue     = "ap-northeast-2"
    val accessKey       = "my-access-key"
    val secretAccessKey = ""

    val configMap: Map[String, String] = Map(
      "region"            -> regionValue,
      "access-key"        -> accessKey,
      "secret-access-key" -> secretAccessKey
    )

    val configValue = ConfigValueFactory.fromMap(configMap.asJava)

    val config = AwsConfig.ConfigReaderForAwsConfig.from(configValue)

    config.map(_.region.id()) should ===(Right(regionValue))
    config.map(_.accessKey) should ===(Right(accessKey))
    config.map(_.secretAccessKey) should ===(Right(secretAccessKey))
    config.map(_.endpoint) should ===(Right(None))
  }
}
