package io.loustler.dpg.config

import pureconfig.ConfigReader
import software.amazon.awssdk.regions.Region

final case class AwsConfig(region: Region, accessKey: String, secretAccessKey: String, endpoint: Option[String])

object AwsConfig {

  implicit val ConfigReaderForAwsConfig: ConfigReader[AwsConfig] =
    ConfigReader.fromCursor { cursor =>
      for {
        obj <- cursor.asObjectCursor

        regionCursor <- obj.atKey("region")
        regionStr    <- regionCursor.asString
        region = Region.of(regionStr)

        accessKeyCursor <- obj.atKey("access-key")
        accessKey       <- accessKeyCursor.asString

        secretAccessKeyCursor <- obj.atKey("secret-access-key")
        secretAccessKey       <- secretAccessKeyCursor.asString

        endpoint = obj.atKeyOrUndefined("endpoint") match {
          case endpointCursor if endpointCursor.isUndefined || endpointCursor.isNull => None
          case endpointCursor                                                        => endpointCursor.asString.toOption
        }
      } yield AwsConfig(
        region = region,
        accessKey = accessKey,
        secretAccessKey = secretAccessKey,
        endpoint = endpoint
      )
    }
}
