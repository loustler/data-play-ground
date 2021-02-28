package io.loustler.dpg.thirdparty.aws.credential

import io.loustler.dpg.config.AwsConfig
import io.loustler.dpg.testing.FunSuite
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.regions.Region

final class BasicAwsCredentialsProviderTest extends FunSuite {
  test("Success credential if given config is valid") {
    val accessKey       = "my-access-key"
    val secretAccessKey = "my-secret-key"

    val config = AwsConfig(
      region = Region.AP_NORTHEAST_2,
      accessKey = accessKey,
      secretAccessKey = secretAccessKey,
      endpoint = None
    )

    val provider = BasicAwsCredentialsProvider.fromConfig(config)

    val credential = provider.resolveCredentials()

    credential.accessKeyId() should ===(accessKey)
    credential.secretAccessKey() should ===(secretAccessKey)
  }

  test("Throw SdkClientException when try resolve credential if given access key is empty") {
    val accessKey       = ""
    val secretAccessKey = "my-secret-key"

    val config = AwsConfig(
      region = Region.AP_NORTHEAST_2,
      accessKey = accessKey,
      secretAccessKey = secretAccessKey,
      endpoint = None
    )

    val provider = BasicAwsCredentialsProvider.fromConfig(config)

    assertThrows[SdkClientException](provider.resolveCredentials())
  }

  test("Throw SdkClientException when try resolve credential if given secret access key is empty") {
    val accessKey       = "my-access-key"
    val secretAccessKey = ""

    val config = AwsConfig(
      region = Region.AP_NORTHEAST_2,
      accessKey = accessKey,
      secretAccessKey = secretAccessKey,
      endpoint = None
    )

    val provider = BasicAwsCredentialsProvider.fromConfig(config)

    assertThrows[SdkClientException](provider.resolveCredentials())
  }

  test("Throw SdkClientException when try resolve credential if given access key and secret access key is empty") {
    val accessKey       = ""
    val secretAccessKey = ""

    val config = AwsConfig(
      region = Region.AP_NORTHEAST_2,
      accessKey = accessKey,
      secretAccessKey = secretAccessKey,
      endpoint = None
    )

    val provider = BasicAwsCredentialsProvider.fromConfig(config)

    assertThrows[SdkClientException](provider.resolveCredentials())
  }
}
