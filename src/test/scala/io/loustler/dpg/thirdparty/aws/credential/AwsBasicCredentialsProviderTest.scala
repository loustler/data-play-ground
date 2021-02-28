package io.loustler.dpg.thirdparty.aws.credential

import io.loustler.dpg.config.AwsConfig
import io.loustler.dpg.testing.FunSpec
import software.amazon.awssdk.regions.Region

final class AwsBasicCredentialsProviderTest extends FunSpec {
  describe("AwsBasicCredentialsProvider") {
    describe("resolveCredentials") {
      it("Success resolve credentials given config is valid") {
        val accessKey       = "my-access-key"
        val secretAccessKey = "my-secret-key"

        val config = AwsConfig(
          region = Region.AP_NORTHEAST_2,
          accessKey = accessKey,
          secretAccessKey = secretAccessKey,
          endpoint = None
        )

        val provider = AwsBasicCredentialsProvider.fromConfig(config)

        provider.resolveCredentials().accessKeyId() should ===(accessKey)
        provider.resolveCredentials().secretAccessKey() should ===(secretAccessKey)
      }
      it("Throw NPE when given access key and secret access key is empty string") {
        val accessKey       = ""
        val secretAccessKey = ""

        val config = AwsConfig(
          region = Region.AP_NORTHEAST_2,
          accessKey = accessKey,
          secretAccessKey = secretAccessKey,
          endpoint = None
        )

        val provider = AwsBasicCredentialsProvider.fromConfig(config)

        assertThrows[NullPointerException](provider.resolveCredentials().accessKeyId())
        assertThrows[NullPointerException](provider.resolveCredentials().secretAccessKey())
      }
    }
  }
}
