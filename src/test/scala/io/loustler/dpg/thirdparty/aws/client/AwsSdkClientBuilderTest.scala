package io.loustler.dpg.thirdparty.aws.client

import io.loustler.dpg.testing.FunSpec
import software.amazon.awssdk.services.ssm.SsmClient

final class AwsSdkClientBuilderTest extends FunSpec {
  describe("AwsSdkClientBuilder") {
    describe("fromConfig") {
      it("Inject options into any client will be success") {
        val ssmBuilder = SsmClient.builder()

        val injectBuilder = AwsSdkClientBuilder.fromConfig(loadAppConfig().aws, ssmBuilder)

        injectBuilder should not equal (ssmBuilder)
      }
    }
  }
}
