package io.loustler.dpg.thirdparty.aws.client

import io.loustler.dpg.config.AwsConfig
import io.loustler.dpg.thirdparty.aws.credential.AwsBasicCredentialsProvider
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder

import java.net.URI

object AwsSdkClientBuilder {

  /** Inject some options for given [[AwsClientBuilder]].
    *
    * @param config aws config
    * @param builder aws client builder
    * @tparam A aws client builder type
    * @tparam B aws client type
    * @return aws client builder
    */
  def fromConfig[A <: AwsClientBuilder[A, B], B](config: AwsConfig, builder: AwsClientBuilder[A, B]): A = {
    val injectedBuilder = builder
      .region(config.region)
      .credentialsProvider(AwsBasicCredentialsProvider.fromConfig(config))

    config.endpoint
      .map(s => injectedBuilder.endpointOverride(URI.create(s)))
      .getOrElse(injectedBuilder)
  }
}
