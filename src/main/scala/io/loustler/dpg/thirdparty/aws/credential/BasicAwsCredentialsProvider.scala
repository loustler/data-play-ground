package io.loustler.dpg.thirdparty.aws.credential

import io.loustler.dpg.config.AwsConfig
import software.amazon.awssdk.auth.credentials.{
  AwsCredentialsProvider,
  AwsCredentialsProviderChain,
  DefaultCredentialsProvider
}

object BasicAwsCredentialsProvider {

  /** Create [[AwsCredentialsProvider]] instance from config
    *
    * @param config aws config
    * @return
    */
  def fromConfig(config: AwsConfig): AwsCredentialsProvider = {
    val chain = AwsCredentialsProviderChain
      .builder()
      .reuseLastProviderEnabled(true)
      .credentialsProviders(DefaultCredentialsProvider.create())

    val chainBuilder =
      if (config.accessKey.nonEmpty && config.secretAccessKey.nonEmpty)
        chain.addCredentialsProvider(AwsBasicCredentialsProvider.fromConfig(config))
      else chain

    chainBuilder.build()
  }
}
