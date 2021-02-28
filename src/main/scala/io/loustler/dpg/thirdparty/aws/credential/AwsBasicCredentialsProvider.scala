package io.loustler.dpg.thirdparty.aws.credential

import io.loustler.dpg.config.AwsConfig
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, AwsCredentials, AwsCredentialsProvider }

/** An implementation of [[BasicAwsCredentialsProvider]] for use AWS IAM user's [[accessKey]] and [[secretAccessKey]].
  *
  * This implementation provide credential directly given [[accessKey]] and [[secretAccessKey]].
  *
  * @param accessKey aws iam user's access key
  * @param secretAccessKey aws iam user's secret access key
  */
final class AwsBasicCredentialsProvider(accessKey: String, secretAccessKey: String) extends AwsCredentialsProvider {
  override def resolveCredentials(): AwsCredentials = AwsBasicCredentials.create(accessKey, secretAccessKey)
}

object AwsBasicCredentialsProvider {

  /** Create [[AwsBasicCredentialsProvider]] from config
    *
    * @param config aws config
    * @return
    */
  def fromConfig(config: AwsConfig): AwsBasicCredentialsProvider =
    new AwsBasicCredentialsProvider(config.accessKey, config.secretAccessKey)
}
