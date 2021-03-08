package io.loustler.dpg.application.raw

import io.loustler.dpg.application.SparkApplication
import io.loustler.dpg.config.AppConfig
import io.loustler.dpg.job.SparkJob
import io.loustler.dpg.job.raw.RawMobilePriceClassificationJob
import org.apache.spark.SparkConf

object RawMobilePriceClassificationApplication extends SparkApplication {
  override val appName: String = "RawMobilePriceClassification"

  override def job: SparkJob = new RawMobilePriceClassificationJob

  override def sparkConfig(config: AppConfig): SparkConf =
    super
      .sparkConfig(
        config
      )
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.hadoop.fs.s3a.access.key", "TESTKEY")
      .set("spark.hadoop.fs.s3a.secret.key", "TESTSECRET")
      .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.committer.name", "directory")
      .set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
      .set("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/staging")
}
