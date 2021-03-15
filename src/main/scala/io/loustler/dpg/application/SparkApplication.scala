package io.loustler.dpg.application

import com.typesafe.config.{ Config, ConfigFactory }
import io.loustler.dpg.config.AppConfig
import io.loustler.dpg.job.SparkJob
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkApplication {
  val appName: String

  def job: SparkJob

  def sparkConfig(config: AppConfig): SparkConf =
    new SparkConf()
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.hadoop.fs.s3a.access.key", "TESTKEY")
      .set("spark.hadoop.fs.s3a.secret.key", "TESTSECRET")
      .set("spark.hadoop.fs.s3a.endpoint", config.storage.dataLake.endpoint.getOrElse(""))
      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.committer.name", "directory")
      .set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
      .set("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/staging")

  final def loadConfig(): Config = ConfigFactory.load()

  final def loadAppConfig(): AppConfig = AppConfig.load(loadConfig())

  def main(args: Array[String]): Unit = {
    val appConfig = loadAppConfig()

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config(sparkConfig(appConfig))
      .getOrCreate()

    job.run(appConfig, spark)
  }
}
