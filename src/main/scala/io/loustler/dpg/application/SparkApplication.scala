package io.loustler.dpg.application

import com.typesafe.config.{ Config, ConfigFactory }
import io.loustler.dpg.config.AppConfig
import io.loustler.dpg.job.SparkJob
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkApplication {
  val appName: String

  def job: SparkJob

  def sparkConfig(config: AppConfig): SparkConf = new SparkConf()

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
