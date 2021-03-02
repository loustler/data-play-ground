package io.loustler.dpg.testing

import io.loustler.dpg.job.SparkJob
import org.apache.spark.sql.SparkSession

trait SparkJobTestingSpec extends TestingSpec {
  lazy val spark: SparkSession = SparkJob.simpleSparkSession(appName = "spark-test", master = "local")
}
