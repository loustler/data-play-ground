package io.loustler.dpg.job

import io.loustler.dpg.model.DataFormat
import io.loustler.dpg.testing.FunSuite
import org.apache.spark.SparkConf

final class SparkJobTest extends FunSuite {
  val sparkJob = new SparkJob {}

  test("SparkJob success loadAppConfig") {
    val appConfig = sparkJob.loadAppConfig()

    appConfig.aws.accessKey should not be empty
    appConfig.aws.secretAccessKey should not be empty
  }

  test("SparkJob success resolve datasource path") {
    val actualCsv = sparkJob.resolveDataSourcePath(DataFormat.CSV, "hello")

    actualCsv should endWith("datasource/csv/hello.csv")

    val actualJson = sparkJob.resolveDataSourcePath(DataFormat.JSON, "hello")

    actualJson should endWith("datasource/json/hello.json")
  }

  test("Create SparkSession from sparkSession") {
    val spark1 = SparkJob.simpleSparkSession(appName = "my-app")
    val spark2 = SparkJob.sparkSession(appName = "my-app")(new SparkConf())

    spark1 should ===(spark2)
    spark1.close()
    spark2.close()
  }

  test("Create SparkSession with options") {
    val conf = new SparkConf()
    conf.set("header", "true")

    val spark = SparkJob.sparkSession(appName = "my-app")(conf)

    spark.conf.getAll should contain key ("header")
    spark.close()
  }

}
