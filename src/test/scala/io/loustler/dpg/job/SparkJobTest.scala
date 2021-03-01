package io.loustler.dpg.job

import io.loustler.dpg.model.DataFormat
import io.loustler.dpg.testing.FunSuite

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

}
