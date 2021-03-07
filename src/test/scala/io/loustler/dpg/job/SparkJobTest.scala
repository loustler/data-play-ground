package io.loustler.dpg.job

import io.loustler.dpg.model.DataFormat
import io.loustler.dpg.testing.FunSuite

final class SparkJobTest extends FunSuite {

  test("SparkJob success resolve datasource path") {
    val actualCsv = SparkJob.resolveDataSourcePath(DataFormat.CSV, "hello")

    actualCsv should endWith("datasource/csv/hello.csv")

    val actualJson = SparkJob.resolveDataSourcePath(DataFormat.JSON, "hello")

    actualJson should endWith("datasource/json/hello.json")
  }
}
