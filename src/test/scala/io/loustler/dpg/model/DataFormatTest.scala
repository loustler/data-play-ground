package io.loustler.dpg.model

import io.loustler.dpg.testing.FunSuite

final class DataFormatTest extends FunSuite {
  test("Get full file name") {
    val csv     = DataFormat.CSV
    val json    = DataFormat.JSON
    val parquet = DataFormat.Parquet

    val fileName = "Hello"

    DataFormat.fullFileName(fileName, csv) should ===(s"${fileName}.csv")
    DataFormat.fullFileName(fileName, json) should ===(s"${fileName}.json")
    DataFormat.fullFileName(fileName, parquet) should ===(s"${fileName}.parquet")
  }

  test("File extension matches with DataSourcetype") {
    val csv     = DataFormat.CSV
    val json    = DataFormat.JSON
    val parquet = DataFormat.Parquet

    DataFormat.getFileExtension(csv) should ===("csv")
    DataFormat.getFileExtension(json) should ===("json")
    DataFormat.getFileExtension(parquet) should ===("parquet")
  }
}
