package io.loustler.dpg.model

import io.loustler.dpg.testing.FunSuite

final class DataFormatTest extends FunSuite {
  test("Get full file name") {
    val fileName = "Hello"

    DataFormat.fullFileName(fileName, DataFormat.CSV) should ===(s"${fileName}.csv")
    DataFormat.fullFileName(fileName, DataFormat.JSON) should ===(s"${fileName}.json")
    DataFormat.fullFileName(fileName, DataFormat.Parquet) should ===(s"${fileName}.parquet")
    DataFormat.fullFileName(fileName, DataFormat.Text) should ===(s"${fileName}.txt")
    DataFormat.fullFileName(fileName, DataFormat.ORC) should ===(s"${fileName}.orc")
    assertThrows[UnsupportedOperationException](DataFormat.fullFileName(fileName, DataFormat.JDBC))
  }

  test("File extension matches with DataSourcetype") {
    DataFormat.getFileExtension(DataFormat.CSV) should ===("csv")
    DataFormat.getFileExtension(DataFormat.JSON) should ===("json")
    DataFormat.getFileExtension(DataFormat.Parquet) should ===("parquet")
    DataFormat.getFileExtension(DataFormat.Text) should ===("txt")
    DataFormat.getFileExtension(DataFormat.ORC) should ===("orc")
    assertThrows[UnsupportedOperationException](DataFormat.getFileExtension(DataFormat.JDBC))
  }
}
