package io.loustler.dpg.model.spark.reader

import io.loustler.dpg.testing.SharedSparkSpec

final class DataFrameReaderTest extends SharedSparkSpec {
  describe("DataFrameReader") {
    it("Success create correct writer instance") {
      DataFrameReader.csv shouldBe a[CsvReader]
      DataFrameReader.json shouldBe a[JsonReader]
      DataFrameReader.orc shouldBe a[OrcReader]
      DataFrameReader.parquet shouldBe a[ParquetReader]
      DataFrameReader.text shouldBe a[TextReader]
    }
  }
}
