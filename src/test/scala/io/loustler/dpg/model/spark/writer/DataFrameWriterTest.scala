package io.loustler.dpg.model.spark.writer

import io.loustler.dpg.testing.SharedSparkSpec

final class DataFrameWriterTest extends SharedSparkSpec {
  describe("DataFrameWriter") {
    it("Success create correct writer instance") {
      DataFrameWriter.parquet shouldBe a[ParquetWriter]
      DataFrameWriter.json shouldBe a[JsonWriter]
      DataFrameWriter.csv shouldBe a[CsvWriter]
      DataFrameWriter.orc shouldBe a[OrcWriter]
      DataFrameWriter.text shouldBe a[TextWriter]
    }
  }
}
