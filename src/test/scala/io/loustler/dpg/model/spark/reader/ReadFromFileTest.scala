package io.loustler.dpg.model.spark.reader

import io.loustler.dpg.testing.SharedSparkSpec
import org.apache.spark.sql.{ DataFrame, SparkSession }

final class ReadFromFileTest extends SharedSparkSpec {

  final class Testable extends ReadFromFile[Testable] {

    override def read(
      spark: SparkSession,
      path: String*
    ): DataFrame = {
      import spark.implicits._

      Seq(
        "Hello",
        "World"
      ).toDF()
    }
  }

  describe("ReadFromFile") {
    it("Checks all options") {
      val reader = new Testable

      reader.options should be(empty)

      reader.pathGlobFilter("*")
      reader.options should have size 1

      reader.modifiedBefore("2020-06-01T13:00:00")
      reader.options should have size 2

      reader.modifiedAfter("2020-06-01T13:00:00")
      reader.options should have size 3

      reader.recursiveFileLookup(true)
      reader.options should have size 4
    }
  }
}
