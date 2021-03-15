package io.loustler.dpg.model.spark.reader

import io.loustler.dpg.testing.SharedSparkSpec
import org.apache.spark.sql.{ DataFrame, SparkSession }

final class BaseReaderTest extends SharedSparkSpec {

  final class Testable extends BaseReader[Testable] {

    override def read(
      spark: SparkSession,
      path: String*
    ): DataFrame = {
      import spark.implicits._

      Seq("hello", "world").toDF()
    }
  }

  describe("BaseReader") {
    it("Checks all options") {
      val reader = new Testable

      reader.options should be(empty)
    }
  }
}
