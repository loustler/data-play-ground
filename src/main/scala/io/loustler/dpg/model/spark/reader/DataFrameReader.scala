package io.loustler.dpg.model.spark.reader

import org.apache.spark.sql.{ DataFrame, SparkSession }

trait DataFrameReader {
  def read(spark: SparkSession, path: String*): DataFrame

  def options: Map[String, String]
}

object DataFrameReader {
  def csv: CsvReader = new CsvReader

  def json: JsonReader = new JsonReader

  def orc: OrcReader = new OrcReader

  def parquet: ParquetReader = new ParquetReader

  def text: TextReader = new TextReader
}
