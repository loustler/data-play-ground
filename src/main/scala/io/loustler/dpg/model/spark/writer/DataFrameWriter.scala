package io.loustler.dpg.model.spark.writer

import org.apache.spark.sql.DataFrame

trait DataFrameWriter {

  /** Write DataFrame into the path
    *
    * @param df dataframe
    * @param path path to write
    */
  def write(df: DataFrame, path: String): Unit

  def options: Map[String, String]
}

object DataFrameWriter {

  def csv: CsvWriter = new CsvWriter

  def json: JsonWriter = new JsonWriter

  def orc: OrcWriter = new OrcWriter

  def parquet: ParquetWriter = new ParquetWriter

  def text: TextWriter = new TextWriter
}
