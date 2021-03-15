package io.loustler.dpg.model.spark.writer

import org.apache.spark.sql.{ DataFrame, Row, DataFrameWriter => SparkWriter }

import scala.collection.mutable.{ Map => MutableMap }

private[writer] abstract class BaseWriter[T <: BaseWriter[T]] extends DataFrameWriter { self: T =>
  private val writeOptions: MutableMap[String, String] = MutableMap.empty

  protected def option(key: String, value: String): T = {
    writeOptions.update(key, value)
    self
  }

  def writer(df: DataFrame): SparkWriter[Row] =
    options.foldLeft(df.write) { case (writer, (key, value)) =>
      writer.option(key, value)
    }

  override def options: Map[String, String] = writeOptions.toMap
}
