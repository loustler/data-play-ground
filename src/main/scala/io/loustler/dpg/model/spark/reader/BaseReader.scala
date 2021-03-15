package io.loustler.dpg.model.spark.reader

import org.apache.spark.sql.{ SparkSession, DataFrameReader => SparkReader }

import scala.collection.mutable.{ Map => MutableMap }

private[reader] abstract class BaseReader[T <: BaseReader[T]] extends DataFrameReader { self: T =>
  private val readOptions: MutableMap[String, String] = MutableMap.empty

  override def options: Map[String, String] = readOptions.toMap

  protected def reader(spark: SparkSession): SparkReader =
    options.foldLeft(spark.read) { case (reader, (key, value)) =>
      reader.option(key, value)
    }

  protected def option(key: String, value: String): T = {
    readOptions.update(key, value)
    self
  }
}
