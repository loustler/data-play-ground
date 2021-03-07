package io.loustler.dpg.model

/** Data Format ADT(Algebraic Data Types)
  */
sealed trait DataFormat extends Serializable with Product

object DataFormat {

  def fullFileName(fileName: String, format: DataFormat): String = s"$fileName.${getFileExtension(format)}"

  def getFileExtension(format: DataFormat): String =
    format match {
      case CSV     => "csv"
      case JSON    => "json"
      case Parquet => "parquet"
      case ORC     => "orc"
      case Text    => "txt"
    }

  case object CSV     extends DataFormat
  case object JSON    extends DataFormat
  case object Parquet extends DataFormat
  case object ORC     extends DataFormat
  case object Text    extends DataFormat
}
