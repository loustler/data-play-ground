package io.loustler.datafactory

sealed trait DataTarget extends Product with Serializable

object DataTarget {
  case object Postgres extends DataTarget

  case object S3 extends DataTarget
}
