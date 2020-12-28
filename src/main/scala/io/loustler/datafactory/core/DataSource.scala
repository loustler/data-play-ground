package io.loustler.datafactory.core

sealed trait DataSource extends Product with Serializable

object DataSource {
  case object Postgres extends DataSource

  case object S3 extends DataSource

  case object Kafka extends DataSource

  case object LocalFileSystem extends DataSource
}
