package io.loustler.dpg.model

sealed trait JobType extends Serializable with Product

object JobType {
  case object RawDataJob  extends JobType
  case object AnalyticJob extends JobType
}
