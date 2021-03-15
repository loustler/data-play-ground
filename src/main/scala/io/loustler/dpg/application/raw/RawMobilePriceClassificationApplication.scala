package io.loustler.dpg.application.raw

import io.loustler.dpg.application.SparkApplication
import io.loustler.dpg.job.SparkJob
import io.loustler.dpg.job.raw.RawMobilePriceClassificationJob

object RawMobilePriceClassificationApplication extends SparkApplication {
  override val appName: String = "RawMobilePriceClassification"

  override def job: SparkJob = new RawMobilePriceClassificationJob
}
