package io.loustler.dpg.job.raw

import io.loustler.dpg.config.AppConfig
import io.loustler.dpg.job.SparkJob
import io.loustler.dpg.model.{ DataFormat, JobType }
import io.loustler.dpg.util.JobUtil
import org.apache.spark.sql.SparkSession

/** Store raw data of mobile price classification into Data Lake
  *
  * @see https://www.kaggle.com/radmirzosimov/telecom-users-dataset
  */
final class RawMobilePriceClassificationJob extends SparkJob {

  override def run(
    config: AppConfig,
    spark: SparkSession
  ): Unit = {
    val csv = spark.read.option("header", true).csv(SparkJob.resolveDataSourcePath(DataFormat.CSV, "telecom_users"))

    val path = JobUtil.path(config.storage, JobType.RawDataJob, DataFormat.Parquet, "kaggle/raw_telecom_users")

    csv.write.parquet(path)
  }
}
