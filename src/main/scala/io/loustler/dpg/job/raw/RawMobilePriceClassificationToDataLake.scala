package io.loustler.dpg.job.raw

import io.loustler.dpg.job.SparkJob
import io.loustler.dpg.model.DataFormat
import org.apache.spark.sql.SparkSession

/** Store raw data of mobile price classification into Data Lake
  *
  * @see https://www.kaggle.com/radmirzosimov/telecom-users-dataset
  */
object RawMobilePriceClassificationToDataLake extends SparkJob {

  def main(args: Array[String]): Unit = {
    val appConfig = loadAppConfig()

    val spark = SparkSession.builder().master("local").appName("MobilePriceClassification").getOrCreate()

    val csv = spark.read.option("header", true).csv(resolveDataSourcePath(DataFormat.CSV, "telecom_users"))

    csv.write.parquet(destination(appConfig.storage, "raw_telecom_users", DataFormat.Parquet))
  }
}
