package io.loustler.dpg.job.raw

import io.loustler.dpg.job.SparkJob
import io.loustler.dpg.model.{ DataFormat, JobType }
import io.loustler.dpg.util.JobUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** Store raw data of mobile price classification into Data Lake
  *
  * @see https://www.kaggle.com/radmirzosimov/telecom-users-dataset
  */
object RawMobilePriceClassificationToDataLake extends SparkJob {

  def main(args: Array[String]): Unit = {
    val appConfig = loadAppConfig()

    val hadoopConfig = new SparkConf()

    hadoopConfig
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.hadoop.fs.s3a.access.key", "TESTKEY")
      .set("spark.hadoop.fs.s3a.secret.key", "TESTSECRET")
      .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.committer.name", "directory")
      .set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
      .set("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/staging")

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("MobilePriceClassification")
      .config(hadoopConfig)
      .getOrCreate()

    val csv = spark.read.option("header", true).csv(resolveDataSourcePath(DataFormat.CSV, "telecom_users"))

    val path = JobUtil.path(appConfig.storage, JobType.RawDataJob, DataFormat.Parquet, "kaggle/raw_telecom_users")

    csv.write.parquet(path)
  }
}
