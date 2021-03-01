package io.loustler.dpg.job.raw

import io.loustler.dpg.job.SparkJob
import io.loustler.dpg.model.DataFormat
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
      .set("spark.hadoop.fs.s3a.access.key", appConfig.aws.accessKey)
      .set("spark.hadoop.fs.s3a.secret.key", appConfig.aws.secretAccessKey)
      .set("spark.hadoop.fs.s3a.endpoint", appConfig.storage.dataLake.endpoint.getOrElse(""))
      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.committer.name", "directory")
      .set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
      .set("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/staging")

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("MobilePriceClassification")
//      .config(hadoopConfig)
      .getOrCreate()

    println(spark.sqlContext.getAllConfs)

    val csv = spark.read.option("header", true).csv(resolveDataSourcePath(DataFormat.CSV, "telecom_users"))

    csv.write.parquet(destination(appConfig.storage, "raw_telecom_users", DataFormat.Parquet))
  }
}
