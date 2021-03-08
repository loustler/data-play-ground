package io.loustler.dpg.util

import io.loustler.dpg.config.StorageConfig
import io.loustler.dpg.model.{ DataFormat, JobType }
import io.loustler.dpg.testing.FunSpec

final class JobUtilTest extends FunSpec {

  val storageConfig = StorageConfig(
    dataLake = StorageConfig.DataLakeConfig(
      name = "my-bucket",
      endpoint = None
    )
  )

  describe("JobUtil") {
    describe("path") {
      it("Success get path if job type is RawDataJob with any data format") {
        JobUtil.path(storageConfig, JobType.RawDataJob, DataFormat.CSV, "/csv/my-csv") should ===(
          "s3a://my-bucket/raw/csv/my-csv.csv"
        )
        JobUtil.path(storageConfig, JobType.RawDataJob, DataFormat.JSON, "/json/my-json") should ===(
          "s3a://my-bucket/raw/json/my-json.json"
        )
        JobUtil.path(storageConfig, JobType.RawDataJob, DataFormat.Parquet, "/parquet/my-parquet") should ===(
          "s3a://my-bucket/raw/parquet/my-parquet.parquet"
        )
      }
      it("Success get path if job type is AnalyticJob with any data format") {
        JobUtil.path(storageConfig, JobType.AnalyticJob, DataFormat.CSV, "/csv/my-csv") should ===(
          "s3a://my-bucket/analytics/csv/my-csv.csv"
        )
        JobUtil.path(storageConfig, JobType.AnalyticJob, DataFormat.JSON, "/json/my-json") should ===(
          "s3a://my-bucket/analytics/json/my-json.json"
        )
        JobUtil.path(storageConfig, JobType.AnalyticJob, DataFormat.Parquet, "/parquet/my-parquet") should ===(
          "s3a://my-bucket/analytics/parquet/my-parquet.parquet"
        )
      }
    }
  }
}
