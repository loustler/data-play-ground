package io.loustler.dpg.testing

import org.apache.spark.SparkConf
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }
import org.scalatest.enablers.Emptiness
import org.scalatest.funspec.AnyFunSpecLike

trait SharedSparkSpec extends AnyFunSpecLike with TestingSpec with BeforeAndAfterAll with BeforeAndAfterEach {
  private var _spark: SparkSession = _

  protected implicit def sparkContext: SparkSession = _spark

  protected def sparkConf: SparkConf =
    new SparkConf()
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.ui.enabled", "false")
      .set("hive.stats.jdbc.timeout", "80")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.hadoop.fs.s3a.access.key", "TESTKEY")
      .set("spark.hadoop.fs.s3a.secret.key", "TESTSECRET")
      .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.committer.name", "directory")
      .set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
      .set("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/staging")

  def spark: SparkSession = _spark

  override protected def beforeAll(): Unit =
    _spark = SparkSession
      .builder()
      .master("local")
      .appName("testing")
      .getOrCreate()

  override protected def afterEach(): Unit = {
//    _spark.stop()
    sparkContext.sharedState.cacheManager.clearCache()
    sparkContext.sessionState.catalog.reset()
  }

  implicit val EmptinessForDataFrame: Emptiness[DataFrame] = new Emptiness[DataFrame] {
    override def isEmpty(thing: DataFrame): Boolean = thing.isEmpty
  }
}
