package io.loustler.dpg.testing

import org.apache.spark.SparkConf
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.enablers.Emptiness
import org.scalatest.funspec.AnyFunSpecLike

trait SharedSparkSpec extends AnyFunSpecLike with TestingSpec with BeforeAndAfterAll {
  private var _spark: SparkSession = _

  protected def sparkConf: SparkConf =
    new SparkConf()
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.ui.enabled", "false")
      .set("hive.stats.jdbc.timeout", "80")

  def spark: SparkSession = _spark

  override protected def beforeAll(): Unit =
    _spark = SparkSession
      .builder()
      .master("local")
      .appName("testing")
      .getOrCreate()

  override protected def afterAll(): Unit = {
    _spark.close()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  implicit val EmptinessForDataFrame: Emptiness[DataFrame] = new Emptiness[DataFrame] {
    override def isEmpty(thing: DataFrame): Boolean = thing.isEmpty
  }
}
