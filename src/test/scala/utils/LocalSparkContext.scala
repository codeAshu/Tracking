package utils

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkConf, SparkContext}

trait LocalSparkContext extends BeforeAndAfterAll { self: Suite =>
  @transient var sc: SparkContext = _

  override def beforeAll() {
    System.setProperty("hadoop.home.dir", "G:\\GitRepos\\Tracking\\winutil\\")  //comment out for linux
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
    sc = new SparkContext(conf)
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }
}