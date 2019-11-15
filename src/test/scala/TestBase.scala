import org.scalatest._
import org.apache.spark.{SparkConf, SparkContext}

abstract class TestBase extends FlatSpec with BeforeAndAfterAll with Matchers {
  private val master = "local"
  private val appName = "testing"
  var sc: SparkContext = _

  val conf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    .set("spark.driver.allowMultipleContexts","true")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sc = SparkContext.getOrCreate(conf)
  }

  override protected def afterAll(): Unit = {
    try {
      sc.stop()
      sc = null
    } finally {
      super.afterAll()
    }
  }
}
