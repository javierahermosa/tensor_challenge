import org.scalatest._
import org.apache.spark._

class TensorTest extends TestBase {
  private var example: Tensor = _

  val resetTicks = List(1000, 1000000)
  val resetTimes = List(1, 60, 3600)


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    example = new Tensor(resetTicks, resetTimes)
  }

  "I Should be Able to Say Hello" should "have the right output" in {
    val greet = sc.parallelize(Seq("Pablo", "Maria", "Pilar"))
    greet.map(example.say_hello).count should be (3)
    greet.map(example.say_hello).first should be ("Hello Pablo")

  }

}