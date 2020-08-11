import co.thebeat.bigdata.takehomeassignment.Assignment
import org.scalatest.FunSuite

class PipelineTest extends  FunSuite with SparkSessionBaseTest {

  test("run pipeline"){
    Assignment.runPipeline()
  }
}
