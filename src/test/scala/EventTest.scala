import co.thebeat.bigdata.takehomeassignment.storage._
import co.thebeat.bigdata.takehomeassignment.utils._
import co.thebeat.bigdata.takehomeassignment.utils.Constants._
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.FunSuite

import scala.util.Success

class EventTest extends  FunSuite with SparkSessionBaseTest {

  test("Read events csv test") {

    val path = getClass.getResource("/data").getPath
    println(path)
    val events  = EventsReader(spark, EVENTS_SCHEMA).read(path)
    events match {
      case Success(ev) => assert( ev.count() == 2)

    }

  }
}
