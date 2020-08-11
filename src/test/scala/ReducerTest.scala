import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.TimeUnit

import co.thebeat.bigdata.takehomeassignment.reducer.Reducer
import co.thebeat.bigdata.takehomeassignment.session.Sessionizer
import co.thebeat.bigdata.takehomeassignment.utils.Models.{EventZone, Session}
import org.scalatest.FunSuite

import scala.concurrent.duration.Duration
import scala.util.Success

class ReducerTest extends  FunSuite  with SparkSessionBaseTest {

  import  spark.implicits._


  /***
  *  For example:
  * input: driver, session_created_at, id_zone, count
  *        A, 12:08:15, 1, 2
  *        B, 12:10:34, 1, 2
  *        A, 12:28:02, 2, 5
  *        B, 12:31:51, 2, 4
  *        B, 12:47:04, 2, 2
  *        B, 13:00:29, 2, 3
  *
  * output: id_zone, driver, session_created_at, count
  *         1, B, 12:10:34, 2
  *         2, A, 12:28:02, 5
  ***/

  test("diver sessions") {

  val input = Seq(
    Session("A", Timestamp.valueOf("2017-08-31 12:08:15"), 1, 2),
    Session("B", Timestamp.valueOf("2017-08-31 12:10:34"), 1, 2),
    Session("A", Timestamp.valueOf("2017-08-31 12:28:02"), 2, 5),
    Session("B", Timestamp.valueOf("2017-08-31 12:31:51"), 2, 4),
    Session("B", Timestamp.valueOf("2017-08-31 12:47:04"), 2, 2),
    Session("B", Timestamp.valueOf("2017-08-31 13:00:29"), 2, 3)
   ).toDF()

    val expected = Seq(
      Session("B", Timestamp.valueOf("2017-08-31 12:10:34"),1,2),
      Session("A", Timestamp.valueOf("2017-08-31 12:28:02"),2,5)
    )

    new Reducer{}.reduce(input) match  {
      case Success(top) => {
        val actual = top.as[Session].collect().toSeq
        assert(expected == actual)
      }
    }

  }
}
