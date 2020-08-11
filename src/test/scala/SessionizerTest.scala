import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import co.thebeat.bigdata.takehomeassignment.session.Sessionizer
import co.thebeat.bigdata.takehomeassignment.utils.Models.{EventZone, Session}
import org.scalatest.FunSuite

import scala.concurrent.duration.Duration
import scala.util.Success

class SessionizerTest extends  FunSuite with SparkSessionBaseTest {

  import spark.implicits._
  /**
  * For example,
  * input: driver, timestamp, id_zone
  *    A 1 12:11:23
  *    A 2 12:12:56
  *    B 2 12:14:28
  *    A 1 12:15:45
  *    B 1 12:21:09
  *    A 2 12:25:50
  *    Α 2 12:32:03
  *    A 1 12:35:22
  *    A 2 12:41:08

  * duration: 10 minutes

  * key: driver
  *
  * The output Dataset[Row] should be:
  *
  * driver, session_created_at, id_zone, count
  *    A 12:11:23 1 2
  *    B 12:14:28 2 1
  *    A 12:25:50 2 2
  *    A 12:41:08 2 1
  *
  */
  test("driver sessions test") {

    val events =  Seq(
      EventZone("A",  Timestamp.valueOf("2017-08-31 12:11:23"), 123.0, 124.0, 1),
      EventZone("A",  Timestamp.valueOf("2017-08-31 12:12:56"), 123.0, 124.0, 2),
      EventZone("B",  Timestamp.valueOf("2017-08-31 12:14:28"), 123.0, 124.0, 2),
      EventZone("A",  Timestamp.valueOf("2017-08-31 12:15:45"), 123.0, 124.0, 1),
      EventZone("B",  Timestamp.valueOf("2017-08-31 12:21:09"), 123.0, 124.0, 1),
      EventZone("A",  Timestamp.valueOf("2017-08-31 12:25:50"), 123.0, 124.0, 2),
      EventZone("A",  Timestamp.valueOf("2017-08-31 12:35:22"), 123.0, 124.0, 1),
      EventZone("A",  Timestamp.valueOf("2017-08-31 12:32:00"), 123.0, 124.0, 1),
      EventZone("A",  Timestamp.valueOf("2017-08-31 12:41:08"), 123.0, 124.0, 2)
    ).toDF()

    val expected = Seq(
        Session("B", Timestamp.valueOf("2017-08-31 12:14:28"),2,1),
        Session("A", Timestamp.valueOf("2017-08-31 12:41:08"),2,1),
        Session("A", Timestamp.valueOf("2017-08-31 12:25:50"),1,2),
        Session("A", Timestamp.valueOf("2017-08-31 12:11:23"),1,2)
    )

    new Sessionizer{}.sessionize(events, Duration(10,TimeUnit.MINUTES)) match  {
      case Success(sessions) => {
        val actual = sessions.as[Session].collect().toSeq
        assert(actual == expected)
      }
    }

  }

}
