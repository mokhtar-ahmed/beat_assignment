import java.sql.Timestamp

import co.thebeat.bigdata.takehomeassignment.geo.ZoneMapper
import co.thebeat.bigdata.takehomeassignment.storage.EventsReader
import co.thebeat.bigdata.takehomeassignment.utils.Constants._
import co.thebeat.bigdata.takehomeassignment.utils.Models._
import co.thebeat.bigdata.takehomeassignment.utils.Utils._
import org.scalatest.FunSuite
import scala.util.{Success, Try}

class ZoneTest  extends  FunSuite  with SparkSessionBaseTest  {

  import  spark.implicits._

  test("Valid Zone Test"){
    val validZones =  getValidZones(ZONES_CONFIG_FILE_PATH)
    assert(validZones.length == 2)

  }

  test("Map events to zones Test") {
    val path = getClass.getResource("/data").getPath

    val expected = Seq(EventZone("c473205b", Timestamp.valueOf("2017-08-31 17:24:25"),5.0, 5.0,2))

    EventsReader(spark,EVENTS_SCHEMA).read(path) match {
      case Success(ev) =>  new ZoneMapper{}.mapToZone(ev, ZONES_CONFIG_FILE_PATH) match  {
          case Success(eventZone) => {
            val actual: Seq[EventZone] = eventZone.as[EventZone].collect().toSeq
            assert(actual== expected)
          }
      }
    }

  }

}
