import co.thebeat.bigdata.takehomeassignment.utils.Models.GeoPoint
import co.thebeat.bigdata.takehomeassignment.utils.Polygon.{isInside, _}
import org.scalatest.FunSuite

class PolygonTest extends  FunSuite{

  val polygon1 = List ( GeoPoint(0, 0),  GeoPoint(10, 0),  GeoPoint(10, 10), GeoPoint(0, 10) )

  test("Point outside polygon")  {
    assert(isInside(polygon1,  GeoPoint(20, 20)) == false)
  }

  test("Point inside polygon") {
    assert(isInside(polygon1,  GeoPoint(5, 5)) == true)
  }

  test("Point at the polygon boundary") {
    assert( isInside(polygon1,  GeoPoint(10, 0))  == false)
  }



}
