package co.thebeat.bigdata.takehomeassignment.utils

import java.io.InputStream

import co.thebeat.bigdata.takehomeassignment.utils.Models.{GeoPoint, Zone, ZonesConfiguration}
import org.json4s.jackson.JsonMethods.parse

object Utils {

  def getValidZones(path:String): List[Zone] = {
    implicit val formats = org.json4s.DefaultFormats
    val in: InputStream = getClass.getResourceAsStream(path)
    val zonesConfig: ZonesConfiguration = parse(in).extract[ZonesConfiguration]
    zonesConfig.zones.filter( zone => zone.polygon.head == zone.polygon.last)
  }

}
