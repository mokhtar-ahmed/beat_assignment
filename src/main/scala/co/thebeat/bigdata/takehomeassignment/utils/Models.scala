package co.thebeat.bigdata.takehomeassignment.utils

import java.sql.Timestamp

import org.apache.spark.sql.Dataset

object Models {

  case class GeoPoint(lat: Double, lng: Double)

  case class Zone(id_zone: Int, polygon: List[GeoPoint])

  case class ZonesConfiguration(zones: List[Zone])

  case class Event(driver: String, timestamp: Timestamp, latitude: Double, longitude: Double)

  case class EventZone(driver: String, timestamp: Timestamp, latitude: Double, longitude: Double, id_zone: Int)

  case class Session(driver: String, session_created_at:Timestamp, id_zone:Int, count:Int)

  case class DriverSessions(driver: String, sessions:Seq[Session])
}
