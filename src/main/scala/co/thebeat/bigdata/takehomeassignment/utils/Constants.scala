package co.thebeat.bigdata.takehomeassignment.utils

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}

object Constants {

  val ZONES_CONFIG_FILE_PATH = "/zones.json"
  val EVENTS_FILE_PATH = "/data"   // for local testing only for production should set the exact path
  val OUTPUT_PATH ="/output"   // for local testing only for production should set the exact path
  val DURATION_MINUTES = 10

  val EVENTS_SCHEMA = StructType(
    List(
      StructField("driver", StringType, false),
      StructField("timestamp", TimestampType, false),
      StructField("latitude", DoubleType, false),
      StructField("longitude", DoubleType, false)
    )
  )

}
