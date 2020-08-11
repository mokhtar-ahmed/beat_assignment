package co.thebeat.bigdata.takehomeassignment.storage

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.util.{Failure, Success, Try}

case class EventsReader(spark:SparkSession, schema:StructType) extends  Reader {

  import spark.implicits._

  override def read(path: String): Try[Dataset[Row]] = Try({

    val events = spark.read
      .schema(schema)
      .option("delimiter", ",")
      .option("header", "true")
      .csv(path)

    events.filter('driver.isNotNull && trim('driver) =!= "")
          .filter('timestamp.isNotNull)
          .filter('latitude.isNotNull)
          .filter('longitude.isNotNull)
  })

}

