import org.apache.spark.sql.SparkSession

trait SparkSessionBaseTest {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("beat assignment")
      .enableHiveSupport()
      .getOrCreate()
  }

}
