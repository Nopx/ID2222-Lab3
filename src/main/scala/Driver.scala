import org.apache.spark.sql.SparkSession

object Driver {
  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load("filename.csv")

    df.printSchema()
  }
}
