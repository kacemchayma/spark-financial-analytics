import org.apache.spark.sql.SparkSession

object PrintSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Print Schema")
      .master("local[*]")
      .getOrCreate()
    
    val df = spark.read.parquet("data/stock_id/stock_id.parquet")
    df.printSchema()
    df.show(5)
    spark.stop()
  }
}
