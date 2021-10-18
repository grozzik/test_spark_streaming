import org.apache.spark.sql.SparkSession

object myRateStream extends App {

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  val lines = spark.readStream
    .format("rate")
    .load()

  val query = lines.writeStream
    .format("console")
    .start()

  query.awaitTermination()

}
