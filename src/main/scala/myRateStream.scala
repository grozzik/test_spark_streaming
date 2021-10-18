import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object myRateStream extends App {

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  val lines = spark.readStream
    .format("rate")
    .load()

  val query = lines.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    batchDF
      .withColumn("batchId", lit(batchId))
      .write
      .format("csv")
      .save(args(0))
  }.start()

  query.awaitTermination()

}
