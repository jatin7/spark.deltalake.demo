package com.spark.monitoring.deltalake

import org.apache.spark.sql.{ Encoders, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object EventGeneratorJob {

  def main(args: Array[String]): Unit = {
    //    val kafkaServers = "3.250.149.133:9094,63.34.145.162:9094,52.30.182.144:9094"
    val kafkaServers = "localhost:9092"
    val checkpoint = "/tmp/random_spark_chk"
    val tagetTopic = "1_1_Order_Stats_Spark"

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .load()

    import spark.implicits._
    implicit val e = Encoders.product[Event]

    val res = inputStream.map(r => {
      EventsGenerator.generateRandomSuspiciousEvent(r.getTimestamp(0), 5)
    })
      .select(to_json(struct($"*")) as "value")

    val writeQuery = res
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("checkpointLocation", checkpoint)
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("topic", tagetTopic)
      .start()

    //        val writeQuery = res
    //          .writeStream
    //          .format("console")
    //          .start()

    writeQuery.awaitTermination()
  }


}
