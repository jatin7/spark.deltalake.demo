package com.spark.monitoring.deltalake

import net.bmjames.opts.{execParser, info, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}
import scalaz.syntax.applicativePlus._
import utils.ArgsParser.parseOpts
import utils.Opts

object EventGeneratorJob {

  def main(args: Array[String]): Unit = {
    val opts: Opts = execParser(args, this.getClass.getName, info(parseOpts <*> helper))

    val kafkaServers = opts.kafkaBootstrap.getOrElse( "3.250.149.133:9094,63.34.145.162:9094,52.30.182.144:9094")
    val checkpoint = opts.checkpoint.getOrElse("/tmp/random_spark_chk")
    val tagetTopic = opts.output.getOrElse("1_1_Order_Stats_Spark")
    val rate: Int = opts.rate.getOrElse("1000").toInt

    val spark = SparkSession
      .builder()
     // .master("local[*]")
      .appName("Event Generator")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", rate)
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

    writeQuery.awaitTermination()
  }
}
