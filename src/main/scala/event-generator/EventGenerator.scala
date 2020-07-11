package com.spark.monitoring.deltalake

import java.sql.Timestamp
import java.util.Calendar

import com.github.javafaker.Faker

object EventsGenerator {

  private val faker = Faker.instance

  def generateRandomSuspiciousEvent(timestamp: Timestamp, persantageOfNulls: Int): Event = {
    val rand = faker.random().nextInt(0, 100)
    val region = if (rand < persantageOfNulls) null else faker.options.option("us-east", "us-west", "eu", "asia", "africa", "australia")
    Event(
      region = region,
      level = faker.options.option(1, 2, 3, 4),
      idAddress = faker.internet.ipV4Address,
      eventTime = timestamp
    )
  }
}