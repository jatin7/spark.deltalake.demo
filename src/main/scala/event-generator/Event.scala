package com.spark.monitoring.deltalake

import java.sql.Timestamp


case class Event(region: String, level: Int, idAddress: String, eventTime: Timestamp)