package monitoring

import com.spark.monitoring.deltalake.Event
import org.apache.spark.sql.Dataset

class DruidIntegrationMap {

  def apply(inpitStream : Dataset[Event]) : Dataset[Event] = {
    inpitStream
  }

}
