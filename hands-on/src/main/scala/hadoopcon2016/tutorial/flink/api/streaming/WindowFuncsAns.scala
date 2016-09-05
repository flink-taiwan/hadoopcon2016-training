package hadoopcon2016.tutorial.flink.api.streaming

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by George T. C. Lai on 2016/8/2 for Apache Flink Tutorial in HadoopCon 2016 Taiwan.
  *
  * In this hands-on tutorial, we are going to calculate average of the last 5 seconds for each
  * team. The raw data for each event is given as a
  * string of the following format:
  *
  * "<timestamp>, <name>, <team id>, <score>"
  *
  * The expected results should be a set of Tuples being similar to
  *
  * (1471830125000,3,91.0)
  * (1471830200000,3,78.0)
  * (1471830025000,1,87.0)
  * ......
  */
object WindowFuncsAns {
  val rawData = Seq("2016-08-22T09:40:21.253+08:00,John,4,90", "2016-08-22T09:40:22.131+08:00,Mary,1,87",
    "2016-08-22T09:40:27.407+08:00,Albert,2,54", "2016-08-22T09:40:28.865+08:00,Kate,2,77",
    "2016-08-22T09:40:29.569+08:00,Bob,4,89", "2016-08-22T09:42:02.728+08:00,Rose,3,91",
    "2016-08-22T09:42:04.114+08:00,Gray,4,73", "2016-08-22T09:42:06.927+08:00,Ted,1,94",
    "2016-08-22T09:42:35.376+08:00,James,2,85", "2016-08-22T09:42:37.219+08:00,Gavin,1,56",
    "2016-08-22T09:43:12.536+08:00,Victor,4,96", "2016-08-22T09:43:15.164+08:00,Mike,3,78")

  def main(args: Array[String]): Unit = {
    val flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment
    flinkEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val datastream = flinkEnv.fromCollection(rawData)
    datastream.map {
      str => {
        val strArray = str.split(',')
        val formater = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        val eventTS = formater.parse(strArray(0)).getTime
        (eventTS, strArray(1), strArray(2).toInt, strArray(3).toInt)
      }
    }
      .startNewChain()
      .filter(student => (student._4 <= 100) && (student._4 >= 0))
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Int, Int)] {
        private var currentTimeStamp: Long = _

        override def getCurrentWatermark: Watermark = new Watermark(currentTimeStamp)

        override def extractTimestamp(element: (Long, String, Int, Int), previousElementTimestamp: Long): Long = {
          val timestamp = element._1
          currentTimeStamp = if (timestamp > currentTimeStamp) timestamp else currentTimeStamp
          timestamp
        }
      })
      .keyBy(2)
      .timeWindow(Time.seconds(5))
      .apply {
        (key: Tuple, window: TimeWindow, in: Iterable[(Long, String, Int, Int)], out: Collector[(Long, Int, Float)]) => {
          val tmpList = in.toList
          val teamid = tmpList.head._3
          val len = tmpList.length
          var total: Float = 0
          tmpList.foreach(element => total += element._4.toFloat)
          out.collect((window.getEnd, teamid, total / len))
        }
      }
      .print
    flinkEnv.execute("DiffWindows")
  }
}
