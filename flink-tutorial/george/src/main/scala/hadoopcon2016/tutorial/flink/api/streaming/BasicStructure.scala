package hadoopcon2016.tutorial.flink.api.streaming

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala._

/**
  * Created by George T. C. Lai on 2016/8/2 for Apache Flink Tutorial in HadoopCon 2016 Taiwan.
  *
  * In this hands-on tutorial, we are going to transform the timestamp in each event into
  * the epoch time of Long in milliseconds. The raw data for each student is given as a
  * string of the following format:
  *
  * "<timestamp>, <name>, <team id>, <score>"
  *
  * The expected results should be a set of Tuples being similar to
  *
  * (1471830021253,John,4,90)
  * (1471830022131,Mary,1,87)
  * (1471830027407,Albert,2,54)
  * ......
  */
object BasicStructure {
  val rawData = Seq("2016-08-22T09:40:21.253+08:00,John,4,90", "2016-08-22T09:40:22.131+08:00,Mary,1,87",
    "2016-08-22T09:40:27.407+08:00,Albert,2,54", "2016-08-22T09:40:28.865+08:00,Kate,2,77",
    "2016-08-22T09:40:29.569+08:00,Bob,4,89", "2016-08-22T09:42:02.728+08:00,Rose,3,91",
    "2016-08-22T09:42:04.114+08:00,Gray,4,73", "2016-08-22T09:42:06.927+08:00,Ted,1,94",
    "2016-08-22T09:42:35.376+08:00,James,2,85", "2016-08-22T09:42:37.219+08:00,Gavin,1,56",
    "2016-08-22T09:43:12.536+08:00,Victor,4,96", "2016-08-22T09:43:15.164+08:00,Mike,3,78")

  def main(args: Array[String]): Unit = {
    // Hints:
    // 1. obtain an StreamExecutionEnvironment
    // 2. prepare the data set by means of fromCollection()
    // 3. transform the data set into the desired format or results
    //   4. specify where to save results of the computation
    //   5. trigger the program execution
  }
}
