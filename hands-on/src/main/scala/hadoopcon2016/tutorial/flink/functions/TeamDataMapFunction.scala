package hadoopcon2016.tutorial.flink.functions

import hadoopcon2016.tutorial.flink.api.dataset.RichFunctions.Team
import org.apache.flink.api.common.functions.RichMapFunction

/**
  * Created by George T. C. Lai on 2016/8/2 for Apache Flink Tutorial in HadoopCon 2016 Taiwan.
  */
class TeamDataMapFunction extends RichMapFunction[String, Team] {
  // Write a rich function to perform map operation on the team data set
  def map(in: String): Team = {
    val strArray = in.split(',')
    Team(strArray(0).toInt, strArray(1))
  }
}
