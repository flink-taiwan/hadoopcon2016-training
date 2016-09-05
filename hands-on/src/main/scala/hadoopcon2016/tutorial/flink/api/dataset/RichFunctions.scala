package hadoopcon2016.tutorial.flink.api.dataset

import hadoopcon2016.tutorial.flink.functions.StudentDataMapFunction
import hadoopcon2016.tutorial.flink.functions.TeamDataMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  * Created by George T. C. Lai on 2016/8/2 for Apache Flink Tutorial in HadoopConf 2016 Taiwan.
  *
  * In this hands-on tutorial, we are going to reuse the code from Transformation except the two
  * map functions firstly applied to teamData and scoreData. These two functions must be written
  * in terms of RichFunction. The final results should be the same as the previous exercise:
  *
  * (Albert,Google,54)
  * (Kate,Google,77)
  * (James,Google,85)
  * (John,RedHat,90)
  * ......
  * ......
  */
object RichFunctions {
  case class Student(name: String, teamid: Int, score: Int)
  case class Team(id: Int, name: String)

  val teamData = Seq("1,Facebook", "2,Google", "3,Uber", "4,RedHat")

  val scoreData = Seq("John,4,90", "Mary,1,87", "Albert,2,54", "Kate,2,77",
    "Bob,4,89", "Rose,3,91", "Gray,4,73", "Ted,1,94",
    "James,2,85", "Gavin,1,56", "Victor,4,96", "Mike,3,78")

  def main(args: Array[String]): Unit = {
    val flinkEnv = ExecutionEnvironment.getExecutionEnvironment

    val teamDataSet = flinkEnv.fromCollection(teamData)
      .map(str => {
        val strArray = str.split(',')
        Team(strArray(0).toInt, strArray(1))
      })
    val scoreDataSet = flinkEnv.fromCollection(scoreData)
      .map(str => {
        val strArray = str.split(',')
        Student(strArray(0), strArray(1).toInt, strArray(2).toInt)
      })

    val joinedDataSet = scoreDataSet.join(teamDataSet).where("teamid").equalTo("id") {
      (student, team) => {
        (student.name, team.name, student.score)
      }
    }

    val res = joinedDataSet.groupBy(1)
      .reduceGroup {
        (in: Iterator[(String, String, Int)], out: Collector[(String, Float)]) => {
          val teamList = in.toList
          val teamName = teamList.head._2
          var totalScore: Int = 0
          teamList.foreach(totalScore += _._3)
          out.collect((teamName, totalScore.toFloat / teamList.length))
        }
      }
    res.print
  }
}
