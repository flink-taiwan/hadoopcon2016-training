package hadoopcon2016.tutorial.flink.api.dataset

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  * Created by George T. C. Lai on 2016/8/2 for Apache Flink Tutorial in HadoopConf 2016 Taiwan.
  *
  * In this hands-on tutorial, we are going to join scoreData and teamData and derive the
  * following results:
  *
  * (Albert,Google,54)
  * (Kate,Google,77)
  * (James,Google,85)
  * (John,RedHat,90)
  * ......
  * ......
  */
object Transformation {
  case class Student(name: String, teamid: Int, score: Int)
  case class Team(id: Int, name: String)

  val teamData = Seq("1,Facebook", "2,Google", "3,Uber", "4,RedHat")

  val scoreData = Seq("John,4,90", "Mary,1,87", "Albert,2,54", "Kate,2,77",
    "Bob,4,89", "Rose,3,91", "Gray,4,73", "Ted,1,94",
    "James,2,85", "Gavin,1,56", "Victor,4,96", "Mike,3,78")

  def main(args: Array[String]): Unit = {
    // Hints:
    //   1. obtain an ExecutionEnvironment
    //   2. prepare data sets by means of fromCollection()
    //   3. transform the data sets into the desired format / results
    //   4. specify where to save results of the computation
    //   5. trigger the program execution
  }
}
