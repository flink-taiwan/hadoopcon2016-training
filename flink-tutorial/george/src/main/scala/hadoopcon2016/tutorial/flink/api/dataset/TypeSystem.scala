package hadoopcon2016.tutorial.flink.api.dataset

import org.apache.flink.api.scala._

/**
  * Created by George T. C. Lai on 2016/8/2 for Apache Flink Tutorial in HadoopCon 2016 Taiwan.
  *
  * In this hands-on tutorial, we are going to find out who passed the exam given a set
  * of students and their corresponding scores. The raw data for each student is given as a
  * string of the following format:
  *
  * "<name>, <team id>, <score>"
  *
  * The expected results should be similar to
  *
  * Student(Albert,2,54)
  * Student(Gavin,1,56)
  */
object TypeSystem {
  case class Student(name: String, teamid: Int, score: Int)
  val rawData = Seq("John,4,90", "Mary,1,87", "Albert,2,54", "Kate,2,77",
    "Bob,4,89", "Rose,3,91", "Gray,4,73", "Ted,1,94",
    "James,2,85", "Gavin,1,56", "Victor,4,96", "Mike,3,78")
  def main(args: Array[String]): Unit = {
    // Hints:
    //   1. obtain an ExecutionEnvironment
    //   2. prepare the data set
    //   3. transform the data set into the wanted one
    //   4. specify where to save results of the computation
    //   5. trigger the program execution
  }
}
