package hadoopcon2016.tutorial.flink.functions

import hadoopcon2016.tutorial.flink.api.dataset.RichFunctions.Student
import org.apache.flink.api.common.functions.RichMapFunction

/**
  * Created by George T. C. Lai on 2016/8/2 for Apache Flink Tutorial in HadoopCon 2016 Taiwan.
  *
  */
class StudentDataMapFunction extends RichMapFunction[String, Student] {
  // Write a rich function to perform map operation on the student data set
  def map(in: String): Student = {
    val strArray = in.split(',')
    Student(strArray(0), strArray(1).toInt, strArray(2).toInt)
  }
}
