package hadoopcon2016.tutorial.flink.accumulators

import org.apache.flink.api.common.accumulators.Accumulator
import org.apache.flink.api.common.accumulators.SimpleAccumulator

/**
  * Created by george on 2016/8/2.
  */
class MovieGenreAccumulator extends SimpleAccumulator[Array[String]] {
  private var localValue = Array[String]()

  override def add(v: Array[String]): Unit = {
    localValue = localValue.toSet.union(v.toSet).toArray
  }

  override def getLocalValue() = localValue

  override def merge(other: Accumulator[Array[String], Array[String]]) = {
    this.localValue.toSet.union(other.getLocalValue.toSet)
  }

  override def resetLocal(): Unit = {
    this.localValue = Array[String]()
  }

  override def clone(): Accumulator[Array[String], Array[String]] = {
    val result: MovieGenreAccumulator = new MovieGenreAccumulator()
    result.localValue = this.localValue
    result
  }

  override def toString(): String = {
    "MovieGenreAccumulator: " + localValue.mkString(", ")
  }
}
