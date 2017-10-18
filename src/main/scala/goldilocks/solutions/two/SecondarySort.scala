package goldilocks.solutions.two

import goldilocks.solutions.Solution
import goldilocks.solutions.one.GroupByKey
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{Partitioner, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * description: 
  * link to test/main class:
  */
class SecondarySort(sparkConf: SparkConf) extends Solution {

  def findRankStatistics(dataFrame: DataFrame,
                         targetRanks: List[Long], partitions: Int): Map[Int, Iterable[Double]] = {

    val pairRDD: RDD[((Int, Double), Int)] =
      GroupByKey.mapToKeyValuePairs(dataFrame).map((_, 1))

    val partitioner = new ColumnIndexPartition(partitions)
    //sort by the existing implicit ordering on tuples first key, second key
    val sorted = pairRDD.repartitionAndSortWithinPartitions(partitioner)

    //filter for target ranks
    val filterForTargetIndex: RDD[(Int, Double)] =
      sorted.mapPartitions((iter: Iterator[((Int, Double), Int)]) => {
        var currentColumnIndex = -1
        var runningTotal = 0
        iter.filter({
          case (((colIndex, _), _)) =>
            if (colIndex != currentColumnIndex) {
              currentColumnIndex = colIndex //reset to the new column index
              runningTotal = 1
            } else {
              runningTotal += 1
            }
            //if the running total corresponds to one of the rank statistics.
            //keep this ((colIndex, value)) pair.
            targetRanks.contains(runningTotal)
        })
      }.map(_._1), preservesPartitioning = true)
    groupSorted(filterForTargetIndex.collect())
  }

  private def groupSorted(
                           it: Array[(Int, Double)]): Map[Int, Iterable[Double]] = {
    val res = List[(Int, ArrayBuffer[Double])]()
    it.foldLeft(res)((list, next) => list match {
      case Nil =>
        val (firstKey, value) = next
        List((firstKey, ArrayBuffer(value)))
      case head :: _ =>
        val (curKey, valueBuf) = head
        val (firstKey, value) = next
        if (!firstKey.equals(curKey)) {
          (firstKey, ArrayBuffer(value)) :: list
        } else {
          valueBuf.append(value)
          list
        }
    }).toMap
  }

  /**
    * Goldilocks wanted us to design an application that would let her input an arbitrary list of integers n1…nk and return the nth best element in each column. For example, if Goldilocks input 8, 1000, and 20 million, our function would need to return the 8th, 1000th, and 20 millionth best-ranking panda for each attribute column.
    *
    * @param dataFrame input dataframe
    * @param ranks     list of positive ranks
    * @return Map [ColumnIndex, Ranks]; Ranks.length == length of ranks pamameter
    */
  override def findRankStatistics(dataFrame: DataFrame, ranks: List[Long]): Map[Int, Iterable[Double]] = {
    val numOfPartitions = sparkConf.getInt("spark.default.parallelism", 10)
    findRankStatistics(dataFrame, ranks, numOfPartitions)
  }
}

class ColumnIndexPartition(override val numPartitions: Int)
  extends Partitioner {
  require(numPartitions >= 0, s"Number of partitions " +
    s"($numPartitions) cannot be negative.")

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(Int, Double)]
    Math.abs(k._1) % numPartitions //hashcode of column index
  }
}

