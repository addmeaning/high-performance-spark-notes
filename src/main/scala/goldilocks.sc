import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


val session = SparkSession.builder().master("local[*]").appName("worksheet").getOrCreate()

val rdd: RDD[(Int, Double)] = session.sparkContext.parallelize(List(
    (1, 15.0),
    (2, 0.25),
    (3, 2467.0),
    (4, 0.0),
    (1, 2.0),
    (2, 1000.0),
    (3, 35.4),
    (4, 0.0),
  (1, 10.0),
  (2, 2.0),
  (3, 50.0),
  (4, 0.0),
  (1, 3.0),
  (2, 8.5),
  (3, 0.2),
  (4, 98.0)
))
def findStatistics(pairRDD: RDD[(Int, Double)], ranks: List[Long]): collection.Map[Int, scala.Iterable[Double]] = {
  assert(ranks.forall(_ > 0))
  pairRDD.groupByKey.mapValues(iter => {
    val sortedIter = iter.toArray.sorted(Ordering[Double].reverse)
    sortedIter.zipWithIndex.flatMap({ case (value, index) =>
      if (ranks.contains(index + 1)) {
        Iterator(value)
      } else Iterator.empty
    }).toIterable
  }).collectAsMap()

}

findStatistics(rdd, List(1l, 2l, 3l)).toString


private def getRanksLocationsWithinEachPart(targetRanks: List[Long], partitionColumnF: Array[(Int, Array[Long])], colNum: Int): Array[(Int, List[(Int, Long)])] = {
  val runningTotal: Array[Long] = Array.fill(colNum)(0l)
  partitionColumnF.sortBy(_._1).map {
    case (partitionIndex, columnFreq) =>
      val relevantIndexList = new mutable.MutableList[(Int, Long)]()
      columnFreq.zipWithIndex.foreach {
        case (colCount, colIndex) =>
          val runningTotalCol = runningTotal(colIndex)
          val ranksHere = targetRanks.filter(rank =>
            runningTotalCol < rank && runningTotalCol + colCount >= rank)
          relevantIndexList ++= ranksHere.map(rank => (colIndex, rank - runningTotalCol))
          runningTotal(colIndex) += colCount


      }
      (partitionIndex, relevantIndexList.toList)
  }
}