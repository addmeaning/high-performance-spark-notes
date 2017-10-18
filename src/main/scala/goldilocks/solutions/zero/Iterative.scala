package goldilocks.solutions.zero

import goldilocks.solutions.Solution
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * description: iterative solution of Goldilocks problem
  *
  * link to test/main class:
  */
class Iterative extends Solution{
  override def findRankStatistics(dataFrame: DataFrame, ranks: List[Long]): Map[Int, Iterable[Double]] ={
      require(ranks.forall(_ > 0))
    val numberOfColumns = dataFrame.schema.length
    var i = 0
    var  result = Map[Int, Iterable[Double]]()

    while(i < numberOfColumns){
      val col = dataFrame.rdd.map(row => row.getDouble(i))
      val sortedCol : RDD[(Double, Long)] = col.sortBy(v => v).zipWithIndex()
      val ranksOnly = sortedCol.filter{
        //rank statistics are indexed from one. e.g. first element is 0
        case (colValue, index) =>  ranks.contains(index + 1)
      }.keys
      val list = ranksOnly.collect()
      result += (i -> list)
      i+=1
    }
    result
  }
}
