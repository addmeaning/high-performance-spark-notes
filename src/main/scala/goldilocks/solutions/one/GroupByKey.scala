package goldilocks.solutions.one

import goldilocks.solutions.Solution
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * description: 
  * link to test/main class:
  */
class GroupByKey extends Solution {


  /**
    * Goldilocks wanted us to design an application that would let her input an arbitrary list of integers n1…nk and return the nth best element in each column. For example, if Goldilocks input 8, 1000, and 20 million, our function would need to return the 8th, 1000th, and 20 millionth best-ranking panda for each attribute column.
    *
    * @param dataFrame input dataframe
    * @param ranks     list of positive ranks
    * @return Map [ColumnIndex, Ranks]; Ranks.length == length of ranks parameter
    */
  override def findRankStatistics(
                                   dataFrame: DataFrame,
                                   ranks: List[Long]): collection.Map[Int, Iterable[Double]] = {
    require(ranks.forall(_ > 0))
    //Map to column index, value pairs
    val pairRDD: RDD[(Int, Double)] = GroupByKey.mapToKeyValuePairs(dataFrame)

    val groupColumns: RDD[(Int, Iterable[Double])] = pairRDD.groupByKey()
    groupColumns.mapValues(
      iter => {
        //convert to an array and sort
        val sortedIter = iter.toArray.sorted

        sortedIter.toIterable.zipWithIndex.flatMap({
          case (colValue, index) =>
            if (ranks.contains(index + 1)) {
              Iterator(colValue)
            } else {
              Iterator.empty
            }
        })
      }).collectAsMap()
  }

  def findRankStatistics(
                          pairRDD: RDD[(Int, Double)],
                          ranks: List[Long]): collection.Map[Int, Iterable[Double]] = {
    assert(ranks.forall(_ > 0))
    pairRDD.groupByKey().mapValues(iter => {
      val sortedIter = iter.toArray.sorted
      sortedIter.zipWithIndex.flatMap(
        {
          case (colValue, index) =>
            if (ranks.contains(index + 1)) {
              //this is one of the desired rank statistics
              Iterator(colValue)
            } else {
              Iterator.empty
            }
        }
      ).toIterable //convert to more generic iterable type to match out spec
    }).collectAsMap()
  }


}
object GroupByKey {
  def mapToKeyValuePairs(dataFrame: DataFrame): RDD[(Int, Double)] = {
    val rowLength = dataFrame.schema.length
    dataFrame.rdd.flatMap(
      row => Range(0, rowLength).map(i => (i, row.getDouble(i)))
    )
  }
}