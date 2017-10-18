package goldilocks.solutions

import org.apache.spark.sql.DataFrame

/**
  * description: 
  * link to test/main class:
  */
trait Solution {
  type Rank = Long
  type Value = Double
  type Index = Int
  type Freq = Long
  type PartNumber = Int
  type ColIndex = Int
  /**
    * Goldilocks wanted us to design an application that would let her input an arbitrary list of integers n1…nk and return the nth best element in each column. For example, if Goldilocks input 8, 1000, and 20 million, our function would need to return the 8th, 1000th, and 20 millionth best-ranking panda for each attribute column.
    * @param dataFrame input dataframe
    * @param ranks list of positive ranks
    * @return Map [ColumnIndex, Ranks]; Ranks.length == length of ranks parameter
    */
  def findRankStatistics(dataFrame: DataFrame, ranks: List[Long]): Map[Int, Iterable[Double]]
}
