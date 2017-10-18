package goldilocks.solutions.four

import goldilocks.solutions.Solution
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * description: 
  * link to test/main class:
  */
object ReduceToDistinct extends Solution {
  /**
    * Goldilocks wanted us to design an application that would let her input an arbitrary list of integers n1…nk and return the nth best element in each column. For example, if Goldilocks input 8, 1000, and 20 million, our function would need to return the 8th, 1000th, and 20 millionth best-ranking panda for each attribute column.
    *
    * @param dataFrame   input dataframe
    * @param targetRanks list of positive ranks
    * @return Map [ColumnIndex, Ranks]; Ranks.length == length of ranks pamameter
    */
  override def findRankStatistics(dataFrame: DataFrame, targetRanks: List[Rank]): collection.Map[PartNumber, Iterable[Value]] = {

    val aggregatedValueColumnPairs: RDD[((Value, Int), Long)] =
      getAggregatedValueColumnPairs(dataFrame)
    val sortedAggregatedValueColumnPairs = aggregatedValueColumnPairs.sortByKey()
    sortedAggregatedValueColumnPairs.persist(StorageLevel.MEMORY_AND_DISK)

    val numOfColumns = dataFrame.schema.length
    val partitionColumnsFreq =
      getColumnsFreqPerPartition(sortedAggregatedValueColumnPairs, numOfColumns)
    val ranksLocations =
      getRanksLocationsWithinEachPart(targetRanks,
        partitionColumnsFreq, numOfColumns)

    val targetRanksValues =
      findTargetRanksIteratively(sortedAggregatedValueColumnPairs, ranksLocations)
    targetRanksValues.groupByKey().collectAsMap()
  }

  def getAggregatedValueColumnPairs(dataFrame: DataFrame):  RDD[((Value, Int), Long)] = {

    val aggregatedValueColumnRDD = dataFrame.rdd.mapPartitions(rows => {
      val valueColumnMap = new mutable.HashMap[(Value, Int), Long]()
      rows.foreach(row => {
        row.toSeq.zipWithIndex.foreach { case (value, columnIndex) =>
          val key = (value.toString.toDouble, columnIndex)
          val count = valueColumnMap.getOrElseUpdate(key, 0)
          valueColumnMap.update(key, count + 1)
        }
      })

      valueColumnMap.toIterator
    })

    aggregatedValueColumnRDD
  }

  private def getColumnsFreqPerPartition(
                                          sortedAggregatedValueColumnPairs: RDD[((Value, Int), Long)],
                                          numOfColumns: Int): Array[(Int, Array[Long])] = {

    val zero = Array.fill[Long](numOfColumns)(0)

    def aggregateColumnFrequencies(
                                    partitionIndex: Int, pairs: Iterator[((Value, Int), Long)]) = {
      val columnsFreq: Array[Long] = pairs.aggregate(zero)(
        (a: Array[Long], v: ((Value, Int), Long)) => {
          val ((value, colIndex), count) = v
          a(colIndex) = a(colIndex) + count
          a
        },
        (a: Array[Long], b: Array[Long]) => {
          a.zip(b).map { case (aVal, bVal) => aVal + bVal }
        })

      Iterator((partitionIndex, columnsFreq))
    }

    sortedAggregatedValueColumnPairs.mapPartitionsWithIndex(
      aggregateColumnFrequencies).collect()
  }

  private def getRanksLocationsWithinEachPart(targetRanks: List[Long],
                                              partitionColumnsFreq: Array[(Int, Array[Long])],
                                              numOfColumns: Int): Array[(Int, List[(Int, Long)])] = {

    val runningTotal = Array.fill[Long](numOfColumns)(0)

    partitionColumnsFreq.sortBy(_._1).map { case (partitionIndex, columnsFreq) =>
      val relevantIndexList = new mutable.MutableList[(Int, Long)]()

      columnsFreq.zipWithIndex.foreach { case (colCount, colIndex) =>
        val runningTotalCol = runningTotal(colIndex)

        val ranksHere: List[Long] = targetRanks.filter(rank =>
          runningTotalCol < rank && runningTotalCol + colCount >= rank)
        relevantIndexList ++= ranksHere.map(
          rank => (colIndex, rank - runningTotalCol))

        runningTotal(colIndex) += colCount
      }

      (partitionIndex, relevantIndexList.toList)
    }
  }


  private def findTargetRanksIteratively(
                                          sortedAggregatedValueColumnPairs: RDD[((Value, Int), Long)],
                                          ranksLocations: Array[(Int, List[(Int, Long)])]): RDD[(Int, Value)] = {

    sortedAggregatedValueColumnPairs.mapPartitionsWithIndex((partitionIndex: Int,
                                                             aggregatedValueColumnPairs: Iterator[((Value, Int), Long)]) => {

      val targetsInThisPart: List[(Int, Long)] = ranksLocations(partitionIndex)._2
      if (targetsInThisPart.nonEmpty) {
        asIteratorToIteratorTransformation(
          aggregatedValueColumnPairs,
          targetsInThisPart)
      } else {
        Iterator.empty
      }
    })
  }

  def asIteratorToIteratorTransformation(
                                          valueColumnPairsIter: Iterator[((Value, Int), Long)],
                                          targetsInThisPart: List[(Int, Long)]): Iterator[(Int, Value)] = {

    val columnsRelativeIndex = targetsInThisPart.groupBy(_._1).mapValues(_.map(_._2))
    val columnsInThisPart = targetsInThisPart.map(_._1).distinct

    val runningTotals: mutable.HashMap[Int, Long] = new mutable.HashMap()
    runningTotals ++= columnsInThisPart.map(columnIndex => (columnIndex, 0L)).toMap

    //filter out the pairs that don't have a column index that is in this part
    val pairsWithRanksInThisPart = valueColumnPairsIter.filter {
      case (((_, colIndex), _)) =>
        columnsInThisPart contains colIndex
    }

    // map the valueColumn pairs to a list of (colIndex, value) pairs that correspond
    // to one of the desired rank statistics on this partition.
    pairsWithRanksInThisPart.flatMap {

      case (((value, colIndex), count)) =>

        val total = runningTotals(colIndex)
        val ranksPresent: List[Long] = columnsRelativeIndex(colIndex)
          .filter(index => (index <= count + total)
            && (index > total))

        val nextElems: Iterator[(Int, Value)] =
          ranksPresent.map(_ => (colIndex, value)).toIterator

        //update the running totals
        runningTotals.update(colIndex, total + count)
        nextElems
    }
  }


}
