package goldilocks.solutions.three

import goldilocks.solutions.Solution
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * description: 
  * link to test/main class:
  */
class SortOnCellValues extends Solution {

  @transient private var _sc : SparkContext = _


  def findRankStatistics(dataFrame: DataFrame, targetRanks: List[Long]):
  collection.Map[Int, Iterable[Double]] = {

    val valueColumnPairs: RDD[(Value, Index)] = getValueColumnPairs(dataFrame)
    val sortedValueColumnPairs = valueColumnPairs.sortByKey()
    sortedValueColumnPairs.persist(StorageLevel.MEMORY_AND_DISK)

    val numOfColumns = dataFrame.schema.length
    val partitionColumnsFreq = getColumnsFreqPerPartition(sortedValueColumnPairs, numOfColumns)
    val ranksLocations = getRanksLocationsWithinEachPart(
      targetRanks, partitionColumnsFreq, numOfColumns)

    val targetRanksValues = findTargetRanksIteratively(
      sortedValueColumnPairs, ranksLocations)
    targetRanksValues.groupByKey.collectAsMap
  }

  private def getColumnsFreqPerPartition(sortedValueColumnPairs: RDD[(Value, Index)],
                                         numOfColumns: Int):
  Array[(PartNumber, Array[Freq])] = {

    val zero = Array.fill[Freq](numOfColumns)(0)

    def aggregateColumnFrequencies(partitionIndex: PartNumber,
                                   valueColumnPairs: Iterator[(Value, Index)]) = {
      val columnsFreq: Array[Freq] = valueColumnPairs.aggregate(zero)(
        (a: Array[Freq], v: (Value, Index)) => {
          val (_, colIndex) = v
          //increment the cell in the zero array corresponding to this column index
          a(colIndex) = a(colIndex) + 1L
          a
        },
        (a: Array[Freq], b: Array[Freq]) => {
          a.zip(b).map { case (aVal, bVal) => aVal + bVal }
        })

      Iterator((partitionIndex, columnsFreq))
    }

    sortedValueColumnPairs.mapPartitionsWithIndex(
      aggregateColumnFrequencies).collect()
  }

  private def getRanksLocationsWithinEachPart(targetRanks: List[Rank],
                                              partitionColumnsFreq: Array[(PartNumber, Array[Freq])],
                                              numOfColumns: Int): Array[(PartNumber, List[(Index, Rank)])] = {

    val runningTotal = Array.fill[Long](numOfColumns)(0)
    // The partition indices are not necessarily in sorted order, so we need
    // to sort the partitionsColumnsFreq array by the partition index (the
    // first value in the tuple).
    partitionColumnsFreq.sortBy(_._1).map { case (partitionIndex, columnsFreq) =>
      val relevantIndexList = new mutable.MutableList[(Int, Long)]()

      columnsFreq.zipWithIndex.foreach { case (colCount, colIndex) =>
        val runningTotalCol = runningTotal(colIndex)
        val ranksHere: List[Rank] = targetRanks.filter(rank =>
          runningTotalCol < rank && runningTotalCol + colCount >= rank)

        // For each of the rank statistics present add this column index and the
        // index it will be at on this partition (the rank - the running total).
        relevantIndexList ++= ranksHere.map(
          rank => (colIndex, rank - runningTotalCol))

        runningTotal(colIndex) += colCount
      }

      (partitionIndex, relevantIndexList.toList)
    }
  }

  private def getValueColumnPairs(dataFrame: DataFrame): RDD[(Value, Index)] = {
    dataFrame.rdd.flatMap {
      row: Row =>
        row.toSeq.zipWithIndex
          .map {
            case (v, index) => (v.toString.toDouble, index)
          }
    }
  }

  private def findTargetRanksIteratively(
                                          sortedValueColumnPairs: RDD[(Value, Index)],
                                          ranksLocations: Array[(Int, List[(Index, Rank)])]):
  RDD[(Index, Value)] = {

    sortedValueColumnPairs.mapPartitionsWithIndex(
      (partitionIndex: PartNumber, valueColumnPairs: Iterator[(Value, Index)]) => {
        val targetsInThisPart: List[(Index, Rank)] = ranksLocations(partitionIndex)._2
        if (targetsInThisPart.nonEmpty) {
          val columnsRelativeIndex: Map[Int, List[Rank]] =
            targetsInThisPart.groupBy(_._1).mapValues(_.map(_._2))
          val columnsInThisPart = targetsInThisPart.map(_._1).distinct

          val runningTotals: mutable.HashMap[Int, Rank] = new mutable.HashMap()
          runningTotals ++= columnsInThisPart.map(
            columnIndex => (columnIndex, 0L)).toMap

          //filter this iterator, so that it contains only those (value, columnIndex)
          //that are the ranks statistics on this partition
          //Keep track of the number of elements we have seen for each columnIndex using the
          //running total hashMap.
          valueColumnPairs.filter {
            case (value, colIndex) =>
              lazy val thisPairIsTheRankStatistic: Boolean = {
                val total = runningTotals(colIndex) + 1L
                runningTotals.update(colIndex, total)
                columnsRelativeIndex(colIndex).contains(total)
              }
              (runningTotals contains colIndex) && thisPairIsTheRankStatistic
          }.map(_.swap)
        }
        else {
          Iterator.empty
        }
      })
  }

}
