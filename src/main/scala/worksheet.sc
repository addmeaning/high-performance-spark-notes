import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


val session = SparkSession.builder().master("local[*]").appName("worksheet").getOrCreate()
session.sparkContext
Logger.getLogger("org").setLevel(Level.OFF)

val sc = session.sparkContext
sc.setLogLevel("ERROR")




val schema = StructType(StructField("DateOfAppication", StringType, true) :: Nil)
//val list2 = List(Date("2015-01-01"), "2015-5-28", "2015-5-29")
val list = List("5/20/2015", "5/28/2015", "5/29/2015")
val simpleRdd = sc.parallelize(list).map(s => Row(s))
